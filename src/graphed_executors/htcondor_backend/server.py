"""The driver-side task server pilots pull from.

Every request is a POST of a pickle signed with the run's secret (``X-Graphed-Sig``, the hex
HMAC-SHA256 of the body); a missing or wrong signature is answered 403 before anything is unpickled.
Routes: ``/hello`` registers a pilot, ``/next`` long-polls a task lease, ``/beat`` keeps the pilot's
leases alive, ``/result`` settles a task's future.

A task waits in PENDING until its future arguments are done, then queues. Its first lease moves it to
RUNNING. A pilot silent for ``LEASE_S`` is lost: each task it held is re-queued once, and a second loss
fails it with :class:`WorkerLost`. When no pilot is left and none can come, queued tasks fail too.
"""

from __future__ import annotations

import hashlib
import hmac
import itertools
import pickle
import secrets
import sys
import threading
import time
import traceback
from collections import deque
from collections.abc import Callable
from concurrent.futures import CancelledError, Future, InvalidStateError
from contextlib import suppress
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler
from typing import Any

from graphed_executors.local._transport import LookupFreeHTTPServer
from graphed_executors.parsl_backend._shim import _parsl_task_shim
from graphed_executors.parsl_backend.backend import _ParslFuture

# A pilot beats every LEASE_S / 6, so it is lost after six missed beats: that rides out a task holding
# the GIL and a transient network drop. Read at call time, never copied, so a test can shorten it.
LEASE_S = 30.0
POLL_S = 10.0  # the /next long-poll bound

SIG_HEADER = "X-Graphed-Sig"
TASK_HEADER = "X-Graphed-Task"


class WorkerLost(Exception):
    """A task's pilot was lost twice (``pilot`` names the last one), or no pilot is left to run it."""

    def __init__(self, key: str, pilot: str) -> None:
        super().__init__(key, pilot)
        self.key = key
        self.pilot = pilot


def sign(secret: bytes, body: bytes) -> str:
    return hmac.new(secret, body, hashlib.sha256).hexdigest()


def settle(fut: Future[Any], value: object = None, exc: BaseException | None = None) -> None:
    """Complete ``fut`` unless it is already done (cancelled, or settled first elsewhere)."""
    with suppress(InvalidStateError):
        if exc is not None:
            fut.set_exception(exc)
        else:
            fut.set_result(value)


@dataclass
class _Task:
    key: str
    fut: Future[Any]
    payload: bytes
    leased_to: str | None = None
    started: bool = False
    requeued: bool = False


class _Http(LookupFreeHTTPServer):
    daemon_threads = True
    # SO_REUSEADDR lets Windows bind a port another server still listens on; elsewhere it only
    # skips TIME_WAIT, which the port scan needs across back-to-back runs
    allow_reuse_address = sys.platform != "win32"
    tasks: TaskServer


def _bind(port_range: tuple[int, int]) -> _Http:
    low, high = port_range
    for port in range(low, high + 1):
        with suppress(OSError):
            return _Http(("", port), _Handler)
    raise OSError(f"no free port in {low}-{high} for the task server")


class TaskServer:
    """Binds the first free port of ``port_range`` (inclusive) on all interfaces and advertises
    ``http://{host}:{port}``. ``launcher`` is asked how many pilots can still come, and its
    ``log_dir`` (if any) is named when none can."""

    def __init__(self, host: str, port_range: tuple[int, int], launcher: Any) -> None:
        self.secret = secrets.token_bytes(32)
        self._launcher = launcher
        self._cond = threading.Condition()
        self._queue: deque[int] = deque()
        self._tasks: dict[int, _Task] = {}
        self._ids = itertools.count()
        self._beats: dict[str, float] = {}
        self._lost: set[str] = set()
        self._closed = False
        self._http = _bind(port_range)
        self._http.tasks = self
        self.url = f"http://{host}:{self._http.server_address[1]}"
        threading.Thread(target=self._http.serve_forever, daemon=True, name="graphed-task-server").start()
        threading.Thread(target=self._reap, daemon=True, name="graphed-task-reaper").start()

    # ---- driver side ----

    def add(self, key: str, fn: Callable[..., object], args: tuple[object, ...], fut: Future[Any]) -> None:
        """Queue ``fn(*args)`` once every future argument is done; a failed one fails ``fut`` with its
        exception, so a lost pilot reaches the dependents of any arity unchanged."""
        deps = [a for a in args if isinstance(a, _ParslFuture)]
        remaining = [len(deps)]
        count_lock = threading.Lock()

        def ready() -> None:
            for dep in deps:
                exc = CancelledError() if dep._raw.cancelled() else dep.exception(0)
                if exc is not None:
                    settle(fut, exc=exc)
                    return
            resolved = tuple(a.result() if isinstance(a, _ParslFuture) else a for a in args)
            try:
                payload = pickle.dumps((_parsl_task_shim, fn, resolved))
            except Exception as exc:  # raised in a done-callback it would be logged and the task would hang
                settle(fut, exc=exc)
                return
            with self._cond:
                tid = next(self._ids)
                self._tasks[tid] = _Task(key, fut, payload)
                self._queue.append(tid)
                self._cond.notify()

        def dep_done(_dep: object) -> None:
            with count_lock:
                remaining[0] -= 1
                last = remaining[0] == 0
            if last:
                ready()

        if not deps:
            ready()
        for dep in deps:
            dep.add_done_callback(dep_done)

    def live_pilots(self) -> int:
        now = time.monotonic()
        with self._cond:
            return sum(p not in self._lost and now - t <= LEASE_S for p, t in self._beats.items())

    def close(self) -> None:
        """Answer 410 to every pilot from now on and fail the tasks still held."""
        with self._cond:
            self._closed = True
            held = list(self._tasks.values())
            self._tasks.clear()
            self._queue.clear()
            self._cond.notify_all()
        for task in held:
            settle(task.fut, exc=RuntimeError(f"task {task.key} unfinished when the HTCondor backend closed"))

    def shutdown(self) -> None:
        self._http.shutdown()
        self._http.server_close()

    # ---- pilot side, called from the request handlers ----

    def hello(self, pilot: str) -> dict[str, float]:
        self.beat(pilot)
        return {"beat_s": LEASE_S / 6, "lease_s": LEASE_S}

    def beat(self, pilot: str) -> bool:
        """Refresh ``pilot``'s leases; False when it should stop (closed, or declared lost)."""
        with self._cond:
            if pilot not in self._lost:
                self._beats[pilot] = time.monotonic()
            return not (self._closed or pilot in self._lost)

    def lease(self, pilot: str) -> tuple[int, bytes] | None:
        """The next task for ``pilot``, or None after ``POLL_S`` or once it should stop."""
        deadline = time.monotonic() + POLL_S
        with self._cond:
            while self.beat(pilot):
                while self._queue:
                    tid = self._queue.popleft()
                    task = self._tasks[tid]
                    if not task.started:
                        task.started = True
                        if not task.fut.set_running_or_notify_cancel():
                            del self._tasks[tid]  # cancelled while it queued
                            continue
                    task.leased_to = pilot
                    return tid, task.payload
                if not self._cond.wait(deadline - time.monotonic()):
                    return None
            return None

    def result(self, pilot: str, tid: int, ok: bool, blob: bytes) -> None:
        """Settle task ``tid`` from ``pilot``; a result from a pilot that no longer holds the lease is dropped."""
        self.beat(pilot)
        with self._cond:
            task = self._tasks.get(tid)
            if task is None or task.leased_to != pilot:
                return
            del self._tasks[tid]
        try:
            value = pickle.loads(blob)
        except Exception as exc:  # a hang otherwise: the future would never settle
            ok, value = False, RuntimeError(f"task {task.key}: its result does not unpickle here: {exc!r}")
        if ok:
            settle(task.fut, value)
        else:
            settle(task.fut, exc=value)

    # ---- the reaper ----

    def _reap(self) -> None:
        while not self._closed:
            time.sleep(LEASE_S / 6)
            self.reap_once()

    def reap_once(self) -> None:
        failed: list[tuple[Future[Any], BaseException]] = []
        now = time.monotonic()
        with self._cond:
            for pilot, beat in self._beats.items():
                if pilot in self._lost or now - beat <= LEASE_S:
                    continue
                self._lost.add(pilot)
                for tid, task in list(self._tasks.items()):
                    if task.leased_to != pilot:
                        continue
                    if task.requeued:
                        del self._tasks[tid]
                        failed.append((task.fut, WorkerLost(task.key, pilot)))
                    else:
                        task.requeued, task.leased_to = True, None
                        self._queue.appendleft(tid)
                        self._cond.notify()
            starving = bool(self._queue) and set(self._beats) <= self._lost
        # the launcher is asked only when work waits with no pilot: a schedd query is not free
        if starving and self._launcher.alive() == 0:
            where = f"no pilots left; see {getattr(self._launcher, 'log_dir', None) or 'the launcher logs'}"
            with self._cond:
                for tid in self._queue:
                    task = self._tasks.pop(tid)
                    failed.append((task.fut, WorkerLost(task.key, where)))
                self._queue.clear()
        for fut, exc in failed:
            settle(fut, exc=exc)


class _Handler(BaseHTTPRequestHandler):
    server: _Http

    def log_message(self, format: str, *args: Any) -> None:
        return None  # one line per long poll would drown the driver's stderr

    def _reply(self, status: HTTPStatus, body: bytes = b"", headers: dict[str, str] | None = None) -> None:
        self.send_response(status)
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:
        tasks = self.server.tasks
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        # headers arrive latin-1 decoded; comparing bytes keeps a malformed one a plain mismatch
        sig = self.headers.get(SIG_HEADER, "").encode("latin-1")
        if not hmac.compare_digest(sig, sign(tasks.secret, body).encode()):
            self._reply(HTTPStatus.FORBIDDEN)
            return
        try:
            self._route(tasks, pickle.loads(body))
        except Exception:
            self._reply(HTTPStatus.BAD_REQUEST, traceback.format_exc().encode())

    def _route(self, tasks: TaskServer, msg: Any) -> None:
        if self.path == "/hello":
            self._reply(HTTPStatus.OK, pickle.dumps(tasks.hello(str(msg))))
        elif self.path == "/beat":
            self._reply(HTTPStatus.OK if tasks.beat(str(msg)) else HTTPStatus.GONE)
        elif self.path == "/next":
            leased = tasks.lease(str(msg))
            if leased is not None:
                self._reply(HTTPStatus.OK, leased[1], {TASK_HEADER: str(leased[0])})
            else:
                self._reply(HTTPStatus.NO_CONTENT if tasks.beat(str(msg)) else HTTPStatus.GONE)
        else:
            pilot, tid, ok, blob = msg  # /result
            tasks.result(pilot, tid, ok, blob)
            self._reply(HTTPStatus.OK)
