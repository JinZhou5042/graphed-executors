"""Shared harness for the m68c frozen suite (plan-executors.md §6): join plans (``DurablePlanV2``) whose
left side calls a server, a counting local HTTP stand-in, a recording backend, and the byte measure.

Frozen suites never import across directories, so the m68a pieces this suite needs are copied here
(``run_bounded``, ``wait_for``, ``pid_gone``, ``free_range``, a trimmed ``RecordingBackend``). Everything a
worker or pilot unpickles is module-level; ``python m68c_harness.py <port> <report>`` is the managed child.
"""

from __future__ import annotations

import contextlib
import dataclasses
import json
import os
import pickle
import secrets
import socket
import sys
import threading
import time
import urllib.request
from collections.abc import Callable, Iterator, Mapping, Sequence
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

HARNESS_DIR = str(Path(__file__).resolve().parent)
HARNESS_FILE = str(Path(__file__).resolve())
RUN_TIMEOUT_S = 240.0
GATE_TIMEOUT_S = 120.0
HOLD_TIMEOUT_S = 120.0
SERVICE = "sf"
CALL_PATH = "/sf"
FACTOR = "3"


# ---- the managed child (stdlib only above this line's imports) -------------------------------------


class _PidHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        body = str(os.getpid()).encode()
        self.send_response(200)
        self.send_header("content-type", "text/plain")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        return None


def child_main(argv: Sequence[str]) -> int:
    """Write ``{"pid"}`` to ``argv[1]``, then answer every GET on ``("", argv[0])`` with this pid."""
    port, report = int(argv[0]), argv[1]
    with open(f"{report}.tmp", "w") as f:
        json.dump({"pid": os.getpid()}, f)
    os.replace(f"{report}.tmp", report)
    server = ThreadingHTTPServer(("", port), _PidHandler)
    server.timeout = 0.5
    deadline = time.monotonic() + 300.0
    while time.monotonic() < deadline:
        server.handle_request()
    server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(child_main(sys.argv[1:]))

import awkward as ak  # noqa: E402
import graphed  # noqa: E402
import numpy as np  # noqa: E402
from graphed import Session  # noqa: E402
from graphed.awkward import AwkwardBackend, AwkwardForm  # noqa: E402
from graphed.core import Partition, Task, WorkerResources  # noqa: E402
from graphed.core.execution import RunControl, RunState, SequentialRunner  # noqa: E402
from graphed.preserve import ExternalPlugin, record_external, sha256_bytes  # noqa: E402
from graphed.services import Launch, ServiceSpec, bind_services  # noqa: E402

from graphed_executors.submit import RunContext, SubmitFuture  # noqa: E402

# ---- bounds and process witnesses (copied from m68a) ----------------------------------------------


def run_bounded(fn: Callable[[], Any], timeout_s: float = RUN_TIMEOUT_S) -> Any:
    """``fn()`` on a daemon thread; a hang fails the test instead of wedging the job."""
    out: dict[str, Any] = {}

    def _drive() -> None:
        try:
            out["result"] = fn()
        except BaseException as exc:
            out["error"] = exc

    thread = threading.Thread(target=_drive, daemon=True)
    thread.start()
    thread.join(timeout_s)
    assert not thread.is_alive(), f"HARD TIMEOUT: call did not finish within {timeout_s}s"
    if "error" in out:
        raise out["error"]
    return out["result"]


class Background:
    """``fn()`` started on a daemon thread; :meth:`result` joins it within a bound."""

    def __init__(self, fn: Callable[[], Any]) -> None:
        self._out: dict[str, Any] = {}
        self._thread = threading.Thread(target=self._drive, args=(fn,), daemon=True)
        self._thread.start()

    def _drive(self, fn: Callable[[], Any]) -> None:
        try:
            self._out["result"] = fn()
        except BaseException as exc:
            self._out["error"] = exc

    def result(self, timeout_s: float = RUN_TIMEOUT_S) -> Any:
        self._thread.join(timeout_s)
        assert not self._thread.is_alive(), f"HARD TIMEOUT: run did not finish within {timeout_s}s"
        if "error" in self._out:
            raise self._out["error"]
        return self._out["result"]


def wait_for(predicate: Callable[[], bool], timeout_s: float = 30.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while not predicate():
        if time.monotonic() >= deadline:
            return predicate()
        time.sleep(0.02)
    return True


def pid_gone(pid: int) -> bool:
    """``pid`` is no longer a live (or unreaped) process."""
    if sys.platform == "win32":
        import ctypes  # noqa: PLC0415  (Windows only)

        kernel32 = ctypes.windll.kernel32
        handle = kernel32.OpenProcess(0x1000, False, pid)
        if not handle:
            return True
        try:
            code = ctypes.c_ulong()
            if not kernel32.GetExitCodeProcess(handle, ctypes.byref(code)):
                return True
            return code.value != 259  # STILL_ACTIVE
        finally:
            kernel32.CloseHandle(handle)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    return False


def kill_quietly(pid: int) -> None:
    if not pid_gone(pid):
        with contextlib.suppress(OSError):
            os.kill(pid, 9 if sys.platform != "win32" else 15)


def _bindable(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("127.0.0.1", port))
        except OSError:
            return False
    return True


def free_range(n: int = 3) -> tuple[int, int]:
    for _ in range(200):
        low = 20000 + secrets.randbelow(30000)
        if all(_bindable(p) for p in range(low, low + n)):
            return (low, low + n - 1)
    raise AssertionError(f"no {n} consecutive free ports found")


def read_report(path: Path, timeout_s: float = 60.0) -> int:
    wait_for(path.is_file, timeout_s)
    assert path.is_file(), f"the managed child never started: no report at {path}"
    return int(json.loads(path.read_text())["pid"])


# ---- the local HTTP stand-in -----------------------------------------------------------------------


class _Handler(BaseHTTPRequestHandler):
    server: _CountingHTTP

    def do_GET(self) -> None:
        body = self.server.owner.answer(self.path)
        self.send_response(200)
        self.send_header("content-type", "text/plain")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        return None


class _CountingHTTP(ThreadingHTTPServer):
    daemon_threads = True
    owner: StandIn


class StandIn:
    """An HTTP server on ``127.0.0.1:0`` answering ``FACTOR`` to every GET and counting GETs by path."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._gets: dict[str, int] = {}
        self._http = _CountingHTTP(("127.0.0.1", 0), _Handler)
        self._http.owner = self
        self.port = int(self._http.server_address[1])
        self._thread = threading.Thread(
            target=self._http.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True
        )
        self._thread.start()

    def answer(self, path: str) -> bytes:
        with self._lock:
            self._gets[path] = self._gets.get(path, 0) + 1
        return FACTOR.encode()

    def calls(self) -> int:
        """GETs of the External's path (readiness checks GET ``/`` and are not counted)."""
        with self._lock:
            return self._gets.get(CALL_PATH, 0)

    def endpoint(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def close(self) -> None:
        self._http.shutdown()
        self._http.server_close()
        self._thread.join(5.0)

    def __enter__(self) -> StandIn:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def given_spec() -> ServiceSpec:
    """A requirement without a recipe: only a given endpoint serves it."""
    return ServiceSpec(SERVICE, "http", check="http:/", ports=(40000, 40010), launch=None, timeout_s=30.0)


def managed_spec(report: Path) -> ServiceSpec:
    """A driver-hostable requirement whose recipe runs this file as the child."""
    launch = Launch(argv=("{python}", HARNESS_FILE, "{port}", str(report)))
    return ServiceSpec(SERVICE, "http", check="http:/", ports=free_range(), launch=launch, timeout_s=60.0)


# ---- the server-calling External -------------------------------------------------------------------

_CALLS_LOCK = threading.Lock()
CALLS: list[str] = []  # every body the External read, in-process runners only


def _samples() -> list[bytes]:
    return [b"m68c"]


def _scale(resource: Any, params: Mapping[str, Any], inputs: list[Any]) -> Any:
    """GET the bound endpoint's ``/sf`` and scale ``x`` by the answer; the record type is unchanged."""
    with urllib.request.urlopen(str(params["url"]) + CALL_PATH, timeout=10.0) as resp:
        body = resp.read().decode()
    with _CALLS_LOCK:
        CALLS.append(body)
    block = inputs[0]
    return ak.with_field(block, block["x"] * float(body), "x")


SCALE = ExternalPlugin(kind="m68c_scale", content_hash=sha256_bytes, evaluate=_scale, samples=_samples)


def calls_reset() -> None:
    with _CALLS_LOCK:
        CALLS.clear()


def calls_snapshot() -> list[str]:
    with _CALLS_LOCK:
        return list(CALLS)


# ---- sources and plans -----------------------------------------------------------------------------

_HOLD_LOCK = threading.Lock()
_HOLDS: dict[str, tuple[threading.Event, list[int]]] = {}


def hold(tag: str) -> str:
    """A fresh hold: under it every partition read after the first waits for :func:`release`."""
    with _HOLD_LOCK:
        _HOLDS[tag] = (threading.Event(), [0])
    return tag


def release(tag: str) -> None:
    with _HOLD_LOCK:
        event = _HOLDS[tag][0]
    event.set()


@dataclasses.dataclass(eq=False)
class Chunks:
    """A ``PartitionedSource`` over ``data`` in ``steps_per_file`` blind partitions."""

    uri: str
    data: Any
    held: str | None = None

    def partitions(self, steps_per_file: int) -> tuple[Partition, ...]:
        return tuple(Partition.blind(self.uri, "", s, steps_per_file) for s in range(steps_per_file))

    def read_partition(self, partition: Partition, columns: Any, resources: WorkerResources) -> Any:
        if self.held is not None:
            with _HOLD_LOCK:
                event, count = _HOLDS[self.held]
                count[0] += 1
                later = count[0] > 1
            if later:
                event.wait(HOLD_TIMEOUT_S)
        part = partition.resolve(len(self.data))
        return self.data[part.entry_start : part.entry_stop]


N_ROWS = 16
LEFT = {"k": [i % 5 for i in range(N_ROWS)], "x": [float(i) for i in range(N_ROWS)]}
RIGHT = {"k": [i % 7 for i in range(N_ROWS)], "y": [100.0 + i for i in range(N_ROWS)]}


def _source(s: Session, name: str, uri: str, columns: Mapping[str, list[Any]], held: str | None) -> Any:
    data = ak.Array({k: np.asarray(v) for k, v in columns.items()})
    form = AwkwardForm(ak.Array(data.layout.to_typetracer(forget_length=True)))
    return s.source(name, form=form, data=Chunks(uri, data, held))


def rows(values: list[Any]) -> list[str]:
    """The joined records of one dest as sorted canonical JSON (exact float reprs)."""
    return sorted(json.dumps(r, sort_keys=True) for v in values for r in ak.to_list(v))


def cat(a: list[str], b: list[str]) -> list[str]:
    return sorted(a + b)


def no_rows() -> list[str]:
    return []


def join_v2(
    tag: str,
    *,
    spec: ServiceSpec | None = None,
    steps_per_file: int = 8,
    held: str | None = None,
    combine: Callable[[list[str], list[str]], list[str]] = cat,
) -> Any:
    """``join_plan`` of ``left`` (through the ``SCALE`` External naming ``spec`` when given) with
    ``right`` on ``k``, folded by ``rows``/``combine``/``no_rows``."""
    s = Session(AwkwardBackend())
    left = _source(s, "left", f"mem://m68c-{tag}/left", LEFT, held)
    right = _source(s, "right", f"mem://m68c-{tag}/right", RIGHT, held)
    if spec is not None:
        s.declare_service(spec)
        left = record_external(s, SCALE, b"m68c-scale", [left], params={"service": spec.name})
    joined = graphed.join(left, right, on=["k"], how="inner")
    return graphed.join_plan(
        joined, steps_per_file=steps_per_file, reduce=rows, combine=combine, empty=no_rows
    )


def sequential(plan: Any, endpoints: Mapping[str, str] | None = None) -> Any:
    """graphed's reference run of ``plan`` (bound to ``endpoints`` when given)."""
    bound = bind_services(plan, endpoints) if endpoints else plan
    return SequentialRunner().run(bound)


def stage_index(plan: Any, kind: str) -> int:
    return [st.kind for st in plan.stages].index(kind)


# ---- task keys (plan-executors §4) -------------------------------------------------------------------


def is_plan_key(key: str) -> bool:
    return key.startswith("graphed-")


def is_probe_key(key: str) -> bool:
    return key.startswith("svc-") and "-probe-" in key


def is_map(key: str) -> bool:
    return "-map_write." in key


def is_pick(key: str) -> bool:
    return "-pick." in key


def is_gather(key: str) -> bool:
    return "-gather" in key


def key_index(key: str) -> int:
    return int(key.rsplit("-", 1)[1])


def pick_edge(key: str) -> tuple[int, int, int]:
    """``...-pick.<s>.<t>-<dest>`` -> ``(s, t, dest)``."""
    kind, dest = key.rsplit("-", 1)
    _, s, t = kind.rsplit("-", 1)[1].split(".")
    return int(s), int(t), int(dest)


# ---- a backend that records what the engine hands it -------------------------------------------------


class RecordingBackend:
    """A ``SubmitBackend`` over ``inner`` recording, in order, every submit (key and args), broadcast
    handle, cancel (by key) and task start; attributes it does not define are ``inner``'s.

    ``gate``: the inner (single) worker is held by a blocker task until the event is set.
    ``fail_key``/``fault``: ``submit`` raises ``fault`` for the first key the predicate accepts.
    ``wrap=False`` submits ``fn`` itself (for backends that ship tasks to other processes): no task
    starts and no gather args are recorded then. With ``wrap``, each gather's resolved args are kept."""

    def __init__(
        self,
        inner: Any,
        *,
        gate: threading.Event | None = None,
        fail_key: Callable[[str], bool] | None = None,
        fault: BaseException | None = None,
        wrap: bool = True,
    ) -> None:
        self.inner = inner
        self.capabilities = inner.capabilities
        self.lock = threading.Lock()
        self.submits: list[tuple[str, tuple[object, ...]]] = []
        self.futures: dict[str, Any] = {}
        self.handles: list[object] = []
        self.cancels: list[str] = []
        self.ran: list[str] = []
        self.gather_args: dict[str, tuple[object, ...]] = {}
        self._key_of: dict[int, str] = {}
        self._fail_key = fail_key
        self._fault = fault
        self._wrap = wrap
        if gate is not None:
            inner.submit(gate.wait, GATE_TIMEOUT_S, key="m68c-gate")

    def __getattr__(self, name: str) -> Any:
        if name == "inner":
            raise AttributeError(name)
        return getattr(self.inner, name)

    def n_workers(self) -> int:
        return int(self.inner.n_workers())

    def submit(self, fn: Callable[..., object], /, *args: object, key: str, **hints: Any) -> Any:
        with self.lock:
            self.submits.append((key, args))
        if self._fail_key is not None and self._fault is not None and self._fail_key(key):
            self._fail_key = None
            raise self._fault
        if self._wrap:
            fut = self.inner.submit(self._started, key, fn, *args, key=key, **hints)
        else:
            fut = self.inner.submit(fn, *args, key=key, **hints)
        with self.lock:
            self._key_of[id(fut)] = key
            self.futures[key] = fut
        return fut

    def _started(self, key: str, fn: Callable[..., object], *args: object) -> object:
        with self.lock:
            self.ran.append(key)
            if is_gather(key):
                self.gather_args[key] = args
        return fn(*args)

    def broadcast(self, payload: bytes, *, token: str) -> object:
        handle = self.inner.broadcast(payload, token=token)
        with self.lock:
            self.handles.append(handle)
        return handle

    def subscribe_events(
        self, topic: str, handler: Callable[[list[dict[str, object]]], None]
    ) -> Callable[[], None]:
        unsub: Callable[[], None] = self.inner.subscribe_events(topic, handler)
        return unsub

    def cancel(self, futures: Sequence[Any]) -> None:
        with self.lock:
            self.cancels.extend(self._key_of.get(id(f), "?") for f in futures)
        self.inner.cancel(futures)

    def drain(self, timeout_s: float = 60.0) -> None:
        """Wait until every task queued before this call has left the inner worker's queue."""
        self.inner.submit(time.sleep, 0, key="m68c-drain").result(timeout_s)

    def close(self) -> None:
        self.inner.close()

    def submitted(self) -> list[str]:
        with self.lock:
            return [k for k, _ in self.submits]

    def plan_keys(self) -> list[str]:
        return [k for k in self.submitted() if is_plan_key(k)]

    def probe_keys(self) -> list[str]:
        return [k for k in self.submitted() if is_probe_key(k)]

    def ran_keys(self) -> list[str]:
        with self.lock:
            return list(self.ran)

    def cancelled(self) -> list[str]:
        with self.lock:
            return list(self.cancels)

    def data_args(self, key: str) -> list[object]:
        """The submit's args that are not a ``RunContext``, ``str``, ``Task`` or broadcast handle."""
        with self.lock:
            args = next(a for k, a in self.submits if k == key)
            handles = list(self.handles)
        return [
            a
            for a in args
            if not isinstance(a, (RunContext, str, Task)) and not any(a is h for h in handles)
        ]

    def measure(self) -> tuple[int, int]:
        """(pickled bytes of the data args of every pick and gather submit, a future counted as its
        result; pickled bytes of every map-write result)."""
        moved = 0
        for key in [k for k in self.submitted() if is_pick(k) or is_gather(k)]:
            for arg in self.data_args(key):
                value = arg.result() if isinstance(arg, SubmitFuture) else arg
                moved += len(pickle.dumps(value))
        maps = sum(len(pickle.dumps(self.futures[k].result())) for k in self.submitted() if is_map(k))
        return moved, maps


class ParkingControl(RunControl):
    """A ``RunControl`` whose ``wait`` sets ``parked`` when entered while PAUSED."""

    def __init__(self) -> None:
        super().__init__()
        self.parked = threading.Event()

    def wait(self, timeout: float | None = None) -> RunState:
        if self.state is RunState.PAUSED:
            self.parked.set()
        return super().wait(timeout)


@contextlib.contextmanager
def closing(obj: Any, timeout_s: float = RUN_TIMEOUT_S) -> Iterator[Any]:
    try:
        yield obj
    finally:
        run_bounded(obj.close, timeout_s)
