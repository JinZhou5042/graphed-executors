"""The parsl worker-transport HTTP plane (plan §1.4; stdlib-only, importable with no parsl/dask).

``EscalatingHttpTransport`` is a :class:`graphed.core.execution.WorkerTransport` built ON the frozen
``local/_transport.py`` substrate (``local/*`` is NEVER edited — this subclasses it) with the two
behaviors the m47 contract needs that the local sender cannot provide:

- **Send-exhaustion escalation (the drop->raise bridge).** The base ``HttpTransport`` counts ``drops``
  from a background sender thread and never raises — a silently dropped reduction/shuffle node is a
  lost partial. This subclass sends **inline in the caller's thread** with the frozen 5-attempt
  policy: 200->True; unknown/self dest->False; a destination **503** (full-inbox reject) ->
  ``False`` + ``sends_rejected`` with **NO retry** (special-cased BEFORE the generic retry — review
  N2; retrying into a full inbox only defers the verdict); a comm-class failure exhausting the
  retries -> **raise** ``TransportDeliveryError`` + ``send_failures`` (never a silent ``drops += 1``).
- **A dual-route server.** The local ``_InboxServer``/``_Handler`` is path-blind. This plane serves
  ``POST /msg`` (inbox delivery, epoch-guarded, optional bounded ``inbox_maxsize`` -> 503) and
  ``POST /pull`` (block-store reads, coalesced, evict-after-serve) on one bound port.

``recv`` also honors an optional ``idle_deadline_s`` (design-A backstop): once that long passes with
no delivered traffic it RAISES ``TransportDeliveryError`` so a reduce peer's drain loop always
terminates even if the driver's ``("done",)`` release is lost. ``broadcast`` attempts EVERY peer
then raises the aggregate (never the base stop-at-first loop), so one dead peer cannot strand the
live ones.
"""

from __future__ import annotations

import contextlib
import hashlib
import os
import pickle
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import Any

from graphed_executors.local._transport import HttpTransport, LookupFreeHTTPServer

#: the frozen local sender policy (``local/_transport.py:293``), restated so the retry arms budget it.
SEND_RETRIES = 5


class TransportDeliveryError(Exception):
    """A send exhausted its retries, or a peer drain deadline passed — a run-fatal, restart-worthy
    loss. Same NAME as the dask-side class so the reused classifier (``is_restart_worthy``) matches."""


class PullTimeoutError(Exception):
    """A block ``/pull`` from a dead/slow holder timed out — restart-worthy (never a partial result)."""


# ---- test/inject seam: a file-backed per-(dest,src) failure budget, PERSISTED across epochs -------
_BUDGET_LOCK = threading.Lock()


def _consume_budget(path: str) -> bool:
    """Atomically decrement the countdown file at ``path``; return True iff it was > 0 (a failure is
    injected). Persisted on disk so an epoch restart resumes the LEFTOVER budget (retry+restart
    compose — the m44 F10 seam analog; production callers never wire it)."""
    with _BUDGET_LOCK:
        try:
            n = int(Path(path).read_text())
        except (OSError, ValueError):
            return False
        if n <= 0:
            return False
        Path(path).write_text(str(n - 1))
        return True


# ---- the dual-route server (subclass of the frozen inbox server; no edit to local/*) --------------
class _DualRouteHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # stdlib handler name
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        server: Any = self.server
        if self.path == "/pull":
            payload = server.serve_pull(body)
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        status = server.deliver_msg(body)  # /msg
        self.send_response(status)
        self.end_headers()

    def log_message(self, *args: Any) -> None:  # silence the default stderr access log
        pass


class _DualRouteServer(LookupFreeHTTPServer):
    daemon_threads = True
    request_queue_size = 256

    def __init__(
        self,
        addr: tuple[str, int],
        *,
        epoch: str,
        inbox_maxsize: int | None,
        recv_failures: dict[str, str] | None,
    ) -> None:
        super().__init__(addr, _DualRouteHandler)
        self._epoch = epoch
        self._inbox_maxsize = inbox_maxsize
        self._recv_failures = recv_failures or {}
        self._inbox: deque[tuple[str, object]] = deque()
        self._lock = threading.Lock()
        self._seen_digests: set[str] = set()  # per-epoch content digests (at-least-once dedup witness)
        # block store (the /pull plane)
        self._blocks: dict[str, bytes] = {}
        self._store_lock = threading.Lock()
        # receiver + block-plane counters
        self.recv_rejects = 0
        self.stale_epoch_rejects = 0
        self.recv_duplicate_deliveries = 0
        self.bytes_served = 0
        self.pull_requests_served = 0
        self.serve_pid = 0
        self.last_delivery = time.monotonic()

    def deliver_msg(self, body: bytes) -> int:
        """Handle ``POST /msg``. Returns the HTTP status: 200 delivered/rejected-stale, 503 full inbox,
        500 to inject a comm-class failure (the deliver-then-fail seam — delivered THEN failed)."""
        try:
            sender, epoch, message = pickle.loads(body)
        except Exception:
            return 200
        if epoch != self._epoch:  # the run-nonce guard: never deliver dead-epoch traffic, count it
            with self._lock:
                self.stale_epoch_rejects += 1
            return 200
        with self._lock:
            if self._inbox_maxsize is not None and len(self._inbox) >= self._inbox_maxsize:
                self.recv_rejects += 1
                return 503  # the destination's VERDICT — the sender maps it to False + sends_rejected
            digest = hashlib.sha256(body).hexdigest()
            if digest in self._seen_digests:
                self.recv_duplicate_deliveries += 1  # at-least-once: a retried identical send re-landed
            else:
                self._seen_digests.add(digest)
            self._inbox.append((sender, message))
            self.last_delivery = time.monotonic()
        # deliver-then-fail injection (armed only in tests): the message IS in the inbox; fail the
        # HTTP response so the sender's REAL inline retry re-sends a genuine at-least-once duplicate.
        fail_path = self._recv_failures.get(sender)
        if fail_path is not None and _consume_budget(fail_path):
            return 500
        return 200

    def serve_pull(self, body: bytes) -> bytes:
        """Handle ``POST /pull`` (``pickle((epoch, [digest, ...]))``): return ``pickle({digest: wire})``
        for the present digests, counting served bytes/requests and EVICTING each after serve (F5)."""
        try:
            _epoch, digests = pickle.loads(body)
        except Exception:
            return pickle.dumps({})
        out: dict[str, bytes] = {}
        with self._store_lock:
            self.pull_requests_served += 1  # one request per (puller, holder) batch — the <= k*k bound
            self.serve_pid = os.getpid()
            for digest in digests:
                wire = self._blocks.pop(digest, None)  # evict-after-serve
                if wire is not None:
                    out[digest] = wire
                    self.bytes_served += len(wire)
        return pickle.dumps(out)

    # ---- inbox helpers (mirror the frozen _InboxServer surface) ----
    def pop_one(self) -> tuple[str, object] | None:
        with self._lock:
            return self._inbox.popleft() if self._inbox else None

    def drain(self) -> list[tuple[str, object]]:
        with self._lock:
            out = list(self._inbox)
            self._inbox.clear()
        return out

    # ---- block-store put / local-take (holder side) ----
    def put_block(self, digest: str, wire: bytes) -> None:
        with self._store_lock:
            self._blocks[digest] = wire

    def take_local(self, digest: str) -> bytes | None:
        """Read + evict a SELF-owned fragment locally (never over HTTP, so it is not billed to
        ``bytes_served`` — the block-plane oracle counts only cross-peer pulls)."""
        with self._store_lock:
            return self._blocks.pop(digest, None)

    def store_blocks_count(self) -> int:
        with self._store_lock:
            return len(self._blocks)


class EscalatingHttpTransport(HttpTransport):
    """A :class:`graphed.core.execution.WorkerTransport` on the dual-route plane (plan §1.4). Reuses
    the base ``set_registry``/``peers`` (registry-only); overrides ``__init__``/``send``/``recv``/
    ``poll``/``broadcast``/``close`` for the inline raising send, the ``/msg``+``/pull`` server, and
    the idle-deadline backstop. The base's background sender thread is NOT started."""

    def __init__(
        self,
        address: str,
        *,
        epoch: str,
        host: str = "127.0.0.1",
        inbox_maxsize: int | None = None,
        idle_deadline_s: float | None = None,
        recv_failures: dict[str, str] | None = None,
    ) -> None:
        self.address = address
        self._epoch = epoch
        # a distinct attribute (not the base's ``_server: _InboxServer``): the dual-route server is a
        # different type, and the inherited set_registry/peers use only ``_registry`` (set below).
        self._dual: _DualRouteServer = _DualRouteServer(
            (host, 0), epoch=epoch, inbox_maxsize=inbox_maxsize, recv_failures=recv_failures
        )
        self.host = str(self._dual.server_address[0])
        self.port = int(self._dual.server_address[1])
        self._registry: dict[str, tuple[str, int]] = {}
        self._idle_deadline_s = idle_deadline_s
        self._stop = threading.Event()
        # sender-side counters (the subclass's OWN witnesses — never the base's silent `drops`)
        self.sends_retried = 0
        self.send_failures = 0
        self.sends_rejected = 0
        self._srv_thread = threading.Thread(
            target=self._dual.serve_forever, name=f"gx-p2p-{address}", daemon=True
        )
        self._srv_thread.start()

    # receiver + block counters live on the server; surface them on the transport for the witnesses
    @property
    def recv_rejects(self) -> int:
        return self._dual.recv_rejects

    @property
    def stale_epoch_rejects(self) -> int:
        return self._dual.stale_epoch_rejects

    @property
    def recv_duplicate_deliveries(self) -> int:
        return self._dual.recv_duplicate_deliveries

    @property
    def bytes_served(self) -> int:
        return self._dual.bytes_served

    @property
    def pull_requests_served(self) -> int:
        return self._dual.pull_requests_served

    @property
    def serve_pid(self) -> int:
        return self._dual.serve_pid

    def send(self, dest: str, message: object) -> bool:
        if dest not in self._registry or dest == self.address:
            return False
        host, port = self._registry[dest]
        body = pickle.dumps((self.address, self._epoch, message))
        url = f"http://{host}:{port}/msg"
        last_exc: BaseException | None = None
        for attempt in range(SEND_RETRIES):
            if attempt > 0:
                self.sends_retried += 1
            try:
                req = urllib.request.Request(url, data=body, method="POST")
                urllib.request.urlopen(req, timeout=5).close()
                return True
            except urllib.error.HTTPError as exc:
                if exc.code == 503:  # the destination's full-inbox verdict — no retry (review N2)
                    self.sends_rejected += 1
                    return False
                last_exc = exc
            except (urllib.error.URLError, OSError) as exc:
                last_exc = exc
            if self._stop.is_set():
                break
            time.sleep(0.01 * (attempt + 1))
        self.send_failures += 1
        raise TransportDeliveryError(f"send to {dest!r} exhausted {SEND_RETRIES} attempts: {last_exc}")

    def broadcast(self, message: object) -> None:
        """Attempt EVERY registered peer, THEN raise the aggregated failure — one dead peer cannot
        strand the live ones (design-A release netting; never the base stop-at-first loop)."""
        failed: list[tuple[str, BaseException]] = []
        for dest in self.peers():
            try:
                self.send(dest, message)
            except TransportDeliveryError as exc:  # collect, keep going
                failed.append((dest, exc))
        if failed:
            names = ", ".join(d for d, _e in failed)
            raise TransportDeliveryError(f"broadcast exhausted for {names}")

    def poll(self) -> list[tuple[str, object]]:
        return self._dual.drain()

    def recv(self, timeout: float | None = None) -> tuple[str, Any] | None:
        # ``Any`` message (not ``object``): callers immediately destructure the pinned message tuples
        # (("hello", …)/("node", …)/…); ``Any`` is LSP-compatible with the base's ``object`` return.
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            item = self._dual.pop_one()
            if item is not None:
                return item
            if (
                self._idle_deadline_s is not None
                and (time.monotonic() - self._dual.last_delivery) > self._idle_deadline_s
            ):
                raise TransportDeliveryError(
                    f"peer drain deadline: no traffic for {self._idle_deadline_s}s, release lost"
                )
            if deadline is not None and time.monotonic() >= deadline:
                return None
            self._stop.wait(0.005)
            if self._stop.is_set():
                return None

    # ---- block plane (holder + puller) ----
    def put_block(self, digest: str, wire: bytes) -> None:
        self._dual.put_block(digest, wire)

    def take_local(self, digest: str) -> bytes | None:
        return self._dual.take_local(digest)

    def store_blocks_count(self) -> int:
        return self._dual.store_blocks_count()

    def pull(self, holder_addr: str, digests: list[str], *, timeout_s: float) -> dict[str, bytes]:
        """Pull ``digests`` from ``holder_addr`` over ``/pull`` — ONE coalesced request. Raises
        ``PullTimeoutError`` on a dead/slow holder (never a partial/empty result)."""
        target = self._registry.get(holder_addr)
        if target is None:
            raise PullTimeoutError(f"pull holder {holder_addr!r} not in the registry")
        body = pickle.dumps((self._epoch, list(digests)))
        url = f"http://{target[0]}:{target[1]}/pull"
        try:
            req = urllib.request.Request(url, data=body, method="POST")
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                got: dict[str, bytes] = pickle.loads(resp.read())
        except (urllib.error.URLError, OSError) as exc:
            raise PullTimeoutError(f"pull from {holder_addr!r} failed/timed out: {exc}") from exc
        missing = [d for d in digests if d not in got]
        if missing:
            raise PullTimeoutError(f"holder {holder_addr!r} served no bytes for {len(missing)} digest(s)")
        return got

    def close(self) -> None:
        self._stop.set()
        with contextlib.suppress(Exception):
            self._dual.shutdown()
        with contextlib.suppress(Exception):
            self._dual.server_close()
