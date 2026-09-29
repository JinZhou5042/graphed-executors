"""Inter-worker comms backends (M38): concrete :class:`graphed.core.execution.WorkerTransport`
implementations for single-machine executors.

Two interchangeable backends behind one interface — the seam peer reduction and work-stealing ride,
and the seam a future *distributed* executor reuses unchanged:

- :class:`QueueTransport` — **IPC** (the default). Per-endpoint inbox queue + a registry of peers'
  inboxes. ``queue.Queue`` for the in-process / thread-pool case; a :class:`PipeInbox`
  (``multiprocessing.SimpleQueue``, **no feeder thread**) for the process pool (queue-type-agnostic —
  it only uses ``put_nowait`` / ``get_nowait`` / ``get``). A ``PipeInbox`` send is a synchronous pipe
  write, so it blocks above the OS pipe buffer: a caller that must keep reading (or keep going) hands
  its sends to a separate thread — ``_peer._Outbox`` for a worker, ``_peer.release_workers`` for the
  driver's teardown broadcast.
- :class:`HttpTransport` — **HTTP** over loopback. Each endpoint runs a tiny stdlib ``http.server`` in
  a daemon thread; ``send`` enqueues to a local outbound buffer that a background sender thread POSTs
  to the destination (so ``send`` stays non-blocking, exactly like the M37 dashboard client). Fully
  exercisable in-process (every endpoint a thread); a real distributed executor swaps the loopback
  registry for remote hosts.

Both honour the contract: message order/delivery are not guaranteed, and reduction determinism is the
protocol layer's job (it keys by leaf index).
"""

from __future__ import annotations

import contextlib
import ipaddress
import pickle
import queue
import socket
import socketserver
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

_DEFAULT_MAXSIZE = 10000
# How long :meth:`HttpTransport.close` waits for already-queued sends to be POSTed. The healthy path
# never pays it (the sender is idle, so the sentinel returns immediately); it exists because the last
# messages an endpoint queues are the ones teardown depends on — the driver's ``done`` release, whose
# loss leaves the workers unreleased and the pool shutdown waiting on them. A dead destination refuses
# fast, so even the crash case usually costs milliseconds, not the bound.
CLOSE_DRAIN_S = 5.0


# ---- routable-address selection (M39 §6.1 / B4) -------------------------------------------------
def is_routable_host(host: str) -> bool:
    """True iff ``host`` is a concrete address a PEER can dial — NOT loopback (``127.0.0.0/8`` / ``::1``)
    and NOT the bind-all wildcard (``0.0.0.0`` / ``::``). Private ranges (10.x, 192.168.x) ARE routable
    (they are exactly the cluster-internal addresses). A non-IP string is not routable."""
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return False
    return not (ip.is_loopback or ip.is_unspecified)


def _detect_routable_host() -> str | None:
    """A concrete non-loopback IP for this host, or ``None``. Tries the hostname's resolved address,
    then the source address a UDP socket would use toward a public IP (no packet is actually sent)."""
    with contextlib.suppress(OSError):
        candidate = socket.gethostbyname(socket.gethostname())
        if is_routable_host(candidate):
            return candidate
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))  # no packet is sent for a connected UDP socket; picks the source IP
        candidate = s.getsockname()[0]
    except OSError:
        return None
    finally:
        s.close()
    return candidate if is_routable_host(candidate) else None


def select_advertise_host(candidate: str | None = None) -> str:
    """The address to ANNOUNCE to peers (the transport may BIND ``0.0.0.0``, but it must never
    announce loopback/wildcard — a peer cannot dial those). Validates an explicit ``candidate`` or
    auto-detects one; raises ``ValueError`` on loopback/``0.0.0.0`` (or when none is available)."""
    if candidate is not None:
        if not is_routable_host(candidate):
            raise ValueError(f"advertise host {candidate!r} is not routable (loopback or 0.0.0.0)")
        return candidate
    host = _detect_routable_host()
    if host is None:
        raise ValueError("no routable non-loopback address available to advertise")
    return host


class PipeInbox:
    """A cross-process inbox over ``multiprocessing.SimpleQueue``, adapted to the small ``queue.Queue``
    API :class:`QueueTransport` uses (``put_nowait`` / ``get_nowait`` / ``get(timeout)``).

    Why not ``multiprocessing.Queue``: that spawns a background **feeder thread per queue** in every
    process that puts to it, so a peer worker writing to its driver + reduction peers ends up with
    ~``log(N)`` extra threads. With workers ≈ cores (the normal HEP batch slot) those idle-but-scheduled
    threads add context-switch pressure that measurably slows the workers' compute (py-spy: 5 threads/
    worker vs the hub's 1). ``SimpleQueue`` has **no feeder thread** — ``put`` writes the pipe
    synchronously under a lock — so a peer worker holds one extra thread, not ``log(N)``. We wait/poll
    the reader side, which exposes ``poll(timeout)``, for the timed receives.

    ``put_nowait`` therefore **blocks** whenever the message exceeds the OS pipe buffer (~64 KB) until
    the peer reads — a reduction partial (many histograms, or systematic universes) easily does, and a
    parked write holds this inbox's write lock against everyone else. Callers keep that safe by never
    sending from the thread that has to keep reading: see ``_peer._Outbox`` / ``_peer.release_workers``."""

    def __init__(self, ctx: Any) -> None:
        self._q = ctx.SimpleQueue()

    def put_nowait(self, item: object) -> None:
        self._q.put(item)  # synchronous pipe write (no feeder): BLOCKS past the pipe buffer

    def get_nowait(self) -> Any:
        if self._q._reader.poll():
            return self._q.get()
        raise queue.Empty

    def get(self, timeout: float | None = None) -> Any:
        if self._q._reader.poll(timeout):
            return self._q.get()
        raise queue.Empty

    def close(self) -> None:
        with contextlib.suppress(Exception):
            self._q.close()


class QueueTransport:
    """IPC transport: a per-endpoint inbox queue plus a registry of peers' inboxes. ``queue.Queue``
    (threads / in-process) or :class:`PipeInbox` (processes) — it only calls ``put_nowait`` /
    ``get_nowait`` / ``get``, so the queue type is the caller's choice."""

    def __init__(self, address: str, inbox: Any, outboxes: dict[str, Any]) -> None:
        self.address = address
        self._inbox = inbox
        self._outboxes = outboxes  # peer address -> that peer's inbox queue

    def peers(self) -> tuple[str, ...]:
        return tuple(self._outboxes)

    def send(self, dest: str, message: object) -> bool:
        q = self._outboxes.get(dest)
        if q is None:
            return False
        try:
            q.put_nowait((self.address, message))
            return True
        except queue.Full:
            return False  # best-effort: a full inbox drops, never back-pressures the sender

    def broadcast(self, message: object) -> None:
        for dest in self._outboxes:
            self.send(dest, message)

    def poll(self) -> list[tuple[str, object]]:
        out: list[tuple[str, object]] = []
        while True:
            try:
                out.append(self._inbox.get_nowait())
            except queue.Empty:
                return out

    def recv(self, timeout: float | None = None) -> tuple[str, object] | None:
        try:
            return self._inbox.get(timeout=timeout) if timeout is not None else self._inbox.get_nowait()  # type: ignore[no-any-return]
        except queue.Empty:
            return None

    def close(self) -> None:
        # queue.Queue needs no teardown; a multiprocessing.Queue is closed by its owner (the driver).
        pass


def build_ipc_transports(
    addresses: tuple[str, ...], *, maxsize: int = _DEFAULT_MAXSIZE
) -> dict[str, QueueTransport]:
    """Build a fully-connected set of in-process IPC transports (one ``queue.Queue`` inbox each).

    Used for the thread pool and the conformance tests; the process pool builds the same class over
    ``multiprocessing.Queue`` inboxes handed to workers via the pool initializer (P5)."""
    inboxes: dict[str, queue.Queue[Any]] = {addr: queue.Queue(maxsize=maxsize) for addr in addresses}
    return {
        addr: QueueTransport(addr, inboxes[addr], {p: inboxes[p] for p in addresses if p != addr})
        for addr in addresses
    }


class _Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        with contextlib.suppress(Exception):
            sender, message = pickle.loads(body)
            self.server._deliver((sender, message))  # type: ignore[attr-defined]
        self.send_response(200)
        self.end_headers()

    def log_message(self, *args: Any) -> None:  # silence the default stderr access log
        pass


class LookupFreeHTTPServer(ThreadingHTTPServer):
    """A ``ThreadingHTTPServer`` that binds without ``HTTPServer.server_bind``'s ``socket.getfqdn``, a
    reverse lookup that can stall for tens of seconds while the socket is bound but not yet listening."""

    def server_bind(self) -> None:
        socketserver.TCPServer.server_bind(self)
        host, port = self.server_address[:2]
        self.server_name = str(host)
        self.server_port = int(port)


class _InboxServer(LookupFreeHTTPServer):
    # threaded + a deep listen backlog so concurrent worker POSTs are never refused: a refused POST
    # would *drop* a partial, and a dropped partial (unlike dropped telemetry) stalls the reduction.
    daemon_threads = True
    request_queue_size = 256

    def __init__(self, addr: tuple[str, int]) -> None:
        super().__init__(addr, _Handler)
        self._inbox: deque[tuple[str, object]] = deque()
        self._lock = threading.Lock()

    def _deliver(self, item: tuple[str, object]) -> None:
        with self._lock:
            self._inbox.append(item)

    def pop_one(self) -> tuple[str, object] | None:
        with self._lock:
            return self._inbox.popleft() if self._inbox else None

    def drain(self) -> list[tuple[str, object]]:
        with self._lock:
            out = list(self._inbox)
            self._inbox.clear()
        return out


class HttpTransport:
    """HTTP transport over loopback. Each endpoint serves an inbox at ``/msg`` and ships outbound
    messages from background sender threads (so ``send`` is non-blocking). Build a connected set with
    :func:`build_http_transports`, which assigns ports and wires the registry.

    One sender thread PER DESTINATION, created on first use. A single shared sender would deliver in
    global order, so a destination that has stopped answering (a crashed worker whose server is mid-
    shutdown accepts the connection and never responds) holds every later message hostage for its
    retries — including the driver's ``done`` for the workers that are still alive, which is exactly
    what the teardown waits on. Per destination, order is still preserved and a dead peer delays only
    its own messages."""

    def __init__(self, address: str, *, host: str = "127.0.0.1", maxsize: int = _DEFAULT_MAXSIZE) -> None:
        self.address = address
        self._server = _InboxServer((host, 0))
        self.host = str(self._server.server_address[0])
        self.port = int(self._server.server_address[1])
        self._registry: dict[str, tuple[str, int]] = {}
        self._maxsize = maxsize
        self._lanes: dict[str, tuple[queue.Queue[object], threading.Thread]] = {}
        self._lock = threading.Lock()  # guards _lanes and the two counters below
        self.deliveries = 0  # POSTs that got a response (witness/diagnostic)
        self.drops = 0  # messages dropped after exhausting retries
        self._stop = threading.Event()
        self._srv_thread = threading.Thread(
            target=self._server.serve_forever, name=f"gx-http-{address}", daemon=True
        )
        self._srv_thread.start()

    def set_registry(self, registry: dict[str, tuple[str, int]]) -> None:
        self._registry = dict(registry)

    def peers(self) -> tuple[str, ...]:
        return tuple(a for a in self._registry if a != self.address)

    def send(self, dest: str, message: object) -> bool:
        if dest not in self._registry or dest == self.address:
            return False
        try:
            self._lane(dest).put_nowait(message)  # non-blocking; the lane's thread does the POST
            return True
        except queue.Full:
            return False

    def _lane(self, dest: str) -> queue.Queue[object]:
        with self._lock:
            lane = self._lanes.get(dest)
            if lane is None:
                q: queue.Queue[object] = queue.Queue(maxsize=self._maxsize)
                t = threading.Thread(
                    target=self._sender,
                    args=(dest, q),
                    name=f"gx-http-send-{self.address}-{dest}",
                    daemon=True,
                )
                lane = self._lanes[dest] = (q, t)
                t.start()
            return lane[0]

    def broadcast(self, message: object) -> None:
        for dest in self.peers():
            self.send(dest, message)

    def poll(self) -> list[tuple[str, object]]:
        return self._server.drain()

    def recv(self, timeout: float | None = None) -> tuple[str, object] | None:
        # the inbox is server-thread-fed; pop ONE (leaving the rest queued — draining all and
        # returning one would silently drop the rest), with a short sleep up to `timeout`.
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            item = self._server.pop_one()
            if item is not None:
                return item
            if deadline is not None and time.monotonic() >= deadline:
                return None
            self._stop.wait(0.005)
            if self._stop.is_set():
                return None

    def _sender(self, dest: str, lane: queue.Queue[object]) -> None:
        while not self._stop.is_set():
            try:
                message = lane.get(timeout=0.1)
            except queue.Empty:
                continue
            if message is None:
                break
            target = self._registry.get(dest)
            if target is None:
                continue
            body = pickle.dumps((self.address, message))
            url = f"http://{target[0]}:{target[1]}/msg"
            # the sender thread is off the data path, so it RETRIES on a transient failure: peer
            # reduction needs delivery (a lost partial = wrong/missing result), and the receiver
            # dedupes by node identity, so an at-least-once retry is safe.
            delivered = False
            for attempt in range(5):
                try:
                    req = urllib.request.Request(url, data=body, method="POST")
                    urllib.request.urlopen(req, timeout=5).close()
                    delivered = True
                    break
                except (urllib.error.URLError, OSError):
                    if self._stop.is_set():
                        break
                    time.sleep(0.01 * (attempt + 1))
            with self._lock:
                if delivered:
                    self.deliveries += 1
                else:
                    self.drops += 1

    def close(self) -> None:
        # Sentinel FIRST, `_stop` only after the lanes have drained (or the bound expired): setting
        # `_stop` up front made the sender's loop condition false with messages still queued, silently
        # dropping them. Once `_stop` is set the in-retry check abandons the rest, which is what keeps
        # this bounded when a destination is unreachable rather than merely refusing. The lanes drain
        # concurrently, so the bound is shared, not paid once per destination.
        with self._lock:
            lanes = list(self._lanes.values())
        for q, _ in lanes:
            with contextlib.suppress(Exception):
                q.put_nowait(None)
        deadline = time.monotonic() + CLOSE_DRAIN_S
        for _, t in lanes:
            t.join(max(0.0, deadline - time.monotonic()))
        self._stop.set()
        with contextlib.suppress(Exception):
            self._server.shutdown()
        with contextlib.suppress(Exception):
            self._server.server_close()


def build_http_transports(addresses: tuple[str, ...]) -> dict[str, HttpTransport]:
    """Build a connected set of loopback HTTP transports: bind each endpoint's server (so it has a
    port), then hand every endpoint the full ``{address: (host, port)}`` registry."""
    transports = {addr: HttpTransport(addr) for addr in addresses}
    registry = {addr: (t.host, t.port) for addr, t in transports.items()}
    for t in transports.values():
        t.set_registry(registry)
    return transports


def build_transports(kind: str, addresses: tuple[str, ...]) -> dict[str, Any]:
    """Factory: ``"ipc"`` -> :func:`build_ipc_transports`, ``"http"`` -> :func:`build_http_transports`."""
    if kind == "ipc":
        return build_ipc_transports(addresses)
    if kind == "http":
        return build_http_transports(addresses)
    raise ValueError(f"unknown transport kind {kind!r} (expected 'ipc' or 'http')")
