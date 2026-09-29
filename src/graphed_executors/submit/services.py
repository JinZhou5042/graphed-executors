"""The engine's service set: resolve each service a plan declares, check it, probe it from a worker, and
release what was started when the submission that started it ends (plan-services §3.1, D2, D10).

A plan's ``services`` are graphed ``ServiceSpec`` requirements; the engine names none of them. A
:class:`ServiceSet` satisfies each spec by three legs, in order: (1) an endpoint the user gave, checked,
and a failing one refuses the run naming it (an explicit instruction is never silently replaced); (2)
the backend's site endpoint for the spec's ``kind`` (``backend.site_services``, duck-typed), checked, a
failing one passed over with the reason kept in the status; (3) a managed start from the spec's
``launch`` recipe: beside the driver (a stdlib ``Popen``) when the recipe has no image and no GPUs and
the backend offers ``"driver"`` in ``service_hosts``, else on the cluster through the backend's
``host_service``/``release_service`` pair (D10: the capability IS the pair of methods). Nothing left
raises :class:`ServiceUnavailable` naming each leg and why it did not apply.

Readiness is :func:`check_ready`, one function every caller runs: where the service runs, then from a
worker. The worker probe is an ordinary ``backend.submit`` of :func:`_probe_services`, one task that
runs every service's check and answers with :func:`host_identity` and each check's reason; each answer
is awaited up to the largest ``timeout_s`` among the set's services. It is resubmitted under a fresh
key (at most ``max(2, n_workers())`` times) until a host other than a managed service's own answers,
and a service only its own host can reach passes only
when that host is the driver's (one machine). Every acquisition registers its release on an
``ExitStack`` the moment it returns, each release logs its failure and never raises, so the first
exception is the one that surfaces and a raising release never skips a later one.

A service lives in the scope of the submission that started it: :class:`SubmitRunner` enters one set
per ``run`` under that run's nonce. A user keeps a service warm across plans by holding a started set
and passing its :class:`Endpoints` as the runner's ``services`` (leg 1 for every run).
"""

from __future__ import annotations

import contextlib
import errno
import importlib
import logging
import os
import socket
import ssl
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Any
from urllib.parse import urlsplit

from graphed.services import ServiceSpec, split_endpoint

logger = logging.getLogger("graphed_executors.services")

PROBE_CHECK_S = 5.0  # each check a probe task runs
_READY_CHECK_S = 5.0  # each check of a starting managed service, polled until its timeout_s
_POLL_S = 0.1  # between readiness checks of a starting managed service
_GRACE_S = 5.0  # a terminated managed service's time to exit before it is killed
_HEALTH_METHOD = "/grpc.health.v1.Health/Check"
_SERVING = b"\x08\x01"  # HealthCheckResponse{status: SERVING}, protobuf-encoded
_MINTED_SCHEME = {"http": "http", "grpc": "grpc", "tcp": "tcp"}

# One port space per process: a scan frees the port it found before the child binds it, so two
# managed starts in this process hold this lock from the scan until readiness (or failure).
_PORT_LOCK = threading.Lock()


# ---- records and refusals ------------------------------------------------------------------------


@dataclass(frozen=True)
class ServiceStatus:
    """How one service of a set was satisfied: ``leg`` is ``"user"``, ``"site"`` or ``"managed"``;
    ``host`` is ``"driver"`` or ``"cluster"`` for a managed one; ``identity`` is the host a managed
    service runs on (``None`` for user and site endpoints, where any worker's passing answer passes);
    ``detail`` keeps why earlier legs were passed over. Times are ``time.time()`` stamps."""

    name: str
    leg: str
    host: str | None
    endpoint: str
    identity: str | None
    started_at: float | None
    ready_at: float | None
    closed_at: float | None = None
    detail: str = ""


class ServiceUnavailable(Exception):
    """No leg satisfied service ``name``; ``legs`` maps each leg tried to why it did not apply."""

    def __init__(self, name: str, legs: Mapping[str, str]) -> None:
        legs = dict(legs)
        super().__init__(name, legs)  # exactly the constructor's arguments: stdlib pickling rebuilds it
        self.name = name
        self.legs = legs

    def __str__(self) -> str:
        why = "; ".join(f"{leg}: {reason}" for leg, reason in self.legs.items())
        return f"service {self.name!r} is unavailable: {why}"


class ServiceUnreachable(Exception):
    """Service ``name`` at ``endpoint`` did not pass its check from a worker (``worker`` the answering
    host's identity, empty when none answered); ``reason`` says why."""

    def __init__(self, name: str, endpoint: str, worker: str, reason: str) -> None:
        super().__init__(name, endpoint, worker, reason)
        self.name = name
        self.endpoint = endpoint
        self.worker = worker
        self.reason = reason

    def __str__(self) -> str:
        where = f" from worker {self.worker}" if self.worker else ""
        return f"service {self.name!r} at {self.endpoint} is unreachable{where}: {self.reason}"


class Endpoints(Mapping[str, str]):
    """A started set's endpoints (service name -> ``scheme://host:port``). :meth:`on_close` registers a
    callback the set runs when it closes, before it releases any service."""

    def __init__(
        self, endpoints: Mapping[str, str], register: Callable[[Callable[[], object]], None]
    ) -> None:
        self._endpoints = dict(endpoints)
        self._register = register

    def __getitem__(self, name: str) -> str:
        return self._endpoints[name]

    def __iter__(self) -> Iterator[str]:
        return iter(self._endpoints)

    def __len__(self) -> int:
        return len(self._endpoints)

    def __repr__(self) -> str:
        return f"Endpoints({self._endpoints!r})"

    def __reduce__(self) -> tuple[Any, ...]:
        return (dict, (self._endpoints,))  # shipped anywhere, it is only the map

    def on_close(self, callback: Callable[[], object]) -> None:
        self._register(callback)


# ---- host identity and readiness -----------------------------------------------------------------


def host_identity() -> str:
    """This host as the pool names it: ``Machine`` from the ad file ``$_CONDOR_MACHINE_AD`` names (a
    container's own hostname is not the execute node's, and no bindings are needed to read it), else
    ``socket.getfqdn()``."""
    path = os.environ.get("_CONDOR_MACHINE_AD")
    if path and os.path.isfile(path):
        with open(path) as ad:
            for line in ad:
                name, _, value = line.partition("=")
                if name.strip() == "Machine":
                    return value.strip().strip('"')
    return socket.getfqdn()


def minted_endpoint(check: str, host: str, port: int) -> str:
    """The endpoint of a managed (plaintext) instance on ``host:port``: its scheme is the check's wire
    (``http:`` -> ``http://``, ``grpc:`` -> ``grpc://``, ``tcp`` -> ``tcp://``)."""
    scheme = _MINTED_SCHEME.get(check.partition(":")[0], "tcp")
    netloc = f"[{host}]" if ":" in host else host
    return f"{scheme}://{netloc}:{port}"


def check_ready(endpoint: str, check: str, timeout: float) -> str | None:
    """``None`` when ``endpoint`` passes ``check`` within ``timeout`` seconds, else why not.

    ``tcp`` connects. ``http:<path>`` GETs ``<scheme>://<host:port><path>`` and is ready on a 2xx whose
    content-type is not ``application/grpc*`` (a gRPC gateway answers 200 to every path). ``grpc:<svc>``
    is the standard ``grpc.health.v1`` Check of ``<svc>`` (grpcio, imported here), over TLS exactly for
    ``grpcs``, ready iff SERVING. A check runs only over a wire that carries it (``http:`` over
    ``http(s)``, ``grpc:`` over ``grpc(s)``, ``tcp`` over any); a mismatch or an unknown form is refused
    without dialling."""
    try:
        scheme, _hostport = split_endpoint(endpoint)
    except ValueError as exc:
        return str(exc)
    kind, colon, arg = check.partition(":")
    if check == "tcp":
        return _check_tcp(endpoint, timeout)
    if kind == "http" and colon and arg.startswith("/"):
        if scheme not in ("http", "https"):
            return f"check {check!r} runs over http:// or https://, not {scheme}:// ({endpoint}); not dialled"
        return _check_http(f"{endpoint}{arg}", timeout)
    if kind == "grpc" and colon:
        if scheme not in ("grpc", "grpcs"):
            return f"check {check!r} runs over grpc:// or grpcs://, not {scheme}:// ({endpoint}); not dialled"
        return _check_grpc(endpoint, arg, tls=scheme == "grpcs", timeout=timeout)
    return f"unknown check {check!r}: 'tcp', 'http:<path>' or 'grpc:<service>'; not dialled"


def _check_tcp(endpoint: str, timeout: float) -> str | None:
    parts = urlsplit(endpoint)
    try:
        with socket.create_connection((str(parts.hostname), int(parts.port or 0)), timeout=timeout):
            return None
    except OSError as exc:
        return f"tcp connect to {endpoint} failed: {exc}"


def _check_http(url: str, timeout: float) -> str | None:
    # a service endpoint is dialled directly: an environment proxy never sits between a job and it
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=ssl.create_default_context())
    )
    try:
        with opener.open(url, timeout=timeout) as resp:
            status, ctype = int(resp.status), str(resp.headers.get("content-type", ""))
    except urllib.error.HTTPError as exc:  # urllib raises for every answer but a 2xx
        return f"GET {url} answered {exc.code}"
    except (OSError, ValueError) as exc:
        return f"GET {url} failed: {exc}"
    if ctype.startswith("application/grpc"):
        return f"GET {url} answered {status} {ctype}: a gRPC endpoint, not an HTTP service"
    return None


def _varint(n: int) -> bytes:
    out = bytearray()
    while True:
        low, n = n & 0x7F, n >> 7
        out.append(low | (0x80 if n else 0))
        if not n:
            return bytes(out)


def _check_grpc(endpoint: str, service: str, *, tls: bool, timeout: float) -> str | None:
    """The health Check with grpcio alone: the one-field request and the reply as raw protobuf bytes
    (generated stubs would add a dependency for a two-byte message)."""
    try:
        grpc: Any = importlib.import_module("grpc")
    except ImportError as exc:
        return f"check 'grpc:{service}' needs grpcio, which is not importable here ({exc})"
    hostport = split_endpoint(endpoint)[1]
    name = service.encode()
    request = b"\x0a" + _varint(len(name)) + name if name else b""
    if tls:
        channel = grpc.secure_channel(hostport, grpc.ssl_channel_credentials())
    else:
        channel = grpc.insecure_channel(hostport)
    try:
        reply = channel.unary_unary(_HEALTH_METHOD)(request, timeout=timeout)
    except grpc.RpcError as exc:
        return f"health Check of {service!r} at {endpoint} failed: {exc.code()} {exc.details()}"
    finally:
        channel.close()
    if reply != _SERVING:
        return f"health Check of {service!r} at {endpoint} answered {bytes(reply)!r}, not SERVING"
    return None


def _probe_services(checks: Sequence[tuple[str, str]]) -> tuple[str, tuple[str | None, ...]]:
    """The worker probe task: this worker's :func:`host_identity` and each ``(endpoint, check)``'s
    reason (``None`` = ready)."""
    return host_identity(), tuple(check_ready(endpoint, check, PROBE_CHECK_S) for endpoint, check in checks)


# ---- releases --------------------------------------------------------------------------------------


def release_quietly(what: str, fn: Callable[..., object], *args: object) -> None:
    """Run one release step; its failure is logged, never raised, so the first exception is the one
    that surfaces and a later release still runs. Every release in this package goes through it."""
    try:
        fn(*args)
    except Exception:
        logger.warning("releasing %s failed", what, exc_info=True)


def _stop_child(proc: subprocess.Popen[bytes], name: str) -> None:
    """Terminate a managed service, wait ``_GRACE_S``, then kill it, and reap it either way."""
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(_GRACE_S)
        except subprocess.TimeoutExpired:
            logger.warning("service %r (pid %d) ignored terminate; killing it", name, proc.pid)
            proc.kill()
    proc.wait()


def _free_port(host: str, ports: tuple[int, int]) -> int:
    """The first port of the inclusive range that binds on ``host`` now and that nothing answers a
    connect on; the last error when none does.

    On POSIX the scan sets ``SO_REUSEADDR`` so a port whose earlier connections sit in TIME_WAIT is
    not skipped (the managed child binds with it too, as ``http.server`` does; without it back-to-back
    runs walk off a small range). On Linux a listener still refuses that bind, but on macOS/BSD
    ``SO_REUSEADDR`` lets a specific-address bind pass a port another process holds on the wildcard
    address, and on Windows a non-exclusive wildcard listener does not refuse a specific bind either;
    the connect catches both: a port something listens on is never handed out."""
    low, high = ports
    error: OSError = OSError(f"no port in {low}-{high}")
    family = socket.AF_INET6 if ":" in host else socket.AF_INET
    for port in range(low, high + 1):
        with socket.socket(family, socket.SOCK_STREAM) as sock:
            if sys.platform != "win32":
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind((host, port))
            except OSError as exc:
                error = exc
                continue
        with socket.socket(family, socket.SOCK_STREAM) as probe:
            probe.settimeout(1.0)
            if probe.connect_ex((host, port)) == 0:
                error = OSError(errno.EADDRINUSE, f"port {port} on {host} answers a connect")
                continue
        return port
    raise error


def _render(argv: Sequence[str], host: str, port: int) -> list[str]:
    subs = {"{port}": str(port), "{host}": host, "{python}": sys.executable}
    out = []
    for arg in argv:
        for token, value in subs.items():
            arg = arg.replace(token, value)
        out.append(arg)
    return out


# ---- the set ---------------------------------------------------------------------------------------


@dataclass(frozen=True)
class _Resolved:
    spec: ServiceSpec
    status: ServiceStatus


class ServiceSet:
    """The services ``specs`` names, satisfied on ``backend`` by the three legs; a context manager
    (``__enter__`` = :meth:`start`, ``__exit__`` = :meth:`close`).

    ``endpoints`` are leg-1 endpoints by service name, each ``scheme://host:port`` (refused here
    otherwise). ``scope`` names this set's probe keys and the keys ``host_service`` mints: a
    submission passes its run nonce; a set held by the user mints its own."""

    def __init__(
        self,
        specs: Sequence[ServiceSpec],
        backend: Any,
        *,
        endpoints: Mapping[str, str] | None = None,
        scope: str | None = None,
    ) -> None:
        given = dict(endpoints or {})
        for endpoint in given.values():
            split_endpoint(endpoint)
        self.specs = tuple(specs)
        self.backend = backend
        self.scope = scope if scope is not None else uuid.uuid4().hex[:8]
        self._given = given
        self._stack: contextlib.ExitStack | None = None
        self._statuses: list[ServiceStatus] = []
        self._driver: str | None = None

    def __enter__(self) -> Endpoints:
        return self.start()

    def __exit__(self, *exc: object) -> None:
        self.close()

    def statuses(self) -> list[ServiceStatus]:
        return list(self._statuses)

    def start(self) -> Endpoints:
        """Resolve, check and probe every spec; on any failure release what was acquired and raise."""
        if self._stack is not None:
            raise RuntimeError("this service set is already started")
        self._statuses = []
        with contextlib.ExitStack() as stack:
            stack.callback(self._stamp_closed)
            resolved = [self._resolve(spec, stack) for spec in self.specs]
            self._probe(resolved, stack)
            self._stack = stack.pop_all()
        return Endpoints({r.spec.name: r.status.endpoint for r in resolved}, self._on_close)

    def close(self) -> None:
        """Run the ``on_close`` callbacks, then release every service this set started."""
        stack, self._stack = self._stack, None
        if stack is not None:
            stack.close()

    # ---- the legs ----

    def _resolve(self, spec: ServiceSpec, stack: contextlib.ExitStack) -> _Resolved:
        legs: dict[str, str] = {}
        given = self._given.get(spec.name)
        if given is not None:
            reason = check_ready(given, spec.check, spec.timeout_s)
            if reason is not None:
                raise ServiceUnavailable(spec.name, {"user": f"the given endpoint {given} failed: {reason}"})
            return self._record(spec, "user", None, given, None, None)
        legs["user"] = "no endpoint given"
        site = getattr(self.backend, "site_services", None) or {}
        endpoint = site.get(spec.kind)
        if endpoint is None:
            legs["site"] = f"the site has no endpoint for kind {spec.kind!r}"
        else:
            reason = check_ready(endpoint, spec.check, spec.timeout_s)
            if reason is None:
                return self._record(spec, "site", None, endpoint, None, None)
            legs["site"] = f"the site endpoint {endpoint} failed: {reason}"
        return self._managed(spec, legs, stack)

    def _managed(self, spec: ServiceSpec, legs: dict[str, str], stack: contextlib.ExitStack) -> _Resolved:
        launch = spec.launch
        detail = "; ".join(f"{leg}: {why}" for leg, why in legs.items())
        if launch is None:
            raise ServiceUnavailable(spec.name, {**legs, "managed": "the spec has no launch recipe"})
        hosts = tuple(getattr(self.backend, "service_hosts", ("driver",)))
        if launch.image is None and not launch.resources.get("gpus", 0) and "driver" in hosts:
            return self._on_driver(spec, legs, detail, stack)
        host_service = getattr(self.backend, "host_service", None)
        if not callable(host_service):
            where = "an image or GPUs" if "driver" in hosts else f"service_hosts={hosts!r}"
            raise ServiceUnavailable(
                spec.name,
                {
                    **legs,
                    "managed": f"the recipe cannot run beside the driver ({where}) and "
                    f"{type(self.backend).__name__} has no host_service to run it on the cluster",
                },
            )
        started = time.time()
        endpoint, identity, key = host_service(spec, self.scope)
        stack.callback(release_quietly, f"service {spec.name!r} ({key})", self.backend.release_service, key)
        return self._record(spec, "managed", "cluster", endpoint, identity, started, detail)

    def _on_driver(
        self, spec: ServiceSpec, legs: dict[str, str], detail: str, stack: contextlib.ExitStack
    ) -> _Resolved:
        assert spec.launch is not None
        host = getattr(self.backend, "advertise_host", None) or socket.getfqdn()
        ports = getattr(self.backend, "service_ports", None) or spec.ports
        identity = self._driver_identity()
        with _PORT_LOCK:  # held from the scan until ready or failed; its wait is outside timeout_s
            started = time.time()
            port = _free_port(host, ports)
            env = {**os.environ, **spec.launch.env}
            proc = subprocess.Popen(_render(spec.launch.argv, host, port), env=env)
            stack.callback(
                release_quietly, f"service {spec.name!r} (pid {proc.pid})", _stop_child, proc, spec.name
            )
            endpoint = minted_endpoint(spec.check, host, port)
            deadline = time.monotonic() + spec.timeout_s
            while True:
                code = proc.poll()
                if code is not None:
                    reason = f"{proc.args!r} exited with returncode {code} before {spec.check!r} passed"
                    raise ServiceUnavailable(spec.name, {**legs, "managed": reason})
                left = deadline - time.monotonic()
                reason_now = check_ready(endpoint, spec.check, max(0.1, min(left, _READY_CHECK_S)))
                if reason_now is None:
                    code = proc.poll()
                    if code is None:
                        break
                    reason = f"{spec.check!r} passed but the service exited with returncode {code}"
                    raise ServiceUnavailable(spec.name, {**legs, "managed": reason})
                if time.monotonic() >= deadline:
                    reason = (
                        f"{spec.check!r} never passed on {endpoint} within {spec.timeout_s}s: {reason_now}"
                    )
                    raise ServiceUnavailable(spec.name, {**legs, "managed": reason})
                time.sleep(_POLL_S)
        return self._record(spec, "managed", "driver", endpoint, identity, started, detail)

    def _record(
        self,
        spec: ServiceSpec,
        leg: str,
        host: str | None,
        endpoint: str,
        identity: str | None,
        started_at: float | None,
        detail: str = "",
    ) -> _Resolved:
        status = ServiceStatus(
            spec.name, leg, host, endpoint, identity, started_at, time.time(), detail=detail
        )
        self._statuses.append(status)
        where = f" ({host})" if host is not None else ""
        logger.info("service %r: %s leg%s at %s", spec.name, leg, where, endpoint, extra={"status": status})
        return _Resolved(spec, status)

    def _driver_identity(self) -> str:
        if self._driver is None:
            own = getattr(self.backend, "host_identity", None)
            self._driver = str(own()) if callable(own) else host_identity()
        return self._driver

    # ---- the worker probe ----

    def _probe(self, resolved: Sequence[_Resolved], stack: contextlib.ExitStack) -> None:
        """Probe every endpoint from a worker until each passes: a managed service needs an answer
        from a host other than its own, or its own host must be the driver's. One task checks every
        service; each answer is awaited up to the largest ``timeout_s`` among the set's services (the
        set's one bound, not a per-service one), and at most ``max(2, n_workers())`` tasks are sent."""
        if not resolved:
            return
        checks = [(r.status.endpoint, r.spec.check) for r in resolved]
        timeout = max(r.spec.timeout_s for r in resolved)
        pending = list(range(len(resolved)))
        worker = ""
        for i in range(max(2, int(self.backend.n_workers()))):
            fut = self.backend.submit(_probe_services, checks, key=f"svc-{self.scope}-probe-{i}")
            stack.callback(release_quietly, f"probe task {i}", self._cancel_pending, fut)
            try:
                worker, answers = fut.result(timeout)
            except TimeoutError:
                first = resolved[pending[0]]
                raise ServiceUnreachable(
                    first.spec.name, first.status.endpoint, "", f"no worker answered within {timeout}s"
                ) from None
            for j in pending:
                if answers[j] is not None:
                    r = resolved[j]
                    raise ServiceUnreachable(r.spec.name, r.status.endpoint, worker, str(answers[j]))
            pending = [j for j in pending if not self._passes(resolved[j].status.identity, worker)]
            if not pending:
                return
        first = resolved[pending[0]]
        raise ServiceUnreachable(
            first.spec.name, first.status.endpoint, worker, "only same-host workers answered"
        )

    def _passes(self, service: str | None, worker: str) -> bool:
        return service is None or worker != service or service == self._driver_identity()

    def _cancel_pending(self, fut: Any) -> None:
        if not fut.done():
            self.backend.cancel([fut])

    # ---- close ----

    def _on_close(self, callback: Callable[[], object]) -> None:
        if self._stack is None:
            raise RuntimeError("this service set is not open")
        self._stack.callback(release_quietly, f"on_close callback {callback!r}", callback)

    def _stamp_closed(self) -> None:
        now = time.time()
        self._statuses = [replace(s, closed_at=now) for s in self._statuses]


__all__ = [
    "Endpoints",
    "ServiceSet",
    "ServiceStatus",
    "ServiceUnavailable",
    "ServiceUnreachable",
    "check_ready",
    "host_identity",
    "minted_endpoint",
    "release_quietly",
]
