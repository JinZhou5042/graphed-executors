"""Shared harness for the m68a frozen suite: the engine service set, driver-hosted and site legs, and
driverless endpoints (plan-services.md §3.1, D1-D10).

Frozen suites never import across directories, so the m67 pieces this suite needs are copied here
(``RecordingSchedd``/``FakeHTCondor``/``record_bindings``, ``job_dir``, ``write_machine_ad``, the bounded
runners). What is new:

- ``RecordingBackend``: wraps any ``SubmitBackend`` and records every submit (key, placement hints, the
  ``RunContext`` nonce of a plan task), every cancel (by key), every task that actually ran, and every
  probe answer; an optional gate holds its inner single worker until the test opens it, and an optional
  fault raises at a chosen submit.
- ``HostFake``: a ``SubmitBackend`` with the duck-typed service attributes (``host_identity``,
  ``advertise_host``, ``site_services``, ``service_hosts`` and, when hosting, ``host_service`` /
  ``release_service``). Its driver is on one host, its hosted services on another, and each probe task
  runs as a worker on a host of the test's choosing (``$_CONDOR_MACHINE_AD`` set around the call).
- Spy plan parts (``SpyProcess``, ``SpyReduce``) that record ``bind_services``/``resolve_services`` into
  one ordered trace shared with the backends, and GET their bound endpoint.
- In-process servers with accept counters (HTTP, a bare TCP listener, a first-bytes sniffing proxy, gRPC
  health servers) and the ``service_child.py`` managed child, which reports its own pid.
- ``pid_gone`` / ``port_free``: the reaped-pid and freed-port witnesses, on every OS.

The implementation under test is reached only through the ``*_api()`` accessors, inside test bodies,
so the suite collects before ``submit/services.py`` and ``submit/recipes.py`` exist. Everything a pilot
or a driver job unpickles is module-level here; the driverless tests ship this file as a user module.
"""

from __future__ import annotations

import contextlib
import dataclasses
import importlib
import json
import logging
import os
import pickle
import secrets
import shutil
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterator, Mapping, Sequence
from concurrent.futures import Future
from dataclasses import dataclass, field, replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar

from graphed.core.execution import Partition, Plan, Task
from graphed.debug import SourceFrame, StageError
from graphed.services import Launch, ServiceSpec, UnboundService

if TYPE_CHECKING:
    import pytest

HARNESS_DIR = str(Path(__file__).resolve().parent)
HARNESS_FILE = str(Path(__file__).resolve())
CHILD_SCRIPT = str(Path(HARNESS_DIR) / "service_child.py")
DATA_DIR = Path(HARNESS_DIR) / "data"
TRITON_MODELS = DATA_DIR / "triton_models"

SERVICES_LOGGER = "graphed_executors.services"
RUN_TIMEOUT_S = 240.0
HTTP_TIMEOUT_S = 5.0
GATE_TIMEOUT_S = 120.0  # a gate the test never opens still lets the worker go

FAKE_POOL = "cm.m68a.example:9618"
FAKE_SCHEDD = "schedd.m68a.example"
FAKE_CLUSTER = 4242

DRIVER_HOST = "host-a.m68a.example"
SERVICE_HOST = "host-b.m68a.example"
THIRD_HOST = "host-c.m68a.example"

# ---- deferred accessors for the implementation under test ---------------------------------------


def services_api() -> Any:
    """``graphed_executors.submit.services``: ServiceSet, ServiceStatus, ServiceUnavailable,
    ServiceUnreachable, Endpoints, check_ready, host_identity, _probe_services."""
    return importlib.import_module("graphed_executors.submit.services")


def recipes_api() -> Any:
    """``graphed_executors.submit.recipes``: triton, http_server."""
    return importlib.import_module("graphed_executors.submit.recipes")


def submit_api() -> Any:
    """``graphed_executors.submit``: SubmitRunner, ThreadBackend, RunContext."""
    return importlib.import_module("graphed_executors.submit")


def local_api() -> Any:
    """``graphed_executors.local``: ThreadExecutor."""
    return importlib.import_module("graphed_executors.local")


def htcondor_api() -> Any:
    """``graphed_executors.htcondor_backend``: submit_driverless, RunHandle, SITES, SiteProfile, ..."""
    return importlib.import_module("graphed_executors.htcondor_backend")


def launch_api() -> Any:
    """``graphed_executors.htcondor_backend.launch``: every bindings call goes through ``_htcondor()``."""
    return importlib.import_module("graphed_executors.htcondor_backend.launch")


def backend_api() -> Any:
    """``graphed_executors.htcondor_backend.backend``: HTCondorBackend, HTCondorRunner."""
    return importlib.import_module("graphed_executors.htcondor_backend.backend")


def driver_api() -> Any:
    """``graphed_executors.htcondor_backend.driver``: main, _runner, _exit_code, _result_blob."""
    return importlib.import_module("graphed_executors.htcondor_backend.driver")


def server_api() -> Any:
    """``graphed_executors.htcondor_backend.server``: WorkerLost."""
    return importlib.import_module("graphed_executors.htcondor_backend.server")


DRIVER_MODULE = "graphed_executors.htcondor_backend.driver"


def require_module(name: str) -> Any:
    """``name`` imported. Where the GIL is enabled (or the interpreter predates the question) a
    missing module is a failure; only a free-threaded interpreter, which has no grpcio wheel, skips."""
    gil_enabled = getattr(sys, "_is_gil_enabled", None)
    if gil_enabled is None or gil_enabled():
        return importlib.import_module(name)
    pytest_mod = importlib.import_module("pytest")
    return pytest_mod.importorskip(name, reason=f"{name} has no wheel for a free-threaded interpreter")


# ---- bounds (copied from m67) --------------------------------------------------------------------


def run_bounded(fn: Callable[[], Any], timeout_s: float = RUN_TIMEOUT_S) -> Any:
    """Run ``fn`` on a daemon thread and fail if it does not finish in ``timeout_s``: a hang is a
    failure, never a wedged CI job. Returns the value or re-raises the call's exception."""
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


def wait_for(predicate: Callable[[], bool], timeout_s: float = 30.0) -> bool:
    """Poll a predicate within a bound; returns its last value (the assertion comes after)."""
    deadline = time.monotonic() + timeout_s
    while not predicate():
        if time.monotonic() >= deadline:
            return predicate()
        time.sleep(0.05)
    return True


# ---- the reaped-pid and freed-port witnesses -----------------------------------------------------


def pid_gone(pid: int) -> bool:
    """Whether ``pid`` is no longer a live (or unreaped) process. POSIX: ``os.kill(pid, 0)`` raises
    ``ProcessLookupError`` once the parent has waited on it (a zombie still answers). Windows, where
    ``os.kill(pid, 0)`` would terminate the process, asks ``GetExitCodeProcess`` instead."""
    if sys.platform == "win32":
        import ctypes  # noqa: PLC0415  (Windows only)

        kernel32 = ctypes.windll.kernel32
        handle = kernel32.OpenProcess(0x1000, False, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
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
    """Teardown backstop for a child the implementation failed to stop."""
    if pid_gone(pid):
        return
    with contextlib.suppress(OSError):
        os.kill(pid, 9 if sys.platform != "win32" else 15)


def _bindable(host: str, port: int) -> bool:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        if sys.platform != "win32":  # skip TIME_WAIT only; a LISTEN socket still refuses the bind
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, port))
        return True
    except OSError:
        return False
    finally:
        s.close()


def _listening(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1.0)
        return s.connect_ex(("127.0.0.1", port)) == 0


def port_free(port: int) -> bool:
    """Nothing listens on ``port``: it binds on loopback and on all interfaces, and a connect is refused."""
    return _bindable("127.0.0.1", port) and _bindable("", port) and not _listening(port)


def free_range(n: int = 3) -> tuple[int, int]:
    """An inclusive range of ``n`` consecutive ports that are free right now (above the 10000-10100 band
    the site rows and task servers use)."""
    for _ in range(200):
        low = 20000 + secrets.randbelow(30000)
        if all(port_free(p) for p in range(low, low + n)):
            return (low, low + n - 1)
    raise AssertionError(f"no {n} consecutive free ports found")


def closed_port() -> int:
    """A port nothing listens on (bound once, then released)."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@contextlib.contextmanager
def bounded_set(service_set: Any, timeout_s: float = RUN_TIMEOUT_S) -> Iterator[Any]:
    """``with service_set as eps`` with ``start`` and ``close`` each bounded: a hang fails the test."""
    endpoints = run_bounded(service_set.start, timeout_s)
    try:
        yield endpoints
    finally:
        run_bounded(service_set.close, timeout_s)


@contextlib.contextmanager
def closing_bounded(obj: Any, timeout_s: float = RUN_TIMEOUT_S) -> Iterator[Any]:
    """``with obj`` whose ``close()`` is bounded: a hanging close fails the test."""
    try:
        yield obj
    finally:
        run_bounded(obj.close, timeout_s)


@contextlib.contextmanager
def hold_port(port: int) -> Iterator[socket.socket]:
    """A listening socket on ``127.0.0.1:port`` for the block (exclusive on Windows): a managed service
    advertised on loopback cannot bind it."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        if sys.platform == "win32":
            s.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        s.bind(("127.0.0.1", port))
        s.listen(8)
        yield s
    finally:
        s.close()


def endpoint_port(endpoint: str) -> int:
    return int(endpoint.rsplit(":", 1)[1])


# ---- a backstop for children that report no pid ------------------------------------------------


class _RecordingPopen(subprocess.Popen[Any]):
    """``subprocess.Popen`` that remembers every process it started, so a child that writes no pid
    report (``python -m http.server`` from ``recipes.http_server``) can still be stopped at teardown."""

    started: ClassVar[list[subprocess.Popen[Any]]] = []

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        _RecordingPopen.started.append(self)


@contextlib.contextmanager
def popen_backstop() -> Iterator[list[subprocess.Popen[Any]]]:
    """For the block, ``subprocess.Popen`` records what it starts; at exit any recorded process still
    alive is killed and waited on (a correct implementation has already stopped them)."""
    real = subprocess.Popen
    _RecordingPopen.started = []
    subprocess.Popen = _RecordingPopen  # the stdlib class, patched for the block
    try:
        yield _RecordingPopen.started
    finally:
        subprocess.Popen = real
        for proc in _RecordingPopen.started:
            if proc.poll() is None:
                proc.kill()
                with contextlib.suppress(subprocess.TimeoutExpired):
                    proc.wait(30)


# ---- the managed child and its report ------------------------------------------------------------


def read_report(path: Path, timeout_s: float = 30.0) -> dict[str, Any]:
    """The ``{"pid", "python"}`` a ``service_child.py`` wrote at start."""
    wait_for(path.is_file, timeout_s)
    assert path.is_file(), f"the managed child never started: no report at {path}"
    report: dict[str, Any] = json.loads(path.read_text())
    return report


def child_launch(mode: str, report: Path) -> Launch:
    return Launch(argv=("{python}", CHILD_SCRIPT, mode, "{port}", str(report)))


def child_spec(
    name: str,
    report: Path,
    *,
    mode: str = "serve",
    ports: tuple[int, int],
    check: str = "http:/",
    kind: str = "http",
    timeout_s: float = 30.0,
) -> ServiceSpec:
    """A driver-hostable requirement (no image, no GPUs) whose recipe runs ``service_child.py``."""
    return ServiceSpec(
        name, kind, check=check, ports=ports, launch=child_launch(mode, report), timeout_s=timeout_s
    )


def hosted_spec(
    name: str, *, kind: str = "http", check: str = "http:/", timeout_s: float = 30.0
) -> ServiceSpec:
    """A requirement whose recipe has an image, so only a backend with ``host_service`` can start it."""
    launch = Launch(argv=("serve", "--port", "{port}"), image="registry.m68a.example/service:1")
    return ServiceSpec(name, kind, check=check, ports=(40000, 40010), launch=launch, timeout_s=timeout_s)


def gpu_spec(name: str, *, timeout_s: float = 30.0) -> ServiceSpec:
    """An image-less recipe that asks for a GPU: never driver-hosted."""
    launch = Launch(argv=("{python}", CHILD_SCRIPT, "serve", "{port}", "unused"), resources={"gpus": 1})
    return ServiceSpec(name, "http", check="http:/", ports=(40000, 40010), launch=launch, timeout_s=timeout_s)


def bare_spec(
    name: str, *, kind: str = "http", check: str = "http:/", timeout_s: float = 30.0
) -> ServiceSpec:
    """A requirement without a recipe: legs 1-2 only."""
    return ServiceSpec(name, kind, check=check, ports=(40000, 40010), launch=None, timeout_s=timeout_s)


# ---- one ordered trace shared by backends and spies ----------------------------------------------

_TRACE_LOCK = threading.Lock()
TRACE: list[tuple[Any, ...]] = []


def trace(*event: Any) -> None:
    with _TRACE_LOCK:
        TRACE.append(event)


def trace_reset() -> None:
    with _TRACE_LOCK:
        TRACE.clear()


def trace_snapshot() -> list[tuple[Any, ...]]:
    with _TRACE_LOCK:
        return list(TRACE)


def is_probe_key(key: str) -> bool:
    return key.startswith("svc-") and "-probe-" in key


def is_plan_key(key: str) -> bool:
    return key.startswith("graphed-")


def probe_scope(key: str) -> str:
    """``svc-<scope>-probe-<i>`` -> ``<scope>``."""
    assert is_probe_key(key), key
    return key[len("svc-") : key.rindex("-probe-")]


# ---- HTTP -----------------------------------------------------------------------------------------


def http_get(url: str, timeout_s: float = HTTP_TIMEOUT_S) -> str:
    """The body of a 2xx answer; anything else raises."""
    with urllib.request.urlopen(url, timeout=timeout_s) as resp:
        return str(resp.read().decode())


def http_alive(url: str) -> bool:
    try:
        http_get(url, 2.0)
    except (OSError, urllib.error.URLError):
        return False
    return True


def to_http(endpoint: str) -> str:
    """``http://host:port`` for an ``http``/``tcp`` endpoint a test GETs."""
    scheme, _, rest = endpoint.partition("://")
    assert scheme in ("http", "tcp"), endpoint
    return f"http://{rest}"


# ---- spy plan parts -------------------------------------------------------------------------------

_SPY_LOCK = threading.Lock()
SPY_EVENTS: dict[str, list[tuple[Any, ...]]] = {}
ON_CLOSE: dict[str, str] = {}  # tag -> "record" | "raise": the spy registers an Endpoints.on_close


def spy_record(tag: str, kind: str, *data: Any) -> None:
    with _SPY_LOCK:
        SPY_EVENTS.setdefault(tag, []).append((kind, *data))
    trace(kind, tag, *data)


def spy_events(tag: str, kind: str | None = None) -> list[tuple[Any, ...]]:
    with _SPY_LOCK:
        got = list(SPY_EVENTS.get(tag, []))
    return [e for e in got if kind is None or e[0] == kind]


def spy_reset() -> None:
    with _SPY_LOCK:
        SPY_EVENTS.clear()
        ON_CLOSE.clear()


@dataclass(frozen=True)
class Resolved:
    """What a spy's ``resolve_services`` returns: the value it was given and the service's answer then."""

    tag: str
    value: Any
    body: str | None


def _register_on_close(tag: str, endpoints: Mapping[str, str], url: str | None) -> None:
    mode = ON_CLOSE.get(tag)
    if mode is None:
        return
    on_close = getattr(endpoints, "on_close", None)
    if on_close is None:
        spy_record(tag, "no-on_close", type(endpoints).__name__)
        return

    def callback() -> None:
        spy_record(tag, "on_close", url is not None and http_alive(url))
        if mode == "raise":
            raise RuntimeError(f"on_close fault injected by {tag}")

    on_close(callback)


@dataclass(frozen=True)
class SpyProcess:
    """A plan ``process`` that is ``Bindable`` and ``Resolvable``: a leaf GETs ``path`` on the bound
    endpoint of ``service`` and returns ``(body,)``. Pickled into workers, so it records into the
    module-level ``SPY_EVENTS`` (in-process backends) and returns what it saw."""

    tag: str
    service: str | None = "web"
    path: str = "/"
    endpoint: str | None = None

    def bind_services(self, endpoints: Mapping[str, str]) -> SpyProcess:
        spy_record(self.tag, "bind", dict(endpoints))
        if self.service is None:
            return self
        endpoint = endpoints.get(self.service, self.endpoint)
        if endpoint is None:
            raise UnboundService(self.service)
        _register_on_close(self.tag, endpoints, to_http(endpoint) + self.path)
        return replace(self, endpoint=endpoint)

    def __call__(self, partition: Partition, resources: object) -> tuple[str, ...]:
        spy_record(self.tag, "call", partition.uri)
        if self.service is None:
            return (partition.uri,)
        assert self.endpoint is not None, f"{self.tag}: process called unbound"
        return (http_get(to_http(self.endpoint) + self.path),)

    def resolve_services(self, value: Any) -> Resolved:
        body: str | None = None
        if self.endpoint is not None:
            with contextlib.suppress(OSError, urllib.error.URLError):
                body = http_get(to_http(self.endpoint) + self.path, 2.0)
        out = Resolved(self.tag, value, body)
        spy_record(self.tag, "resolve", value, body is not None, out)
        return out


@dataclass(frozen=True)
class SpyReduce:
    """An ``aggregate_plan`` ``reduce`` with the same hooks: it GETs the bound endpoint per partition."""

    tag: str
    service: str = "web"
    path: str = "/"
    endpoint: str | None = None

    def bind_services(self, endpoints: Mapping[str, str]) -> SpyReduce:
        spy_record(self.tag, "bind", dict(endpoints))
        endpoint = endpoints.get(self.service, self.endpoint)
        if endpoint is None:
            raise UnboundService(self.service)
        return replace(self, endpoint=endpoint)

    def __call__(self, values: list[Any]) -> tuple[str, ...]:
        spy_record(self.tag, "call", len(values))
        assert self.endpoint is not None, f"{self.tag}: reduce called unbound"
        return (http_get(to_http(self.endpoint) + self.path),)

    def resolve_services(self, value: Any) -> Resolved:
        body: str | None = None
        if self.endpoint is not None:
            with contextlib.suppress(OSError, urllib.error.URLError):
                body = http_get(to_http(self.endpoint) + self.path, 2.0)
        out = Resolved(self.tag, value, body)
        spy_record(self.tag, "resolve", value, body is not None, out)
        return out


def concat(a: tuple[str, ...], b: tuple[str, ...]) -> tuple[str, ...]:
    return a + b


def empty_tuple() -> tuple[str, ...]:
    return ()


def mem_partitions(n: int, tag: str) -> tuple[Partition, ...]:
    return tuple(Partition(f"mem://{tag}/{i}", "", i, i + 1) for i in range(n))


def spy_plan(process: Any, n: int, specs: Sequence[ServiceSpec], tag: str | None = None) -> Plan[Any]:
    tasks = tuple(Task(i, p) for i, p in enumerate(mem_partitions(n, tag or getattr(process, "tag", "m68a"))))
    return Plan(process=process, combine=concat, empty=empty_tuple, tasks=tasks, services=tuple(specs))


def text_process(partition: Partition, resources: object) -> tuple[str, ...]:
    return (partition.uri,)


def plain_plan(n: int, tag: str) -> Plan[Any]:
    tasks = tuple(Task(i, p) for i, p in enumerate(mem_partitions(n, tag)))
    return Plan(process=text_process, combine=concat, empty=empty_tuple, tasks=tasks)


# ---- driverless plan parts (module-level: the job side imports this file by name) ----------------


@dataclass(frozen=True)
class GetProcess:
    """A driverless plan process: each pilot task GETs ``/`` on the bound endpoint and returns
    ``((pilot pid, body),)``; ``resolve_services`` GETs again in the driver and wraps the value."""

    service: str = "web"
    endpoint: str | None = None
    fail: str | None = None  # "ValueError" | "OSError": the task raises that instead

    def bind_services(self, endpoints: Mapping[str, str]) -> GetProcess:
        endpoint = endpoints.get(self.service, self.endpoint)
        if endpoint is None:
            raise UnboundService(self.service)
        return replace(self, endpoint=endpoint)

    def __call__(self, partition: Partition, resources: object) -> tuple[tuple[int, str], ...]:
        if self.fail == "ValueError":
            raise ValueError(f"negative pt in {partition.uri}")
        if self.fail == "OSError":
            raise OSError(f"cannot open {partition.uri}")
        assert self.endpoint is not None, "process called unbound"
        return ((os.getpid(), http_get(to_http(self.endpoint) + "/")),)

    def resolve_services(self, value: Any) -> DriverResolved:
        assert self.endpoint is not None, "resolve_services called on an unbound process"
        return DriverResolved(value, os.getpid(), self.endpoint, http_get(to_http(self.endpoint) + "/"))


@dataclass(frozen=True)
class DriverResolved:
    """A driverless run's value after the driver resolved it: the leaves, the driver's pid, the endpoint
    and the body the service answered while it was up."""

    leaves: Any
    driver_pid: int
    endpoint: str
    body: str


def pair_concat(a: tuple[Any, ...], b: tuple[Any, ...]) -> tuple[Any, ...]:
    return a + b


def get_plan(n: int, tag: str, specs: Sequence[ServiceSpec], fail: str | None = None) -> Plan[Any]:
    tasks = tuple(Task(i, p) for i, p in enumerate(mem_partitions(n, tag)))
    return Plan(
        process=GetProcess(fail=fail),
        combine=pair_concat,
        empty=empty_tuple,
        tasks=tasks,
        services=tuple(specs),
    )


class NarrowError(RuntimeError):
    """An exception whose constructor takes more arguments than it passes to ``super().__init__``:
    stdlib pickling dumps it, and the load raises ``TypeError``."""

    def __init__(self, name: str, endpoint: str, worker: str, reason: str) -> None:
        super().__init__(f"{name}: {reason}")
        self.name, self.endpoint, self.worker, self.reason = name, endpoint, worker, reason


USER_FRAME = SourceFrame(filename="user_analysis.py", lineno=42, function="my_cut", source="pt > 30")


def task_stage_error() -> StageError:
    """A ``StageError`` raised by a task's ``ValueError``."""
    return StageError(
        op="mul",
        frames=(USER_FRAME,),
        input_forms=("float64[]",),
        partition="mem://m68a/0:0-1",
        cause_type="ValueError",
        cause_message="negative pt",
        opt_level=2,
    )


# ---- in-process servers with accept counters -----------------------------------------------------


class _CountingHTTP(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = sys.platform != "win32"
    owner: CountingHTTPServer

    def get_request(self) -> tuple[socket.socket, Any]:
        conn, addr = super().get_request()
        self.owner._accepted()
        return conn, addr


class _CountingHandler(BaseHTTPRequestHandler):
    server: _CountingHTTP

    def do_GET(self) -> None:
        owner = self.server.owner
        status, ctype, body = owner.answer(self.path)
        self.send_response(status)
        self.send_header("content-type", ctype)
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        return None


class CountingHTTPServer:
    """An HTTP server on ``127.0.0.1:0`` counting accepted connections and answered GETs.

    ``mode``: ``"pid"`` answers 200 ``text/plain`` with this process's pid; ``"grpcish"`` answers 200
    ``application/grpc`` to every path (the EAF gateway's shape, P8); ``ok_first=n`` answers the first
    ``n`` GETs as ``"pid"`` and every later one 503."""

    def __init__(self, mode: str = "pid", ok_first: int | None = None) -> None:
        self.mode = mode
        self.ok_first = ok_first
        self.accepts = 0
        self.gets = 0
        self._lock = threading.Lock()
        self._http = _CountingHTTP(("127.0.0.1", 0), _CountingHandler)
        self._http.owner = self
        self.port = int(self._http.server_address[1])
        self._thread = threading.Thread(
            target=self._http.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True
        )
        self._thread.start()

    def _accepted(self) -> None:
        with self._lock:
            self.accepts += 1

    def answer(self, path: str) -> tuple[int, str, bytes]:
        with self._lock:
            self.gets += 1
            n = self.gets
        if self.ok_first is not None and n > self.ok_first:
            return 503, "text/plain", b"refusing"
        if self.mode == "grpcish":
            return 200, "application/grpc", b""
        return 200, "text/plain", str(os.getpid()).encode()

    def endpoint(self, scheme: str = "http") -> str:
        return f"{scheme}://127.0.0.1:{self.port}"

    def close(self) -> None:
        self._http.shutdown()
        self._http.server_close()
        self._thread.join(5.0)

    def __enter__(self) -> CountingHTTPServer:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class CountingListener:
    """A bare TCP listener on ``127.0.0.1:0``: accepts, counts, closes. ``settled()`` is the count once
    every connection made before the call was accepted (a sentinel connection, accepted in order after
    them, is not counted)."""

    SENTINEL = b"m68a-sentinel"

    def __init__(self) -> None:
        self.accepts = 0
        self._sentinels = 0
        self._lock = threading.Lock()
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(16)
        self._sock.settimeout(0.1)
        self.port = int(self._sock.getsockname()[1])
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                conn, _ = self._sock.accept()
            except (TimeoutError, OSError):
                continue
            conn.settimeout(0.5)
            try:
                first = conn.recv(len(self.SENTINEL))
            except OSError:
                first = b""
            conn.close()
            with self._lock:
                if first == self.SENTINEL:
                    self._sentinels += 1
                else:
                    self.accepts += 1

    def settled(self, timeout_s: float = 10.0) -> int:
        with self._lock:
            target = self._sentinels + 1
        with socket.create_connection(("127.0.0.1", self.port), timeout=timeout_s) as s:
            s.sendall(self.SENTINEL)
            assert wait_for(lambda: self._sentinels >= target, timeout_s), "the listener stopped accepting"
        with self._lock:
            return self.accepts

    def endpoint(self, scheme: str = "tcp") -> str:
        return f"{scheme}://127.0.0.1:{self.port}"

    def close(self) -> None:
        self._stop.set()
        self._thread.join(5.0)
        self._sock.close()

    def __enter__(self) -> CountingListener:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


TLS_HANDSHAKE = 0x16  # the first byte of a TLS ClientHello record


class SniffProxy:
    """A TCP proxy on ``127.0.0.1:0`` in front of ``127.0.0.1:target``: counts accepted connections,
    records each connection's first bytes (a TLS ClientHello starts ``0x16``, h2 ``PRI``, HTTP/1 ``GET``)
    and forwards both ways."""

    def __init__(self, target: int) -> None:
        self.target = target
        self.accepts = 0
        self.first_bytes: list[bytes] = []
        self._lock = threading.Lock()
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(16)
        self._sock.settimeout(0.1)
        self.port = int(self._sock.getsockname()[1])
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                conn, _ = self._sock.accept()
            except (TimeoutError, OSError):
                continue
            with self._lock:
                self.accepts += 1
            threading.Thread(target=self._serve, args=(conn,), daemon=True).start()

    def _serve(self, conn: socket.socket) -> None:
        conn.settimeout(10.0)
        try:
            first = conn.recv(64)
        except OSError:
            conn.close()
            return
        with self._lock:
            self.first_bytes.append(first)
        try:
            upstream = socket.create_connection(("127.0.0.1", self.target), timeout=10.0)
        except OSError:
            conn.close()
            return
        upstream.sendall(first)
        threading.Thread(target=_pump, args=(upstream, conn), daemon=True).start()
        _pump(conn, upstream)

    def endpoint(self, scheme: str) -> str:
        return f"{scheme}://127.0.0.1:{self.port}"

    def tls_attempted(self) -> bool:
        with self._lock:
            return any(b[:1] == bytes([TLS_HANDSHAKE]) for b in self.first_bytes)

    def close(self) -> None:
        self._stop.set()
        self._thread.join(5.0)
        self._sock.close()

    def __enter__(self) -> SniffProxy:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def _pump(src: socket.socket, dst: socket.socket) -> None:
    try:
        while True:
            data = src.recv(65536)
            if not data:
                break
            dst.sendall(data)
    except OSError:
        pass
    finally:
        for s in (src, dst):
            with contextlib.suppress(OSError):
                s.shutdown(socket.SHUT_RDWR)
            s.close()


@contextlib.contextmanager
def grpc_health_server(statuses: Mapping[str, str]) -> Iterator[int]:
    """A gRPC server with the reference ``grpc_health.v1`` servicer (``grpcio-health-checking``) on
    ``127.0.0.1``; ``statuses`` maps a service name to ``"SERVING"``/``"NOT_SERVING"``. Yields the port."""
    grpc = require_module("grpc")
    health = require_module("grpc_health.v1.health")
    health_pb2 = require_module("grpc_health.v1.health_pb2")
    health_pb2_grpc = require_module("grpc_health.v1.health_pb2_grpc")
    futures = importlib.import_module("concurrent.futures")
    server = grpc.server(futures.ThreadPoolExecutor(4))
    servicer = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(servicer, server)
    for name, status in statuses.items():
        servicer.set(name, getattr(health_pb2.HealthCheckResponse, status))
    port = int(server.add_insecure_port("127.0.0.1:0"))
    server.start()
    try:
        yield port
    finally:
        server.stop(0).wait(5.0)


@contextlib.contextmanager
def grpc_bare_server() -> Iterator[int]:
    """A gRPC server with no service registered, so no health service either."""
    grpc = require_module("grpc")
    futures = importlib.import_module("concurrent.futures")
    server = grpc.server(futures.ThreadPoolExecutor(2))
    port = int(server.add_insecure_port("127.0.0.1:0"))
    server.start()
    try:
        yield port
    finally:
        server.stop(0).wait(5.0)


# ---- backends -------------------------------------------------------------------------------------


@dataclass
class BackendLog:
    """What a recording backend saw, in order. ``submits``: (key, hints, run_nonce of a plan task's
    RunContext or None); ``cancels``: keys; ``ran``: keys whose task body started; ``answers``: (key,
    value) of completed probe tasks."""

    submits: list[tuple[str, dict[str, Any], str | None]] = field(default_factory=list)
    cancels: list[str] = field(default_factory=list)
    ran: list[str] = field(default_factory=list)
    answers: list[tuple[str, Any]] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def keys(self) -> list[str]:
        with self.lock:
            return [k for k, _h, _n in self.submits]

    def probe_keys(self) -> list[str]:
        return [k for k in self.keys() if is_probe_key(k)]

    def plan_keys(self) -> list[str]:
        return [k for k in self.keys() if is_plan_key(k)]

    def plan_nonces(self) -> list[str | None]:
        with self.lock:
            return [n for k, _h, n in self.submits if is_plan_key(k)]

    def ran_keys(self) -> list[str]:
        with self.lock:
            return list(self.ran)

    def cancelled(self) -> list[str]:
        with self.lock:
            return list(self.cancels)


def _run_nonce(args: tuple[object, ...]) -> str | None:
    """The ``run_nonce`` of the first ``RunContext``-shaped argument (a plan task's first argument)."""
    for arg in args:
        nonce = getattr(arg, "run_nonce", None)
        if isinstance(nonce, str):
            return nonce
    return None


class RecordingBackend:
    """A ``SubmitBackend`` over ``inner`` that records submits, cancels, task starts and probe answers
    into ``log`` and the shared trace. Attributes it does not define are ``inner``'s, so a duck-typed
    service attribute is visible exactly when ``inner`` has it.

    ``gate``: the inner backend's (single) worker is held by a blocker task until the event is set, so
    every task queues and none starts. ``fail_key``/``fault``: ``submit`` raises ``fault`` for the first
    key the predicate accepts."""

    def __init__(
        self,
        inner: Any,
        *,
        gate: threading.Event | None = None,
        fail_key: Callable[[str], bool] | None = None,
        fault: BaseException | None = None,
    ) -> None:
        self.inner = inner
        self.capabilities = inner.capabilities
        self.log = BackendLog()
        self._key_of: dict[int, str] = {}
        self._fail_key = fail_key
        self._fault = fault
        self.blocker: Any = None
        if gate is not None:
            self.blocker = inner.submit(gate.wait, GATE_TIMEOUT_S, key="m68a-gate")

    def __getattr__(self, name: str) -> Any:
        if name == "inner":
            raise AttributeError(name)
        return getattr(self.inner, name)

    def n_workers(self) -> int:
        return int(self.inner.n_workers())

    def submit(
        self,
        fn: Callable[..., object],
        /,
        *args: object,
        key: str,
        retries: int = 0,
        priority: int = 0,
        resources: Mapping[str, float] | None = None,
        workers: Sequence[str] | None = None,
    ) -> Any:
        hints = {"retries": retries, "priority": priority, "resources": resources, "workers": workers}
        with self.log.lock:
            self.log.submits.append((key, hints, _run_nonce(args)))
        trace("submit", key)
        if self._fail_key is not None and self._fault is not None and self._fail_key(key):
            self._fail_key = None
            raise self._fault
        fut = self.inner.submit(self._started, key, fn, *args, key=key, retries=retries, priority=priority)
        self._key_of[id(fut)] = key
        if is_probe_key(key):
            fut.add_done_callback(lambda f: self._answered(key, f))
        return fut

    def _started(self, key: str, fn: Callable[..., object], *args: object) -> object:
        with self.log.lock:
            self.log.ran.append(key)
        trace("ran", key)
        return fn(*args)

    def _answered(self, key: str, fut: Any) -> None:
        if fut.cancelled() or fut.exception() is not None:
            return
        with self.log.lock:
            self.log.answers.append((key, fut.result()))

    def broadcast(self, payload: bytes, *, token: str) -> object:
        return self.inner.broadcast(payload, token=token)

    def subscribe_events(
        self, topic: str, handler: Callable[[list[dict[str, object]]], None]
    ) -> Callable[[], None]:
        unsub: Callable[[], None] = self.inner.subscribe_events(topic, handler)
        return unsub

    def cancel(self, futures: Sequence[Any]) -> None:
        keys = [self._key_of.get(id(f), "?") for f in futures]
        with self.log.lock:
            self.log.cancels.extend(keys)
        trace("cancel", *keys)
        self.inner.cancel(futures)

    def drain(self, timeout_s: float = 60.0) -> None:
        """Wait until every task queued before this call has left the inner worker's queue."""
        self.inner.submit(time.sleep, 0, key="m68a-drain").result(timeout_s)

    def close(self) -> None:
        self.inner.close()


_ENV_LOCK = threading.Lock()


def _as_host(ad: str, fn: Callable[..., object], *args: object) -> object:
    """Run ``fn`` as a worker on the host whose machine ad is ``ad``: ``host_identity()`` reads it."""
    with _ENV_LOCK:
        before = os.environ.get("_CONDOR_MACHINE_AD")
        os.environ["_CONDOR_MACHINE_AD"] = ad
        try:
            return fn(*args)
        finally:
            if before is None:
                os.environ.pop("_CONDOR_MACHINE_AD", None)
            else:
                os.environ["_CONDOR_MACHINE_AD"] = before


def write_machine_ad(path: Path, host: str = "127.0.0.1") -> Path:
    """A machine ad file in the classad text form of ``$_CONDOR_MACHINE_AD``."""
    path.write_text(f'Machine = "{host}"\nName = "slot1@{host}"\nCpus = 2\n')
    return path


@dataclass
class HostFaults:
    """Faults a ``HostFake`` injects: ``host_raises`` from ``host_service``; ``release_raises`` from
    ``release_service``; ``probe_raises`` from a probe ``submit``; ``probe_silent``: a probe submit
    returns a future no worker ever answers; ``dead_hosted``: ``host_service`` returns an endpoint
    that accepts TCP and answers every HTTP GET with 503."""

    host_raises: BaseException | None = None
    release_raises: BaseException | None = None
    probe_raises: BaseException | None = None
    probe_silent: bool = False
    dead_hosted: bool = False


class HostFake:
    """A two-host ``SubmitBackend``: the driver is ``driver_host``; ``host_service`` starts an HTTP
    server here but reports it on ``service_host``; probe task ``i`` runs as a worker on
    ``worker_hosts[i % len]``. Tasks run on ``inner`` (a ``RecordingBackend``). ``host_service`` and
    ``release_service`` exist as instance attributes only when ``hosting``."""

    def __init__(
        self,
        ad_dir: Path,
        *,
        inner: RecordingBackend | None = None,
        worker_hosts: Sequence[str] = (THIRD_HOST,),
        n_workers: int = 1,
        site_services: Mapping[str, str] | None = None,
        service_hosts: tuple[str, ...] = ("driver", "cluster"),
        hosting: bool = True,
        faults: HostFaults | None = None,
    ) -> None:
        submit = submit_api()
        self.inner = inner if inner is not None else RecordingBackend(submit.ThreadBackend(1))
        self.capabilities = self.inner.capabilities
        self.driver_host = DRIVER_HOST
        self.service_host = SERVICE_HOST
        self.worker_hosts = tuple(worker_hosts)
        self._n_workers = n_workers
        self.advertise_host = "127.0.0.1"
        self.site_services = dict(site_services or {})
        self.service_hosts = service_hosts
        self.faults = faults or HostFaults()
        self.lock = threading.Lock()
        self.calls: list[tuple[Any, ...]] = []  # ("host_service", name, scope) / ("release_service", key)
        self.minted: list[tuple[str, str]] = []  # (scope, key) of every host_service that returned
        self.released: list[str] = []
        self.probe_hints: list[dict[str, Any]] = []
        self.silent: list[Future[Any]] = []
        self._servers: dict[str, CountingHTTPServer] = {}
        self._probes = 0
        ad_dir.mkdir(parents=True, exist_ok=True)
        hosts = {DRIVER_HOST, SERVICE_HOST, THIRD_HOST, *self.worker_hosts}
        self.ads = {h: str(write_machine_ad(ad_dir / f"{h}.ad", h)) for h in hosts}
        if hosting:
            self.host_service = self._host_service
            self.release_service = self._release_service

    # the duck-typed service surface
    def host_identity(self) -> str:
        return self.driver_host

    def _host_service(self, spec: ServiceSpec, scope: str) -> tuple[str, str, str]:
        with self.lock:
            self.calls.append(("host_service", spec.name, scope))
        trace("host_service", spec.name, scope)
        if self.faults.host_raises is not None:
            raise self.faults.host_raises
        key = f"{scope}-{secrets.token_hex(8)}"
        scheme = {"http": "http", "grpc": "grpc"}.get(spec.check.partition(":")[0], "tcp")
        # dead_hosted: a server that accepts TCP but answers every GET 503, so only a probe that runs
        # the spec's own check (not a connect) refuses it
        server = CountingHTTPServer("pid", ok_first=0 if self.faults.dead_hosted else None)
        self._servers[key] = server
        port = server.port
        with self.lock:
            self.minted.append((scope, key))
        return f"{scheme}://127.0.0.1:{port}", self.service_host, key

    def _release_service(self, key: str) -> None:
        with self.lock:
            self.calls.append(("release_service", key))
            self.released.append(key)
        trace("release_service", key)
        server = self._servers.pop(key, None)
        if server is not None:
            server.close()
        if self.faults.release_raises is not None:
            raise self.faults.release_raises

    def minted_under(self, scope: str) -> list[str]:
        with self.lock:
            return [k for s, k in self.minted if s == scope]

    def released_keys(self) -> list[str]:
        with self.lock:
            return list(self.released)

    def host_service_calls(self) -> list[tuple[Any, ...]]:
        with self.lock:
            return [c for c in self.calls if c[0] == "host_service"]

    # the SubmitBackend protocol
    def n_workers(self) -> int:
        return self._n_workers

    def submit(
        self,
        fn: Callable[..., object],
        /,
        *args: object,
        key: str,
        retries: int = 0,
        priority: int = 0,
        resources: Mapping[str, float] | None = None,
        workers: Sequence[str] | None = None,
    ) -> Any:
        if not is_probe_key(key):
            return self.inner.submit(
                _as_host, self.ads[self.worker_hosts[0]], fn, *args, key=key, retries=retries
            )
        with self.lock:
            self.probe_hints.append({"resources": resources, "workers": workers})
            host = self.worker_hosts[self._probes % len(self.worker_hosts)]
            self._probes += 1
        if self.faults.probe_raises is not None:
            with self.inner.log.lock:
                self.inner.log.submits.append((key, {}, None))
            trace("submit", key)
            raise self.faults.probe_raises
        if self.faults.probe_silent:
            fut: Future[Any] = Future()
            with self.inner.log.lock:
                self.inner.log.submits.append((key, {}, None))
            self.inner._key_of[id(fut)] = key
            trace("submit", key)
            self.silent.append(fut)
            return fut
        return self.inner.submit(_as_host, self.ads[host], fn, *args, key=key, retries=retries)

    def broadcast(self, payload: bytes, *, token: str) -> object:
        return payload

    def subscribe_events(
        self, topic: str, handler: Callable[[list[dict[str, object]]], None]
    ) -> Callable[[], None]:
        return self.inner.subscribe_events(topic, handler)

    def cancel(self, futures: Sequence[Any]) -> None:
        silent = [f for f in futures if any(f is s for s in self.silent)]
        for fut in silent:
            fut.cancel()
        with self.inner.log.lock:
            self.inner.log.cancels.extend(self.inner._key_of.get(id(f), "?") for f in silent)
        self.inner.cancel([f for f in futures if not any(f is s for s in self.silent)])

    def close(self) -> None:
        for server in list(self._servers.values()):
            server.close()
        self._servers.clear()
        self.inner.close()


# ---- the services log ------------------------------------------------------------------------------


def status_records(records: Sequence[logging.LogRecord]) -> list[Any]:
    """The ``ServiceStatus`` of every record the ``graphed_executors.services`` logger emitted."""
    return [r.status for r in records if r.name == SERVICES_LOGGER and hasattr(r, "status")]


def statuses_named(records: Sequence[logging.LogRecord], name: str) -> list[Any]:
    return [s for s in status_records(records) if s.name == name]


# ---- recorded bindings (copied from m67) -----------------------------------------------------------


class SubmitResult:
    def __init__(self, cluster: int) -> None:
        self._cluster = cluster

    def cluster(self) -> int:
        return self._cluster


class RecordingSchedd:
    """A stand-in ``htcondor2.Schedd`` that appends every call to ``log``; ``query`` answers from
    ``queue`` (the last answer repeats), ``history`` from ``history``; ``spool_raises`` makes ``spool``
    raise it."""

    def __init__(
        self,
        queue: list[list[dict[str, Any]]] | None = None,
        history: list[dict[str, Any]] | None = None,
        spool_raises: BaseException | None = None,
    ) -> None:
        self.log: list[tuple[Any, ...]] = []
        self.queue = [list(answer) for answer in (queue or [[]])]
        self.history_ads = list(history or [])
        self.spool_raises = spool_raises

    def query(self, constraint: str = "true", projection: Any = None, *args: Any, **kwargs: Any) -> list[Any]:
        self.log.append(("query", str(constraint), tuple(projection or ())))
        return self.queue.pop(0) if len(self.queue) > 1 else list(self.queue[0])

    def history(self, constraint: Any = None, projection: Any = None, *args: Any, **kwargs: Any) -> list[Any]:
        self.log.append(("history", str(constraint), tuple(projection or ())))
        return list(self.history_ads)

    def submit(self, description: Any, count: int = 0, spool: bool = False, **kwargs: Any) -> SubmitResult:
        self.log.append(("submit", dict(description), count, spool))
        return SubmitResult(FAKE_CLUSTER)

    def spool(self, result: Any, *args: Any, **kwargs: Any) -> None:
        self.log.append(("spool",))
        if self.spool_raises is not None:
            raise self.spool_raises

    def retrieve(self, constraint: Any = None, *args: Any, **kwargs: Any) -> None:
        self.log.append(("retrieve", str(constraint)))

    def act(self, action: Any, constraint: Any = None, *args: Any, **kwargs: Any) -> None:
        self.log.append(("act", str(action), str(constraint)))


class _Collector:
    def __init__(self, fake: FakeHTCondor, pool: str | None) -> None:
        self.fake = fake
        self.pool = pool

    def locate(self, daemon_type: Any, name: str | None = None, *args: Any, **kwargs: Any) -> dict[str, Any]:
        self.fake.log.append(("locate", self.pool, str(daemon_type), name))
        return {"Name": name or FAKE_SCHEDD, "MyAddress": "<127.0.0.1:9618>", "located": True}

    def query(
        self, ad_type: Any = None, constraint: Any = None, projection: Any = None, **kwargs: Any
    ) -> Any:
        self.fake.log.append(("collector-query", self.pool))
        return []


class FakeHTCondor:
    """What ``launch._htcondor()`` returns under the recorder: ``param``, ``Collector``, ``Schedd``,
    ``Submit`` and the enums the backend names."""

    class DaemonType:
        Schedd = "Schedd"

    class AdType:
        Schedd = "Schedd"

    class JobAction:
        Remove = "Remove"

    def __init__(self, schedd: RecordingSchedd, full_hostname: str = "login.m68a.example") -> None:
        self.schedd = schedd
        self.log = schedd.log
        self.param = {
            "COLLECTOR_HOST": FAKE_POOL,
            "SCHEDD_HOST": FAKE_SCHEDD,
            "FULL_HOSTNAME": full_hostname,
        }

    def Collector(self, pool: str | None = None, *args: Any, **kwargs: Any) -> _Collector:
        self.log.append(("Collector", pool))
        return _Collector(self, pool)

    def Schedd(self, location: Any = None, *args: Any, **kwargs: Any) -> RecordingSchedd:
        self.log.append(("Schedd", None if location is None else location["Name"]))
        return self.schedd

    def Submit(self, description: Any = None, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return dict(description or {})


def record_bindings(monkeypatch: pytest.MonkeyPatch, schedd: RecordingSchedd, **kwargs: Any) -> FakeHTCondor:
    """Patch ``launch._htcondor`` to log ``("_htcondor",)`` and return a ``FakeHTCondor`` over ``schedd``."""
    fake = FakeHTCondor(schedd, **kwargs)

    def _htcondor() -> FakeHTCondor:
        fake.log.append(("_htcondor",))
        return fake

    monkeypatch.setattr(launch_api(), "_htcondor", _htcondor)
    return fake


def submits(log: list[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
    return [entry for entry in log if entry[0] == "submit"]


# ---- the job side (copied from m67) ----------------------------------------------------------------


def input_entries(desc: dict[str, Any]) -> list[str]:
    return [e.strip() for e in str(desc["transfer_input_files"]).split(",") if e.strip()]


def job_dir(desc: dict[str, Any], log_dir: Path, dest: Path) -> Path:
    """A job scratch dir holding what the schedd transfers: each ``transfer_input_files`` entry, resolved
    against ``initialdir`` (else ``log_dir``), under its base name."""
    iwd = Path(desc.get("initialdir") or log_dir)
    dest.mkdir(parents=True)
    for entry in input_entries(desc):
        src = iwd / entry
        target = dest / src.name
        if src.is_dir():
            shutil.copytree(src, target)
        else:
            shutil.copy2(src, target)
    return dest


class NoStartLauncher:
    """A ``PilotLauncher`` that starts nothing; ``profile`` is what an attached backend reads its site
    data from. ``stop_raises`` makes ``stop`` raise it."""

    def __init__(self, profile: Any = None, stop_raises: BaseException | None = None) -> None:
        if profile is not None:
            self.profile = profile
        self.stop_raises = stop_raises
        self.stops = 0
        self.log_dir = None

    def start(self, url: str, secret: bytes, n: int) -> None:
        self.url = url

    def alive(self) -> int:
        return 0

    def stop(self) -> None:
        self.stops += 1
        if self.stop_raises is not None:
            raise self.stop_raises


def with_ports(spec: ServiceSpec, ports: tuple[int, int], timeout_s: float = 30.0) -> ServiceSpec:
    """A recipe-built spec on a free port range with a test-sized ``timeout_s`` (its kind, check and
    launch unchanged)."""
    return dataclasses.replace(spec, ports=ports, timeout_s=timeout_s)


# ---- fresh-interpreter legs ------------------------------------------------------------------------


def narrow_blob_main(out: str) -> None:
    """``driver._result_blob`` of a ``NarrowError`` in an interpreter where nothing replaced stdlib
    exception pickling (``distributed`` installs a process-wide one that skips ``__init__``)."""
    narrow = NarrowError("t", "grpc://h:1", "w", "refused")
    try:
        pickle.loads(pickle.dumps((False, narrow)))
        control = "loaded"
    except TypeError:
        control = "TypeError"
    ok, exc = pickle.loads(driver_api()._result_blob(False, narrow))
    Path(out).write_text(
        json.dumps({"control": control, "ok": ok, "type": type(exc).__name__, "text": str(exc)})
    )


# ---- the whole path in a fresh interpreter ----------------------------------------------------------


def whole_path_main(out: str, root: str) -> None:
    """``recipes.http_server`` through ``SubmitRunner(ThreadBackend)`` in a fresh interpreter: writes
    what the parent asserts on (the resolved value, the order of bind/probe/plan submits, the endpoint
    and whether its port is free after ``run``, and every service-client module imported)."""
    recipes, submit = recipes_api(), submit_api()
    spec = with_ports(recipes.http_server("web", root=root), free_range(3))
    assert (spec.kind, spec.check) == ("http", "http:/"), spec
    backend = RecordingBackend(submit.ThreadBackend(2))
    plan = spy_plan(SpyProcess("whole", service="web", path="/"), 2, [spec])
    with popen_backstop(), submit.SubmitRunner(backend) as runner:
        result = runner.run(plan)
        (bind,) = spy_events("whole", "bind")
        endpoint = bind[1]["web"]
        free = port_free(endpoint_port(endpoint))
    order = [(e[0], e[1]) for e in trace_snapshot() if e[0] in ("submit", "bind")]
    modules = sorted(m for m in sys.modules if "triton" in m.lower() or "histserv" in m.lower())
    resolves = spy_events("whole", "resolve")
    Path(out).write_text(
        json.dumps(
            {
                "endpoint": endpoint,
                "port_free_after_run": free,
                "order": order,
                "modules": modules,
                "value_is_resolved": isinstance(result.value, Resolved) and result.value.tag == "whole",
                "resolves": [(r[2], len(r[1])) for r in resolves],
                "leaf_bodies": list(result.value.value) if isinstance(result.value, Resolved) else None,
            }
        )
    )
