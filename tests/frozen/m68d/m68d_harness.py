"""Harness for the m68d frozen suite (plan §4): a lost service ends the run as a service failure (C), a
SERVICE node restarts its service and the driver reruns (B), the driver fails fast on a node that can
never announce (A), and a port counts only when the child holds its listener.

The accessors look the implementation up inside test bodies, so the suite collects before it exists.
Frozen suites never import across directories, so what this suite shares with m68b (bounds, signing,
the bindings recorder's shape) is written here again. This file is shipped to pilots and driver jobs as
a user module: at module level it imports only the stdlib, graphed and graphed_executors.
"""

from __future__ import annotations

import builtins
import hashlib
import hmac
import importlib
import json
import logging
import os
import pickle
import re
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import types
import urllib.error
import urllib.request
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager, suppress
from dataclasses import dataclass, replace
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import TYPE_CHECKING, Any, TextIO

from graphed.core.execution import Partition, Plan, Task
from graphed.core.plan import DurablePlanV2, OpSpec, StageSpec
from graphed.services import Launch, ServiceSpec, UnboundService

from graphed_executors.local._transport import LookupFreeHTTPServer

if TYPE_CHECKING:
    import pytest

HARNESS_DIR = str(Path(__file__).resolve().parent)
HARNESS_FILE = str(Path(__file__).resolve())
CHILD_FILE = str(Path(HARNESS_DIR, "m68d_child.py"))

RUN_TIMEOUT_S = 240.0
GATE_S = 120.0
SERVICE = "web"
NODE = "svc0"
SERVICES_LOGGER = "graphed_executors.services"
SIG_HEADER = "X-Graphed-Sig"
DRIVER_MODULE = "graphed_executors.htcondor_backend.driver"
ANNOUNCE_MODULE = "graphed_executors.htcondor_backend.announce"

# digitless, so no asserted text can come from a tmp path, a test name or a random suffix
WORKER_ID = "m-worker-node.example"
DRIVER_ID = "m-driver-node.example"
SERVICE_ID = "m-service-node.example"
LEAF_ERROR = "m-leaf-one-failed"
PLAN_ERROR = "m-plan-error-beside-a-live-service"
HOLD_TEXT = "m-held-by-the-owner"
LOCATE_TEXT = "m-the-collector-refused-the-locate"
QUERY_TEXT = "m-the-schedd-refused-the-query"

FAKE_POOL = "cm.m-dee.example:9618"
FAKE_SCHEDD = "schedd-chosen.m-dee.example"
DAG_ID = 4517  # the DAGMan job of the recorded ads
DRIVER_CLUSTER = 4519
NODE_CLUSTER = 4520
SLOT = "slot1@m-node.example"


# ---- deferred accessors for the implementation under test ---------------------------------------


def engine_api() -> Any:
    return importlib.import_module("graphed_executors.submit.engine")


def services_api() -> Any:
    """``graphed_executors.submit.services``: ServiceSet, ServiceUnreachable, listeners, held_by."""
    return importlib.import_module("graphed_executors.submit.services")


def announce_api() -> Any:
    """``graphed_executors.htcondor_backend.announce``: start, RESTARTS, listeners, held_by."""
    return importlib.import_module(ANNOUNCE_MODULE)


def backend_api() -> Any:
    return importlib.import_module("graphed_executors.htcondor_backend.backend")


def driver_api() -> Any:
    """``graphed_executors.htcondor_backend.driver``: ``main(argv)`` and ``_runner(run, job, log)``."""
    return importlib.import_module(DRIVER_MODULE)


def launch_api() -> Any:
    return importlib.import_module("graphed_executors.htcondor_backend.launch")


def server_api() -> Any:
    return importlib.import_module("graphed_executors.htcondor_backend.server")


def htcondor_api() -> Any:
    """``graphed_executors.htcondor_backend``: SITES, submit_driverless, RunHandle."""
    return importlib.import_module("graphed_executors.htcondor_backend")


# ---- bounds ----------------------------------------------------------------------------------------


class Background:
    """``fn()`` started on a daemon thread; :meth:`result` joins it within a bound (a hang fails)."""

    def __init__(self, fn: Callable[[], Any]) -> None:
        self._out: dict[str, Any] = {}
        self._thread = threading.Thread(target=self._drive, args=(fn,), daemon=True)
        self._thread.start()

    def _drive(self, fn: Callable[[], Any]) -> None:
        try:
            self._out["result"] = fn()
        except BaseException as exc:
            self._out["error"] = exc

    def done(self) -> bool:
        return not self._thread.is_alive()

    def result(self, timeout_s: float = RUN_TIMEOUT_S) -> Any:
        self._thread.join(timeout_s)
        assert not self._thread.is_alive(), f"HARD TIMEOUT: call did not finish within {timeout_s}s"
        if "error" in self._out:
            raise self._out["error"]
        return self._out["result"]

    def error(self, timeout_s: float = RUN_TIMEOUT_S) -> BaseException:
        """The exception the call raised; a failure when it returned or did not finish in time."""
        try:
            value = self.result(timeout_s)
        except AssertionError as exc:
            if str(exc).startswith("HARD TIMEOUT"):
                raise
            return exc
        except BaseException as exc:
            return exc
        raise AssertionError(f"the call returned {value!r} instead of raising")


def run_bounded(fn: Callable[[], Any], timeout_s: float = RUN_TIMEOUT_S) -> Any:
    return Background(fn).result(timeout_s)


def raised_by(fn: Callable[[], Any], timeout_s: float = RUN_TIMEOUT_S) -> BaseException:
    return Background(fn).error(timeout_s)


def wait_for(predicate: Callable[[], bool], timeout_s: float = 30.0, poll_s: float = 0.02) -> bool:
    deadline = time.monotonic() + timeout_s
    while not predicate():
        if time.monotonic() >= deadline:
            return predicate()
        time.sleep(poll_s)
    return True


def cut(text: str, *paths: Path) -> str:
    """``text`` without any path token under ``paths`` (as given and resolved)."""
    for path in paths:
        for root in {str(path), str(Path(path).resolve())}:
            text = re.sub(re.escape(root) + r"\S*", "<cut>", text)
    return text


def pid_gone(pid: int) -> bool:
    if sys.platform == "win32":
        return True  # only the posix legs ask
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    return False


# ---- ports and servers -------------------------------------------------------------------------------


def _bindable(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("", port))
        except OSError:
            return False
    return True


def free_range(n: int = 3) -> tuple[int, int]:
    """An inclusive range of ``n`` consecutive free ports, outside the 10000-10100 band of the site rows."""
    for _ in range(400):
        low = 20000 + secrets.randbelow(30000)
        if all(_bindable(p) for p in range(low, low + n)):
            return (low, low + n - 1)
    raise AssertionError(f"no {n} consecutive free ports found")


class _Answer(BaseHTTPRequestHandler):
    server: Web

    def do_GET(self) -> None:
        self.server.count()
        body = b"m-web"
        self.send_response(self.server.status)
        self.send_header("content-type", "text/plain")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        return None


class Web(LookupFreeHTTPServer):
    """An HTTP server answering every GET with ``status``, counting them; ``host=""`` binds every interface."""

    daemon_threads = True

    def __init__(self, port: int = 0, *, host: str = "127.0.0.1", status: int = 200) -> None:
        super().__init__((host, port), _Answer)
        self.status = status
        self._lock = threading.Lock()
        self._gets = 0
        self._thread = threading.Thread(target=self.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
        self._thread.start()

    def count(self) -> None:
        with self._lock:
            self._gets += 1

    def gets(self) -> int:
        with self._lock:
            return self._gets

    @property
    def port(self) -> int:
        return int(self.server_address[1])

    @property
    def endpoint(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def close(self) -> None:
        self.shutdown()
        self.server_close()
        self._thread.join(5.0)

    def __enter__(self) -> Web:
        return self

    def __exit__(self, *exc: object) -> None:
        with suppress(Exception):
            self.close()


def http_status(endpoint: str, timeout_s: float = 10.0) -> int:
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(endpoint + "/", timeout=timeout_s) as resp:
        return int(resp.status)


# ---- the plan parts (pilots and driver jobs import them from this file by name) -------------------------


def mark_of(partition: Partition) -> Path:
    """The marker directory a task's partition names: ``mem://<dir>/<i>``."""
    return Path(partition.uri.removeprefix("mem://").rsplit("/", 1)[0])


def partitions(mark: Path, n: int) -> tuple[Partition, ...]:
    return tuple(Partition(f"mem://{mark}/{i}", "", i, i + 1) for i in range(n))


def touch(path: Path) -> None:
    path.write_text(str(os.getpid()))


@dataclass(frozen=True)
class GatedGet:
    """Task ``i`` marks ``t<i>.start``; from task 1 on it waits for ``killed``; then it GETs the bound
    service, marking ``t<i>.done``, or ``t<i>.error`` holding the exception's repr before re-raising."""

    endpoint: str | None = None

    def bind_services(self, endpoints: Mapping[str, str]) -> GatedGet:
        if SERVICE not in endpoints:
            raise UnboundService(SERVICE)
        return replace(self, endpoint=endpoints[SERVICE])

    def __call__(self, partition: Partition, resources: object) -> tuple[tuple[int, int], ...]:
        assert self.endpoint is not None, "process called unbound"
        mark, i = mark_of(partition), partition.entry_start
        touch(mark / f"t{i}.start")
        if i >= 1:
            wait_for((mark / "killed").exists, GATE_S, 0.05)
        try:
            status = http_status(self.endpoint)
        except Exception as exc:
            (mark / f"t{i}.error").write_text(repr(exc))
            raise
        touch(mark / f"t{i}.done")
        return ((i, status),)


@dataclass(frozen=True)
class SlowToLoad(GatedGet):
    """:class:`GatedGet` whose first unpickling in a driver job sleeps ``delay_s``, so the driver binds its
    task server that much later."""

    delay_s: float = 0.0

    def __setstate__(self, state: dict[str, Any]) -> None:
        if sys.argv[0].endswith("driver.py") and not _LOADED:
            _LOADED.append(True)
            time.sleep(state.get("delay_s", 0.0))
        self.__dict__.update(state)


_LOADED: list[bool] = []


@dataclass(frozen=True)
class RaiseIn:
    """Raises the builtin ``error`` named (``PLAN_ERROR`` its message), or returns ``((i, "ok"),)``."""

    error: str | None
    endpoint: str | None = None

    def bind_services(self, endpoints: Mapping[str, str]) -> RaiseIn:
        return replace(self, endpoint=endpoints[SERVICE])

    def __call__(self, partition: Partition, resources: object) -> tuple[tuple[int, str], ...]:
        if self.error is not None:
            raise getattr(builtins, self.error)(PLAN_ERROR)
        return ((partition.entry_start, "ok"),)


@dataclass(frozen=True)
class StageGet:
    """A ``DurablePlanV2`` stage process running :class:`GatedGet` on its task's partition."""

    endpoint: str | None = None

    def bind_services(self, endpoints: Mapping[str, str]) -> StageGet:
        return replace(self, endpoint=endpoints[SERVICE])

    def __call__(self, task: Task, inputs: Sequence[bytes], resources: object) -> bytes:
        return pickle.dumps(GatedGet(self.endpoint)(task.partition, resources))


def stop_leaf(partition: Partition, resources: object) -> tuple[int, ...]:
    """Marks ``start-<i>``; leaf 1 raises ``ValueError(LEAF_ERROR)``, every other sleeps 0.3 s."""
    i = partition.entry_start
    touch(mark_of(partition) / f"start-{i}")
    if i == 1:
        raise ValueError(LEAF_ERROR)
    time.sleep(0.3)
    return (i,)


def pair_concat(a: tuple[Any, ...], b: tuple[Any, ...]) -> tuple[Any, ...]:
    return a + b


def empty_tuple() -> tuple[Any, ...]:
    return ()


def given_spec(timeout_s: float = 30.0) -> ServiceSpec:
    """A requirement without a recipe: only a given endpoint serves it."""
    return ServiceSpec(SERVICE, "http", check="http:/", ports=(40000, 40010), launch=None, timeout_s=timeout_s)


def node_spec(
    timeout_s: float = 30.0,
    argv: Sequence[str] = ("{python}", "-m", "http.server", "{port}"),
    *,
    memory_mb: int | None = None,
    inputs: Sequence[str] = (),
) -> ServiceSpec:
    """Image-less with one GPU, so only a DAG SERVICE node hosts it."""
    resources: dict[str, Any] = {"gpus": 1}
    if memory_mb is not None:
        resources["memory_mb"] = memory_mb
    launch = Launch(argv=tuple(argv), resources=resources, inputs=tuple(inputs))
    return ServiceSpec(SERVICE, "http", check="http:/", ports=(40000, 40010), launch=launch, timeout_s=timeout_s)


def gated_plan(mark: Path, spec: ServiceSpec | None, n: int = 3, process: Any = None) -> Plan[Any]:
    mark.mkdir(parents=True, exist_ok=True)
    tasks = tuple(Task(i, p) for i, p in enumerate(partitions(mark, n)))
    return Plan(
        process=GatedGet() if process is None else process,
        combine=pair_concat,
        empty=empty_tuple,
        tasks=tasks,
        services=() if spec is None else (spec,),
    )


def stage_plan(mark: Path, spec: ServiceSpec, n: int = 3) -> DurablePlanV2:
    mark.mkdir(parents=True, exist_ok=True)
    tasks = tuple(Task(i, p) for i, p in enumerate(partitions(mark, n)))
    stage = StageSpec(kind="gather", process=OpSpec.from_callable(StageGet()), tasks=tasks)
    return DurablePlanV2(ir=b"m-dee", stages=(stage,), services=(spec,))


class EventLog:
    """A passive monitor that records every event."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.events: list[Any] = []

    def on_task(self, event: Any) -> None:
        with self._lock:
            self.events.append(event)

    def on_profile(self, worker: str, payload: bytes) -> None:
        return None

    def on_combine(self, leaves_done: int) -> None:
        return None

    def worker_profiler_factory(self) -> None:
        return None

    def phases(self, phase: str) -> list[Any]:
        with self._lock:
            return [e for e in self.events if str(e.phase) == phase]


# ---- the managed child ------------------------------------------------------------------------------


def child_starts(report: Path) -> list[tuple[int, int]]:
    """``(pid, port)`` of every start of the child writing ``report``."""
    path = Path(f"{report}.starts")
    if not path.is_file():
        return []
    rows = [line.split() for line in path.read_text().splitlines()]
    return [(int(r[1]), int(r[2])) for r in rows if len(r) == 3 and r[0] == "start"]


def end_child(report: Path, pid: int, code: int) -> None:
    """Make the child ``pid`` writing ``report`` exit with ``code``."""
    path, tmp = Path(f"{report}.exit.{pid}"), Path(f"{report}.exit.{pid}.tmp")
    tmp.write_text(str(code))
    os.replace(tmp, path)


def write_machine_ad(path: Path, machine: str) -> Path:
    path.write_text(f'Machine = "{machine}"\nName = "slot1@{machine}"\nCpus = 2\n')
    return path


def write_job_ad(path: Path) -> Path:
    """A DAG driver node's ``$_CONDOR_JOB_AD``: its own cluster and its DAGMan job."""
    path.write_text(
        f'ClusterId = {DRIVER_CLUSTER}\nProcId = 0\nDAGManJobId = {DAG_ID}\nDAGNodeName = "driver"\nJobStatus = 2\n'
    )
    return path


# ---- signing and announces -----------------------------------------------------------------------------


def sign(secret: bytes, body: bytes) -> str:
    return hmac.new(secret, body, hashlib.sha256).hexdigest()


def post(url: str, body: bytes, secret: bytes, timeout: float = 30.0) -> int:
    request = urllib.request.Request(url, data=body, method="POST")
    request.add_header(SIG_HEADER, sign(secret, body))
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return int(response.status)
    except urllib.error.HTTPError as exc:
        return int(exc.code)


def dag_pair(dag_dir: Path, timeout_s: float = 60.0) -> tuple[str, bytes]:
    """The driver's ``(url, announce secret)`` as it published them in ``dag_dir``."""
    url_file, secret_file = dag_dir / "driver.url", dag_dir / "graphed-secret"
    assert wait_for(lambda: url_file.is_file() and secret_file.is_file(), timeout_s), "the driver published nothing"
    return url_file.read_text().strip().rstrip("/"), bytes.fromhex(secret_file.read_text().strip())


def announce_node(dag_dir: Path, port: int, identity: str = SERVICE_ID) -> int:
    url, secret = dag_pair(dag_dir)
    return post(url + "/announce", f"{NODE} 127.0.0.1:{port} {identity}".encode(), secret)


def announce_later(dag_dir: Path, port: int, delay_s: float) -> threading.Timer:
    timer = threading.Timer(delay_s, announce_node, args=(dag_dir, port))
    timer.daemon = True
    timer.start()
    return timer


class AnnounceRun:
    """``python -m …announce service.json`` with ``cwd=job``; output collected on a thread. Leaving the
    ``with`` SIGTERMs it (then kills it) and kills every child pid ``report`` lists."""

    def __init__(self, job: Path, env: Mapping[str, str], report: Path) -> None:
        self.report = report
        self.proc = subprocess.Popen(
            [sys.executable, "-m", ANNOUNCE_MODULE, "service.json"],
            cwd=job,
            env=dict(env),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        self._lines: list[str] = []
        self._lock = threading.Lock()
        self._reader = threading.Thread(target=self._read, daemon=True)
        self._reader.start()

    def _read(self) -> None:
        assert self.proc.stdout is not None
        for line in self.proc.stdout:
            with self._lock:
                self._lines.append(line)

    def output(self) -> str:
        with self._lock:
            return "".join(self._lines)

    def count(self, text: str) -> int:
        with self._lock:
            return sum(text in line for line in self._lines)

    def wait(self, timeout_s: float) -> int:
        try:
            code = self.proc.wait(timeout_s)
        except subprocess.TimeoutExpired:
            raise AssertionError(f"announce.py did not exit within {timeout_s}s; output:\n{self.output()}") from None
        self._reader.join(5.0)
        return code

    def __enter__(self) -> AnnounceRun:
        return self

    def __exit__(self, *exc: object) -> None:
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(15.0)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        for pid, _port in child_starts(self.report):
            if not pid_gone(pid):
                with suppress(OSError):
                    os.kill(pid, signal.SIGKILL)


def service_json(job: Path, **cfg: Any) -> Path:
    """``job/service.json`` (and ``job/service/``) as the node's ``announce.py`` reads them."""
    job.mkdir(parents=True, exist_ok=True)
    (job / "service").mkdir(exist_ok=True)
    full = {
        "env": {},
        "check": "http:/",
        "key": NODE,
        "url": None,
        "watch": None,
        "python": sys.executable,
        "timeout_s": 30.0,
        "lease_s": 30.0,
        "beat_s": 1.0,
        **cfg,
    }
    (job / "service.json").write_text(json.dumps(full))
    return job


class NoLauncher:
    """A launcher stand-in for a bare ``TaskServer``."""

    log_dir = None

    def alive(self) -> int:
        return 1


# ---- the driver job's directory ---------------------------------------------------------------------------


def run_json(site: str, *, dag_dir: Path | None = None, **over: Any) -> dict[str, Any]:
    """``run.json`` as the submitter writes it for a local-pilots driver job."""
    run: dict[str, Any] = {
        "pilots": "local",
        "n_pilots": 1,
        "site": site,
        "image": None,
        "log_dir": "",
        "request_memory_mb": 1024,
        "min_pilots": 1,
        "retries": 0,
        "max_in_flight": 1,
        "schedd_locate": None,
        "user_modules": [],
        "endpoints": {},
        "announce_only": {SERVICE: NODE} if dag_dir is not None else {},
        "dag_dir": None if dag_dir is None else str(dag_dir),
        "extra_submit": {},
    }
    run.update(over)
    return run


def write_driver_job(job: Path, plan: Any, run: Mapping[str, Any]) -> Path:
    """``job`` holding ``plan.pkl``, ``run.json`` and this harness (the plan's module)."""
    job.mkdir(parents=True, exist_ok=True)
    (job / "plan.pkl").write_bytes(pickle.dumps(plan))
    (job / "run.json").write_text(json.dumps(dict(run)))
    shutil.copy(HARNESS_FILE, job / Path(HARNESS_FILE).name)
    return job


def log_lines(job: Path, text: str) -> list[str]:
    path = job / "driver.log"
    return [line for line in path.read_text().splitlines() if text in line] if path.is_file() else []


@contextmanager
def driver_log(job: Path) -> Iterator[TextIO]:
    """``job/driver.log`` open for ``_runner``, with the package's records routed into it as
    ``driver.main`` routes them."""
    job.mkdir(parents=True, exist_ok=True)
    with open(job / "driver.log", "a") as log:
        handler = logging.StreamHandler(log)
        handler.setFormatter(logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s"))
        package = logging.getLogger("graphed_executors")
        level = package.level
        package.addHandler(handler)
        package.setLevel(logging.INFO)
        try:
            yield log
        finally:
            package.removeHandler(handler)
            handler.close()
            package.setLevel(level)


def site_copy(monkeypatch: pytest.MonkeyPatch, key: str, base: str, **changes: Any) -> Any:
    api = htcondor_api()
    profile = replace(api.SITES[base], name=key, **changes)
    monkeypatch.setitem(api.SITES, key, profile)
    return profile


# ---- recorded bindings: a schedd that evaluates simple constraints, a collector, ClassAds ------------------


class FakeAd(dict[str, Any]):
    """A ClassAd stand-in: ``eval`` reads an attribute, ``symmetricMatch`` fits a job's ``Request*`` into a
    slot's free ``Memory``/``Cpus``/``GPUs`` (both sides need ``Requirements``), ``str`` is ClassAd text."""

    def eval(self, attr: str) -> Any:
        return self.get(attr)

    def symmetricMatch(self, other: Mapping[str, Any]) -> bool:
        job, slot = (self, other) if "RequestMemory" in self else (other, self)
        if "Requirements" not in job or "Requirements" not in slot:
            return False
        return all(int(slot.get(r, 0)) >= int(job.get(f"Request{r}", 0)) for r in ("Memory", "Cpus", "GPUs"))

    def __str__(self) -> str:
        def lit(v: Any) -> str:
            if isinstance(v, bool):
                return "true" if v else "false"
            return f'"{v}"' if isinstance(v, str) else str(v)

        return "[ " + "; ".join(f"{k} = {lit(v)}" for k, v in self.items()) + " ]"


def _literal(text: str) -> Any:
    text = text.strip()
    if text.startswith('"') and text.endswith('"'):
        return text[1:-1]
    if text.lower() in ("true", "false"):
        return text.lower() == "true"
    try:
        return int(text)
    except ValueError:
        return text


def classad(arg: Any = None) -> FakeAd:
    """``classad2.ClassAd``: from nothing, a mapping, or the text :meth:`FakeAd.__str__` writes."""
    if arg is None:
        return FakeAd()
    if isinstance(arg, Mapping):
        return FakeAd(arg)
    body = str(arg).strip().removeprefix("[").removesuffix("]")
    ad = FakeAd()
    for item in body.split(";"):
        key, sep, value = item.partition("=")
        if sep:
            ad[key.strip()] = _literal(value)
    return ad


def fake_classad2() -> types.ModuleType:
    module = types.ModuleType("classad2")
    module.ClassAd = classad  # type: ignore[attr-defined]
    return module


_TERM = re.compile(r'^\(?\s*(\w+)\s*(?:==|=\?=)\s*("[^"]*"|-?\d+|true|false)\s*\)?$', re.IGNORECASE)


def matches(ad: Mapping[str, Any], constraint: str) -> bool:
    """``constraint``, a conjunction of ``Attr == literal`` terms, over ``ad`` (names case-insensitive)."""
    text = constraint.strip()
    if text.lower() in ("", "true"):
        return True
    lower = {k.lower(): v for k, v in ad.items()}
    for term in text.split("&&"):
        m = _TERM.match(term.strip())
        if m is None:
            raise ValueError(f"the m68d recorded schedd cannot evaluate {constraint!r}")
        if lower.get(m.group(1).lower()) != _literal(m.group(2)):
            return False
    return True


def driver_job_ad() -> FakeAd:
    """The DAG's running driver node, holding 8000 MiB of ``SLOT``."""
    return FakeAd(
        ClusterId=DRIVER_CLUSTER,
        ProcId=0,
        DAGManJobId=DAG_ID,
        DAGNodeName="driver",
        JobStatus=2,
        RemoteHost="slot1_1@m-node.example",
        RequestMemory=8000,
        MemoryProvisioned=8000,
        RequestCpus=1,
        CpusProvisioned=1,
        RequestGPUs=0,
        GPUsProvisioned=0,
        Requirements=True,
    )


def node_ad(status: int, memory_mb: int = 4000, **extra: Any) -> FakeAd:
    """svc0's queue ad in ``status``, asking ``memory_mb``, one CPU and one GPU."""
    return FakeAd(
        ClusterId=NODE_CLUSTER,
        ProcId=0,
        DAGManJobId=DAG_ID,
        DAGNodeName=NODE,
        JobStatus=status,
        RequestMemory=memory_mb,
        RequestCpus=1,
        RequestGPUs=1,
        Requirements=True,
        **extra,
    )


def machine_ads() -> list[FakeAd]:
    """One partitionable slot of 16000 MiB, 8 CPUs and 1 GPU, busy down to 2000 MiB free, and its
    dynamic slot (which the collector lists too)."""
    return [
        FakeAd(
            MyType="Machine",
            Name=SLOT,
            Machine="m-node.example",
            SlotType="Partitionable",
            PartitionableSlot=True,
            TotalSlotMemory=16000,
            TotalSlotCpus=8,
            TotalSlotGPUs=1,
            Memory=2000,
            Cpus=1,
            GPUs=1,
            Requirements=True,
        ),
        FakeAd(
            MyType="Machine",
            Name="slot1_1@m-node.example",
            Machine="m-node.example",
            SlotType="Dynamic",
            Memory=8000,
            Cpus=1,
            GPUs=0,
            Requirements=True,
        ),
    ]


class SubmitResult:
    def __init__(self, cluster: int) -> None:
        self._cluster = cluster

    def cluster(self) -> int:
        return self._cluster


class NodeSchedd:
    """A stand-in ``htcondor2.Schedd`` holding the driver node's ad and svc0's, whose ad follows ``phases``
    (``(until_s, ad or None)`` from the first query; the last one's ``until_s`` is ignored). ``query``
    evaluates its constraint, honours its projection, and logs ``("query", constraint, projection,
    svc0's JobStatus then, monotonic time)``; with ``refuse`` set it raises that text instead."""

    def __init__(self, phases: Sequence[tuple[float, FakeAd | None]] = (), refuse: str | None = None) -> None:
        self.started: float | None = None
        self.phases = list(phases)
        self.refuse = refuse
        self.driver = driver_job_ad()
        self.log: list[tuple[Any, ...]] = []

    def node(self) -> FakeAd | None:
        if self.started is None:
            self.started = time.monotonic()
        elapsed = time.monotonic() - self.started
        for i, (until_s, ad) in enumerate(self.phases):
            if i == len(self.phases) - 1 or elapsed < until_s:
                return ad
        return None

    def query(self, constraint: Any = "true", projection: Any = None, *args: Any, **kwargs: Any) -> list[Any]:
        text = str(kwargs.get("constraint", constraint))
        proj = tuple(kwargs.get("projection", projection) or ())
        ad = self.node()
        self.log.append(("query", text, proj, None if ad is None else ad.get("JobStatus"), time.monotonic()))
        if self.refuse is not None:
            raise RuntimeError(self.refuse)
        rows = [job for job in (self.driver, ad) if job is not None and matches(job, text)]
        return [FakeAd({k: job[k] for k in proj if k in job}) if proj else FakeAd(job) for job in rows]

    def history(self, constraint: Any = None, projection: Any = None, *args: Any, **kwargs: Any) -> list[Any]:
        self.log.append(("history", str(constraint), tuple(projection or ())))
        return []

    def submit(self, description: Any, count: int = 0, spool: bool = False, **kwargs: Any) -> SubmitResult:
        self.log.append(("submit", dict(description), count, spool))
        return SubmitResult(DAG_ID)

    def spool(self, *args: Any, **kwargs: Any) -> None:
        self.log.append(("spool",))

    def retrieve(self, *args: Any, **kwargs: Any) -> None:
        self.log.append(("retrieve",))

    def act(self, action: Any, constraint: Any = None, *args: Any, **kwargs: Any) -> None:
        self.log.append(("act", str(action), str(constraint)))

    def node_queries(self) -> list[tuple[Any, ...]]:
        """The logged queries whose constraint names svc0 by its node name within its DAG."""
        return [e for e in self.log if e[0] == "query" and names_the_node(e[1])]


def names_the_node(constraint: str) -> bool:
    return bool(
        re.search(rf"DAGManJobId\s*(==|=\?=)\s*{DAG_ID}\b", constraint, re.IGNORECASE)
        and re.search(rf'DAGNodeName\s*(==|=\?=)\s*"{NODE}"', constraint, re.IGNORECASE)
    )


class FakeCollector:
    def __init__(self, fake: FakeHTCondor, pool: str | None) -> None:
        self.fake = fake
        self.pool = pool

    def locate(self, daemon_type: Any, name: str | None = None, *args: Any, **kwargs: Any) -> dict[str, Any]:
        self.fake.log.append(("locate", self.pool, str(daemon_type), name))
        if self.fake.locate_refuses:
            raise RuntimeError(LOCATE_TEXT)
        return {"Name": name or FAKE_SCHEDD, "MyAddress": "<127.0.0.1:9618>", "CondorVersion": "$CondorVersion$"}

    def query(self, ad_type: Any = None, constraint: Any = None, projection: Any = None, **kwargs: Any) -> list[Any]:
        text = str(kwargs.get("constraint", constraint))
        self.fake.log.append(("collector-query", self.pool, str(ad_type), text))
        if "Machine" in text or str(ad_type) in ("Startd", "Machine"):
            return [FakeAd(ad) for ad in self.fake.machines]
        return [
            FakeAd(
                Name=FAKE_SCHEDD,
                RecentDaemonCoreDutyCycle=0.1,
                ShadowsRunning=1,
                MaxJobsRunning=10,
                TotalIdleJobs=1,
            )
        ]


class _SubmitType:
    def __init__(self, fake: FakeHTCondor) -> None:
        self.fake = fake

    def __call__(self, description: Any = None, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return dict(description or {})

    def from_dag(self, filename: Any, options: Any = None, **kwargs: Any) -> dict[str, Any]:
        self.fake.log.append(("from_dag", str(filename), dict(options or {})))
        return {"dag_file": str(filename)}


class _Credd:
    def query_user_cred(self, *args: Any, **kwargs: Any) -> int:
        return 1


class FakeHTCondor:
    """What ``launch._htcondor()`` returns under the recorder; ``log`` is the schedd's."""

    class DaemonType:
        Schedd = "Schedd"

    class AdType:
        Schedd = "Schedd"
        Startd = "Startd"

    class JobAction:
        Remove = "Remove"
        Hold = "Hold"
        Release = "Release"

    class CredType:
        Kerberos = "Kerberos"

    def __init__(self, schedd: NodeSchedd, *, locate_refuses: bool = False) -> None:
        self.schedd = schedd
        self.log = schedd.log
        self.locate_refuses = locate_refuses
        self.machines = machine_ads()
        self.Submit = _SubmitType(self)
        self.param = {
            "COLLECTOR_HOST": FAKE_POOL,
            "SCHEDD_HOST": FAKE_SCHEDD,
            "FULL_HOSTNAME": DRIVER_ID,
        }

    def Collector(self, pool: str | None = None, *args: Any, **kwargs: Any) -> FakeCollector:
        self.log.append(("Collector", pool))
        return FakeCollector(self, pool)

    def Schedd(self, location: Any = None, *args: Any, **kwargs: Any) -> NodeSchedd:
        self.log.append(("Schedd", None if location is None else location.get("Name")))
        return self.schedd

    def Credd(self, *args: Any, **kwargs: Any) -> _Credd:
        return _Credd()

    def RemoteParam(self, location: Any) -> dict[str, str]:
        return {"BIN": "/usr/bin"}


def record_bindings(monkeypatch: pytest.MonkeyPatch, fake: FakeHTCondor) -> FakeHTCondor:
    """``launch._htcondor`` and ``sys.modules["htcondor2"]`` answer with ``fake``."""
    monkeypatch.setattr(launch_api(), "_htcondor", lambda: fake)
    monkeypatch.setitem(sys.modules, "htcondor2", fake)
    return fake


def logged(log: list[tuple[Any, ...]], kind: str) -> list[tuple[Any, ...]]:
    return [entry for entry in log if entry[0] == kind]
