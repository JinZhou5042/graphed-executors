"""Harness for the m68b part B2 frozen tests (plan-services.md §3.3 "B2 — DAG driverless, `job_root`,
lxplus"): a driverless plan whose services need SERVICE nodes is submitted as a DAG, and
``RunHandle(dag=True)`` tracks the DAGMan cluster and its ``driver`` node.

Frozen suites never import across directories, and B1's ``m68b_harness`` is its own, so what this part
needs is copied here: the accessors (looked up inside test bodies, so the suite collects before the
implementation exists), the bounds, the m67 bindings recorder, a fake venv, a machine ad, HMAC signing
and the m66 ``MarkerBomb``. What is new:

- ``DagSchedd``: ``query``/``history`` answer by constraint (one naming ``DAGNodeName`` is the driver
  node's, any other the DAGMan cluster's) and ``history`` logs its ``match``.
- ``FakeHTCondor.Submit``: called, it returns a ``FakeSubmit`` (a dict that prints as a submit file);
  its ``from_dag`` is logged and, given real bindings, delegated to them.
- ``read_sub``/``dag_statements``: what the submitter wrote, parsed.
- The live file's plan parts (``BodyGet``, ``kill_driver``). This file is shipped to the job as a
  ``user_modules`` entry, so it imports only the stdlib, graphed and graphed_executors at module level.
"""

from __future__ import annotations

import hashlib
import hmac
import importlib
import json
import os
import socket
import sys
import sysconfig
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, replace
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import TYPE_CHECKING, Any

from graphed.core.execution import Partition, Plan, Task
from graphed.services import Launch, ServiceSpec, UnboundService

from graphed_executors.local._transport import LookupFreeHTTPServer

if TYPE_CHECKING:
    import pytest

HARNESS_DIR = str(Path(__file__).resolve().parent)
HARNESS_FILE = str(Path(__file__).resolve())
DATA_DIR = Path(HARNESS_DIR) / "data"

RUN_TIMEOUT_S = 240.0
SERVICES_LOGGER = "graphed_executors.services"
SIG_HEADER = "X-Graphed-Sig"
DRIVER_MODULE = "graphed_executors.htcondor_backend.driver"

FAKE_POOL = "cm.m68b.example:9618"
FAKE_SCHEDD = "schedd.m68b.example"
FAKE_CLUSTER = 4242

# the only options the DAG is submitted with (§3.3 B2 "DAG")
DAG_OPTIONS = {"UseDagDir": True, "AddToEnv": "_CONDOR_DAGMAN_USE_STRICT=0"}
PLACEHOLDER_TEXT = "exited before writing a result"
IMAGE = "/cvmfs/unpacked.cern.ch/registry.hub.docker.com/coffeateam/coffea-almalinux9-noml:2026.9.0-py3.12"
TRITON_IMAGE = "/cvmfs/unpacked.cern.ch/nvcr.io/nvidia/tritonserver:24.11-py3"

# ---- deferred accessors for the implementation under test ---------------------------------------


def htcondor_api() -> Any:
    """``graphed_executors.htcondor_backend``: SITES, SiteProfile, submit_driverless, RunHandle."""
    return importlib.import_module("graphed_executors.htcondor_backend")


def driverless_api() -> Any:
    return importlib.import_module("graphed_executors.htcondor_backend.driverless")


def driver_api() -> Any:
    """``graphed_executors.htcondor_backend.driver``: ``_runner(run, job, log)``."""
    return importlib.import_module(DRIVER_MODULE)


def launch_api() -> Any:
    """``graphed_executors.htcondor_backend.launch``: every bindings call goes through ``_htcondor()``."""
    return importlib.import_module("graphed_executors.htcondor_backend.launch")


def services_api() -> Any:
    """``graphed_executors.submit.services``: ServiceSet."""
    return importlib.import_module("graphed_executors.submit.services")


def recipes_api() -> Any:
    return importlib.import_module("graphed_executors.submit.recipes")


# ---- bounds --------------------------------------------------------------------------------------


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


def wait_for(predicate: Callable[[], bool], timeout_s: float = 30.0, poll_s: float = 0.05) -> bool:
    """Poll a predicate within a bound; returns its last value (the assertion comes after)."""
    deadline = time.monotonic() + timeout_s
    while not predicate():
        if time.monotonic() >= deadline:
            return predicate()
        time.sleep(poll_s)
    return True


# ---- ports, HTTP, signing --------------------------------------------------------------------------


def _bindable(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("", port))
        except OSError:
            return False
    return True


def free_range(n: int = 3) -> tuple[int, int]:
    """An inclusive range of ``n`` consecutive free ports, outside the 10000-10100 band of the site rows."""
    for low in range(20000 + os.getpid() % 1000 * 20, 60000, 20):
        if all(_bindable(p) for p in range(low, low + n)):
            return (low, low + n - 1)
    raise AssertionError(f"no {n} consecutive free ports found")


def sign(secret: bytes, body: bytes) -> str:
    """The hex HMAC-SHA256 pilots and ``announce.py`` put in ``X-Graphed-Sig``."""
    return hmac.new(secret, body, hashlib.sha256).hexdigest()


def post(url: str, body: bytes, secret: bytes, timeout: float = 30.0) -> int:
    """The status of a POST of ``body`` signed with ``secret``."""
    request = urllib.request.Request(url, data=body, method="POST")
    request.add_header(SIG_HEADER, sign(secret, body))
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return int(response.status)
    except urllib.error.HTTPError as exc:
        return int(exc.code)


def http_body(endpoint: str, timeout_s: float = 10.0) -> str:
    """The body a GET of ``/`` answers on an ``http://`` endpoint (anything but a 2xx raises)."""
    scheme, _, rest = endpoint.partition("://")
    assert scheme == "http", endpoint
    with urllib.request.urlopen(f"http://{rest}/", timeout=timeout_s) as resp:
        return str(resp.read().decode())


@contextmanager
def ok_server() -> Iterator[int]:
    """A lookup-free HTTP server on ``127.0.0.1`` answering every GET 200 ``text/plain``; yields its port."""

    class _Ok(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            body = b"m68b-b2"
            self.send_response(200)
            self.send_header("content-type", "text/plain")
            self.send_header("content-length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            return None

    server = LookupFreeHTTPServer(("127.0.0.1", 0), _Ok)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield int(server.server_address[1])
    finally:
        server.shutdown()
        server.server_close()


def touch_marker(path: str) -> str:
    Path(path).touch()
    return path


@dataclass(frozen=True)
class MarkerBomb:
    """Unpickling this object creates ``path``: the file's existence shows ``pickle.loads`` ran."""

    path: str

    def __reduce__(self) -> tuple[Any, tuple[str]]:
        return touch_marker, (self.path,)


# ---- site data ----------------------------------------------------------------------------------------


def fake_venv(root: Path) -> Path:
    """A directory shaped like a non-editable venv with one dist-info (no interpreter in it)."""
    root.mkdir(parents=True)
    (root / "pyvenv.cfg").write_text("home = /usr/bin\ninclude-system-site-packages = false\n")
    purelib = Path(
        sysconfig.get_path("purelib", scheme="venv", vars={"base": str(root), "platbase": str(root)})
    )
    dist = purelib / "probedist-0.1.dist-info"
    dist.mkdir(parents=True)
    (dist / "direct_url.json").write_text(json.dumps({"url": "file:///src/probedist", "dir_info": {}}))
    return root


def write_machine_ad(path: Path, host: str = "localhost") -> Path:
    """A machine ad file in the classad text form of ``$_CONDOR_MACHINE_AD``."""
    path.write_text(f'Machine = "{host}"\nName = "slot1@{host}"\nCpus = 2\n')
    return path


def site_copy(monkeypatch: pytest.MonkeyPatch, key: str, base: str, **changes: Any) -> Any:
    """``dataclasses.replace(SITES[base], **changes)`` registered as ``SITES[key]`` for this test."""
    api = htcondor_api()
    profile = replace(api.SITES[base], **changes)
    monkeypatch.setitem(api.SITES, key, profile)
    return profile


# ---- recorded bindings ---------------------------------------------------------------------------------


class SubmitResult:
    def __init__(self, cluster: int) -> None:
        self._cluster = cluster

    def cluster(self) -> int:
        return self._cluster


class FakeSubmit(dict[str, Any]):
    """What the fake ``htcondor2.Submit(...)`` returns: a dict that prints as a submit file does."""

    def __str__(self) -> str:
        return "".join(f"{k} = {v}\n" for k, v in self.items()) + "queue\n"


def is_node_constraint(constraint: str) -> bool:
    return "DAGNodeName" in constraint


class DagSchedd:
    """A stand-in ``htcondor2.Schedd`` that appends every call to ``log``. ``query`` and ``history`` answer
    the driver node's constraint (one naming ``DAGNodeName``) from ``node``/``node_history`` and every
    other from ``dagman``/``dagman_history``; ``history`` logs its ``match``. ``retrieve`` writes nothing."""

    def __init__(
        self,
        dagman: list[dict[str, Any]] | None = None,
        dagman_history: list[dict[str, Any]] | None = None,
        node: list[dict[str, Any]] | None = None,
        node_history: list[dict[str, Any]] | None = None,
    ) -> None:
        self.log: list[tuple[Any, ...]] = []
        self.dagman = list(dagman or [])
        self.dagman_history = list(dagman_history or [])
        self.node = list(node or [])
        self.node_history = list(node_history or [])

    def query(self, constraint: Any = "true", projection: Any = None, *args: Any, **kwargs: Any) -> list[Any]:
        text = str(constraint)
        self.log.append(("query", text, tuple(projection or ())))
        return [dict(ad) for ad in (self.node if is_node_constraint(text) else self.dagman)]

    def history(self, constraint: Any = None, projection: Any = None, *args: Any, **kwargs: Any) -> list[Any]:
        text = str(constraint)
        match = kwargs.get("match", args[0] if args else None)
        self.log.append(("history", text, tuple(projection or ()), match))
        return [dict(ad) for ad in (self.node_history if is_node_constraint(text) else self.dagman_history)]

    def submit(self, description: Any, count: int = 0, spool: bool = False, **kwargs: Any) -> SubmitResult:
        self.log.append(("submit", dict(description), count, spool))
        return SubmitResult(FAKE_CLUSTER)

    def spool(self, result: Any, *args: Any, **kwargs: Any) -> None:
        self.log.append(("spool",))

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
    ) -> list[dict[str, Any]]:
        self.fake.log.append(("collector-query", self.pool))
        return [
            {
                "Name": FAKE_SCHEDD,
                "RecentDaemonCoreDutyCycle": 0.1,
                "ShadowsRunning": 1,
                "MaxJobsRunning": 10,
                "TotalIdleJobs": 1,
            }
        ]


class _SubmitType:
    """``htcondor2.Submit``: a call builds a description, ``from_dag`` is logged (and delegated to real
    bindings when the fake has them)."""

    def __init__(self, fake: FakeHTCondor) -> None:
        self.fake = fake

    def __call__(self, description: Any = None, *args: Any, **kwargs: Any) -> FakeSubmit:
        return FakeSubmit(dict(description or {}))

    def from_dag(self, filename: Any, options: Any = None, **kwargs: Any) -> Any:
        opts = dict(options if options is not None else kwargs.get("options", {}))
        self.fake.log.append(("from_dag", str(filename), opts))
        if self.fake.real is not None:
            return self.fake.real.Submit.from_dag(str(filename), opts)
        return FakeSubmit({"dag_file": str(filename)})


class _Credd:
    """A credd that holds the user's Kerberos credential, so a submit stores none."""

    def query_user_cred(self, *args: Any, **kwargs: Any) -> int:
        return 1790802437


class FakeHTCondor:
    """What ``launch._htcondor()`` returns under the recorder: ``param``, ``Collector``, ``Schedd``,
    ``Submit`` and the enums the backend names."""

    class DaemonType:
        Schedd = "Schedd"

    class AdType:
        Schedd = "Schedd"

    class JobAction:
        Remove = "Remove"

    def __init__(self, schedd: DagSchedd, real: Any = None) -> None:
        self.schedd = schedd
        self.log = schedd.log
        self.real = real
        self.Submit = _SubmitType(self)
        self.param = {
            "COLLECTOR_HOST": FAKE_POOL,
            "SCHEDD_HOST": FAKE_SCHEDD,
            "FERMIHTC_REMOTE_POOL": FAKE_POOL,
            "FULL_HOSTNAME": "login.m68b.example",
        }

    class CredType:
        Kerberos = "Kerberos"

    def Credd(self, *args: Any, **kwargs: Any) -> _Credd:
        return _Credd()

    def RemoteParam(self, location: Any) -> dict[str, str]:
        """The located schedd's config: the CI pool's RPM layout."""
        return {"BIN": "/usr/bin"}

    def Collector(self, pool: str | None = None, *args: Any, **kwargs: Any) -> _Collector:
        self.log.append(("Collector", pool))
        return _Collector(self, pool)

    def Schedd(self, location: Any = None, *args: Any, **kwargs: Any) -> DagSchedd:
        self.log.append(("Schedd", None if location is None else location["Name"]))
        return self.schedd


def record_bindings(monkeypatch: pytest.MonkeyPatch, schedd: DagSchedd, real: Any = None) -> FakeHTCondor:
    """Patch ``launch._htcondor`` (logging ``("_htcondor",)``) and ``sys.modules["htcondor2"]`` to a
    ``FakeHTCondor`` over ``schedd``; ``real`` is the bindings module ``from_dag`` delegates to."""
    fake = FakeHTCondor(schedd, real)

    def _htcondor() -> FakeHTCondor:
        fake.log.append(("_htcondor",))
        return fake

    monkeypatch.setattr(launch_api(), "_htcondor", _htcondor)
    monkeypatch.setitem(sys.modules, "htcondor2", fake)
    return fake


def logged(log: list[tuple[Any, ...]], kind: str) -> list[tuple[Any, ...]]:
    return [entry for entry in log if entry[0] == kind]


def submitted(log: list[tuple[Any, ...]]) -> list[dict[str, Any]]:
    """The description of every ``schedd.submit`` in ``log``, in order."""
    return [entry[1] for entry in logged(log, "submit")]


# ---- what the submitter wrote --------------------------------------------------------------------


def read_sub(path: Path) -> dict[str, str]:
    """A submit file's ``key = value`` statements, keys lower-cased (condor's keys are case-insensitive)."""
    out: dict[str, str] = {}
    for line in path.read_text().splitlines():
        text = line.strip()
        if not text or text.startswith("#") or text.split()[0].lower() == "queue":
            continue
        key, sep, value = text.partition("=")
        assert sep, f"{path.name}: not a submit statement: {line!r}"
        out[key.strip().lower()] = value.strip()
    return out


def job_attr(sub: Mapping[str, str], name: str) -> str | None:
    """A job ad attribute set in a submit file as ``MY.<name>`` or ``+<name>``."""
    return sub.get(f"my.{name.lower()}", sub.get(f"+{name.lower()}"))


def squashed(value: str | None) -> str | None:
    return None if value is None else " ".join(value.split())


def dag_statements(path: Path) -> list[tuple[str, ...]]:
    """``run.dag``'s statements as token tuples, the keyword upper-cased (DAGMan's are case-insensitive)."""
    rows: list[tuple[str, ...]] = []
    for line in path.read_text().splitlines():
        words = line.split()
        if words and not words[0].startswith("#"):
            rows.append((words[0].upper(), *words[1:]))
    return rows


def tree_bytes(root: Path) -> dict[str, bytes]:
    """Every file under ``root`` (links not followed into directories) by relative path: its bytes, or
    its link target for a symlink."""
    out: dict[str, bytes] = {}
    for dirpath, dirnames, filenames in os.walk(root):
        for name in [*filenames, *(d for d in dirnames if os.path.islink(os.path.join(dirpath, d)))]:
            path = Path(dirpath, name)
            rel = str(path.relative_to(root))
            out[rel] = os.readlink(path).encode() if path.is_symlink() else path.read_bytes()
    return out


# ---- specs and plans ----------------------------------------------------------------------------


def http_spec(name: str, *, timeout_s: float = 60.0, **launch: Any) -> ServiceSpec:
    """An ``http:/``-checked requirement whose recipe runs ``http.server`` (``launch`` fields: image,
    inputs, env, resources)."""
    argv = ("{python}", "-m", "http.server", "{port}")
    return ServiceSpec(
        name,
        "http",
        check="http:/",
        ports=(40000, 40010),
        launch=Launch(argv=argv, **launch),
        timeout_s=timeout_s,
    )


def gpu_spec(name: str, **launch: Any) -> ServiceSpec:
    """Image-less, one GPU: a driver job cannot host it."""
    return http_spec(name, resources={"gpus": 1}, **launch)


def imaged_spec(name: str) -> ServiceSpec:
    """In an image: a driver job cannot host it."""
    return http_spec(name, image="registry.m68b.example/service:1")


def cpu_spec(name: str) -> ServiceSpec:
    """Image-less, no GPU: the driver job hosts it beside itself."""
    return http_spec(name)


def mem_partitions(n: int, tag: str) -> tuple[Partition, ...]:
    return tuple(Partition(f"mem://{tag}/{i}", "", i, i + 1) for i in range(n))


def text_process(partition: Partition, resources: object) -> tuple[Any, ...]:
    return (partition.uri,)


def pair_concat(a: tuple[Any, ...], b: tuple[Any, ...]) -> tuple[Any, ...]:
    return a + b


def empty_tuple() -> tuple[Any, ...]:
    return ()


def service_plan(specs: Sequence[ServiceSpec], process: Any = text_process, n: int = 2) -> Plan[Any]:
    tasks = tuple(Task(i, p) for i, p in enumerate(mem_partitions(n, "m68b-b2")))
    return Plan(process=process, combine=pair_concat, empty=empty_tuple, tasks=tasks, services=tuple(specs))


# ---- the live file's plan parts (the job imports them from this file by name) ----------------------


@dataclass(frozen=True)
class DagResolved:
    """A run's value after the driver resolved it: the leaves and the body the service answered then."""

    leaves: Any
    body: str


@dataclass(frozen=True)
class BodyGet:
    """Each pilot task GETs ``/`` on the bound service; ``resolve_services`` GETs it again in the driver."""

    service: str = "web"
    endpoint: str | None = None

    def bind_services(self, endpoints: Mapping[str, str]) -> BodyGet:
        endpoint = endpoints.get(self.service, self.endpoint)
        if endpoint is None:
            raise UnboundService(self.service)
        return replace(self, endpoint=endpoint)

    def __call__(self, partition: Partition, resources: object) -> tuple[str, ...]:
        assert self.endpoint is not None, "process called unbound"
        return (http_body(self.endpoint),)

    def resolve_services(self, value: Any) -> DagResolved:
        assert self.endpoint is not None, "resolve_services called on an unbound process"
        return DagResolved(value, http_body(self.endpoint))


def kill_driver(partition: Partition, resources: object) -> tuple[Any, ...]:
    """On a pilot of a ``pilots="local"`` driver job: SIGKILL the pilot's parent, the driver."""
    os.kill(os.getppid(), 9)  # SIGKILL, spelled so the module type-checks on every platform
    time.sleep(120.0)  # the job's end takes this pilot with it
    return ()
