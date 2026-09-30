"""Shared harness for the m68b frozen suite: condor cluster-hosted services (plan-services.md §3.3).

Frozen suites never import across directories, so what this suite needs from m66/m67/m68a is copied here
under a basename of its own (a second ``services_harness`` would shadow m68a's in ``sys.modules``): the
deferred accessors, the bounded runners, the reaped-pid and freed-port witnesses, the bindings recorder
(``RecordingSchedd``/``FakeHTCondor``/``record_bindings``), ``write_machine_ad``, ``fake_venv``,
``NoStartLauncher`` and the m66 auth pieces (``sign``, ``post``, ``MarkerBomb``). What is new:

- ``AnnounceRun``: ``announce.py`` run as ``python -m graphed_executors.htcondor_backend.announce
  service.json`` in a job dir, the same code the job runs by path, so ``test-htcondor``'s coverage records it
  (the env keeps ``COVERAGE_PROCESS_START``); its output is read on a thread, so a test can wait for a line.
- ``job_config``/``write_job``: a job dir as ``ServiceJob`` lays it out (``service.json``, ``graphed-secret``,
  optionally ``service/``), from the plan's field list.
- ``m68b_child.py`` (``child_argv``): the managed child, which reports its own state at start.
- Spies that record method calls into one ordered list (``spy_method``).
- Plan parts pilots unpickle for the live file (``TimedGet``, ``GatedGet``); everything a pilot imports is
  module-level here, and the live file ships this file in ``user_modules``.

The implementation under test is reached only through the ``*_api()`` accessors, inside test bodies, so
the suite collects before ``htcondor_backend/services.py`` and ``announce.py`` exist. Every bindings call
goes through ``launch._htcondor`` (``record_bindings`` also answers a direct ``import htcondor2``).
"""

from __future__ import annotations

import contextlib
import hashlib
import hmac
import importlib
import json
import multiprocessing
import os
import secrets
import socket
import subprocess
import sys
import sysconfig
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

from graphed.core.execution import Partition, Plan, Task
from graphed.services import Launch, ServiceSpec, UnboundService

if TYPE_CHECKING:
    import pytest

HARNESS_DIR = str(Path(__file__).resolve().parent)
HARNESS_FILE = str(Path(__file__).resolve())
CHILD_SCRIPT = str(Path(HARNESS_DIR) / "m68b_child.py")

ANNOUNCE_MODULE = "graphed_executors.htcondor_backend.announce"
SIG_HEADER = "X-Graphed-Sig"
RUN_TIMEOUT_S = 240.0
REAP_S = 5.0  # announce.py's one bounded reap: terminate, wait at most 5 s, kill
MARGIN_S = 4.0

FAKE_POOL = "cm.m68b.example:9618"
FAKE_SCHEDD = "schedd.m68b.example"
FAKE_CLUSTER = 4242

# ---- deferred accessors for the implementation under test ---------------------------------------


def htcondor_api() -> Any:
    """``graphed_executors.htcondor_backend``: SITES, SiteProfile, HTCondorBackend, HTCondorRunner, ..."""
    return importlib.import_module("graphed_executors.htcondor_backend")


def launch_api() -> Any:
    """``graphed_executors.htcondor_backend.launch``: CondorPilots; every bindings call goes through
    ``_htcondor()``."""
    return importlib.import_module("graphed_executors.htcondor_backend.launch")


def backend_api() -> Any:
    """``graphed_executors.htcondor_backend.backend``: HTCondorBackend, HTCondorRunner, htcondor_runner."""
    return importlib.import_module("graphed_executors.htcondor_backend.backend")


def server_api() -> Any:
    """``graphed_executors.htcondor_backend.server``: TaskServer (announce_secret, forget_announce,
    wait_announce), LEASE_S, POLL_S."""
    return importlib.import_module("graphed_executors.htcondor_backend.server")


def cluster_api() -> Any:
    """``graphed_executors.htcondor_backend.services``: ServiceJob."""
    return importlib.import_module("graphed_executors.htcondor_backend.services")


def announce_api() -> Any:
    """``graphed_executors.htcondor_backend.announce``: main, serve, on_sigterm, _Stop, CHILD."""
    return importlib.import_module(ANNOUNCE_MODULE)


def services_api() -> Any:
    """``graphed_executors.submit.services``: host_identity, ServiceSet, ..."""
    return importlib.import_module("graphed_executors.submit.services")


def recipes_api() -> Any:
    """``graphed_executors.submit.recipes``: http_server, triton."""
    return importlib.import_module("graphed_executors.submit.recipes")


# ---- bounds ----------------------------------------------------------------------------------------


def run_bounded(fn: Callable[[], Any], timeout_s: float = RUN_TIMEOUT_S) -> Any:
    """Run ``fn`` on a worker thread and fail if it does not finish in ``timeout_s``: a hang is a failure,
    never a wedged CI job. Returns the value or re-raises the call's exception."""
    try:
        return in_background(fn).get(timeout_s)
    except multiprocessing.TimeoutError:
        raise AssertionError(f"HARD TIMEOUT: call did not finish within {timeout_s}s") from None


def in_background(fn: Callable[[], Any]) -> Any:
    """``fn`` started on a daemon thread; the handle's ``ready()``/``get(t)`` behave as ``AsyncResult``'s."""
    out: dict[str, Any] = {}

    def _drive() -> None:
        try:
            out["result"] = fn()
        except BaseException as exc:
            out["error"] = exc

    thread = threading.Thread(target=_drive, daemon=True)
    thread.start()

    def get(timeout_s: float | None = None) -> Any:
        thread.join(timeout_s)
        if thread.is_alive():
            raise multiprocessing.TimeoutError
        if "error" in out:
            raise out["error"]
        return out["result"]

    return SimpleNamespace(ready=lambda: not thread.is_alive(), get=get)


def get_within(result: Any, timeout_s: float, what: str) -> Any:
    """``result.get(timeout_s)`` of an ``in_background`` call, failing with ``what`` when it is not done."""
    try:
        return result.get(timeout_s)
    except multiprocessing.TimeoutError:
        raise AssertionError(f"{what}: not done within {timeout_s}s") from None


def wait_announce(server: Any, key: str, timeout_s: float) -> Any:
    """``server.wait_announce(key, timeout_s)``, failing when it overruns its own timeout by ``MARGIN_S``."""
    return run_bounded(lambda: server.wait_announce(key, timeout_s), timeout_s + MARGIN_S)


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
    """Whether ``pid`` is no longer a live (or unreaped) process: ``os.kill(pid, 0)`` raises
    ``ProcessLookupError`` once its parent has waited on it (POSIX legs only)."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    return False


def kill_quietly(pid: int) -> None:
    """Teardown backstop for a child the implementation failed to stop."""
    if sys.platform == "win32" or pid_gone(pid):
        return
    with contextlib.suppress(OSError):
        os.kill(pid, 9)


def _bindable(host: str, port: int) -> bool:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        if sys.platform != "win32":
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, port))
        return True
    except OSError:
        return False
    finally:
        s.close()


def port_free(port: int) -> bool:
    """Nothing listens on ``port``: it binds on loopback and on all interfaces, and a connect is refused."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1.0)
        listening = s.connect_ex(("127.0.0.1", port)) == 0
    return _bindable("127.0.0.1", port) and _bindable("", port) and not listening


def free_range(n: int = 3) -> tuple[int, int]:
    """An inclusive range of ``n`` consecutive free ports, above the 10000-10100 band the site rows use."""
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
def wildcard_listener(port: int) -> Iterator[socket.socket]:
    """A listener on ``("", port)``, the address ``announce.py``'s free-port bind uses (on macOS a
    ``127.0.0.1`` listener does not refuse that bind)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("", port))
        s.listen(8)
        yield s
    finally:
        s.close()


# ---- HTTP and the task server's signing (the m66 auth pieces) --------------------------------------


def sign(secret: bytes, body: bytes) -> str:
    """The hex HMAC-SHA256 pilots and ``announce.py`` put in ``X-Graphed-Sig``."""
    return hmac.new(secret, body, hashlib.sha256).hexdigest()


def post(url: str, body: bytes, sig: str | None, timeout: float = 30.0) -> int:
    """The status of a POST of ``body`` (signed with ``sig`` when given)."""
    request = urllib.request.Request(url, data=body, method="POST")
    if sig is not None:
        request.add_header(SIG_HEADER, sig)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return int(response.status)
    except urllib.error.HTTPError as exc:
        return int(exc.code)


def announce_body(key: str, hostport: str, identity: str) -> bytes:
    return f"{key} {hostport} {identity}".encode()


def post_announce(url: str, body: bytes, secret: bytes | None) -> int:
    """POST ``body`` to ``<url>/announce``, signed with ``secret`` (unsigned when ``None``)."""
    return post(url.rstrip("/") + "/announce", body, None if secret is None else sign(secret, body))


def http_status(url: str, timeout_s: float = 5.0) -> int:
    """The status a GET of ``url`` answers."""
    try:
        with urllib.request.urlopen(url, timeout=timeout_s) as resp:
            return int(resp.status)
    except urllib.error.HTTPError as exc:
        return int(exc.code)


def http_get(url: str, timeout_s: float = 5.0) -> str:
    """The body of a 2xx answer; anything else raises."""
    with urllib.request.urlopen(url, timeout=timeout_s) as resp:
        return str(resp.read().decode())


def touch_marker(path: str) -> str:
    Path(path).touch()
    return path


@dataclass(frozen=True)
class MarkerBomb:
    """Unpickling this object creates ``path``: the file's existence shows ``pickle.loads`` ran."""

    path: str

    def __reduce__(self) -> tuple[Any, tuple[str]]:
        return touch_marker, (self.path,)


class NoStartLauncher:
    """A ``PilotLauncher`` that starts nothing (a task server's ``launcher`` when no pilot is needed)."""

    def __init__(self) -> None:
        self.log_dir = None

    def start(self, url: str, secret: bytes, n: int) -> None:
        self.url = url

    def alive(self) -> int:
        return 0

    def stop(self) -> None:
        return None


@contextlib.contextmanager
def task_server() -> Iterator[Any]:
    """A real ``TaskServer`` on ``127.0.0.1`` and an ephemeral port; closed and shut down at exit."""
    server = server_api().TaskServer("127.0.0.1", (0, 0), NoStartLauncher())
    try:
        yield server
    finally:
        server.close()
        server.shutdown()


# ---- spies ---------------------------------------------------------------------------------------------


def spy_method(
    monkeypatch: pytest.MonkeyPatch,
    cls: Any,
    name: str,
    events: list[tuple[Any, ...]],
    tag: str | None = None,
) -> None:
    """Replace ``cls.name`` with a wrapper that appends ``(tag, *args, *kwargs.values())`` to ``events`` and
    calls through, so a one-parameter method's argument lands at ``[1]`` however it was passed.
    ``monkeypatch.setattr`` raises when ``cls`` has no ``name``: a missing planned method fails there."""
    real = getattr(cls, name)
    label = tag or name

    def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        passed = (*args, *kwargs.values())
        events.append((label, *[list(a) if isinstance(a, (list, tuple, set)) else a for a in passed]))
        return real(self, *args, **kwargs)

    monkeypatch.setattr(cls, name, wrapper)


def keys_of(events: list[tuple[Any, ...]], tag: str) -> list[str]:
    """Every key an ``announce_secret``/``forget_announce`` spy saw under ``tag``, in order."""
    out: list[str] = []
    for event in events:
        if event[0] == tag:
            out.extend(str(k) for k in event[1])
    return out


# ---- recorded bindings (copied from m68a) -----------------------------------------------------------


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

    def __init__(self, schedd: RecordingSchedd, full_hostname: str = "login.m68b.example") -> None:
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
    """Patch ``launch._htcondor`` (and ``sys.modules["htcondor2"]``) to a ``FakeHTCondor`` over ``schedd``."""
    fake = FakeHTCondor(schedd, **kwargs)

    def _htcondor() -> FakeHTCondor:
        fake.log.append(("_htcondor",))
        return fake

    monkeypatch.setattr(launch_api(), "_htcondor", _htcondor)
    monkeypatch.setitem(sys.modules, "htcondor2", fake)
    return fake


def submits(log: list[tuple[Any, ...]]) -> list[dict[str, Any]]:
    """The description of every ``schedd.submit`` in ``log``, in order."""
    return [entry[1] for entry in log if entry[0] == "submit"]


def input_entries(desc: Mapping[str, Any]) -> list[str]:
    return [e.strip() for e in str(desc["transfer_input_files"]).split(",") if e.strip()]


def write_machine_ad(path: Path, host: str = "localhost") -> Path:
    """A machine ad file in the classad text form of ``$_CONDOR_MACHINE_AD``."""
    path.write_text(f'Machine = "{host}"\nName = "slot1@{host}"\nCpus = 2\n')
    return path


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


# ---- the job side: announce.py in a job dir ----------------------------------------------------------


def child_argv(mode: str, report: Path, *extra: str, python: str = "{python}") -> tuple[str, ...]:
    """A recipe argv running ``m68b_child.py <mode> {port} <report> [extra...]``."""
    return (python, CHILD_SCRIPT, mode, "{port}", str(report), *extra)


def job_config(**overrides: Any) -> dict[str, Any]:
    """``service.json`` as the plan lists its fields (attached mode: ``url`` set, ``watch`` null)."""
    cfg: dict[str, Any] = {
        "argv": ["{python}", "-m", "http.server", "{port}"],
        "env": {},
        "check": "http:/",
        "ports": list(free_range(3)),
        "key": "k-m68b",
        "url": None,
        "watch": None,
        "python": sys.executable,
        "timeout_s": 30.0,
        "lease_s": 30.0,
        "beat_s": 1.0,
    }
    cfg.update(overrides)
    cfg["argv"] = list(cfg["argv"])
    cfg["ports"] = list(cfg["ports"])
    return cfg


def write_job(root: Path, cfg: Mapping[str, Any], secret: bytes | None) -> Path:
    """A job dir holding ``service.json`` and, in attached mode, ``graphed-secret`` (hex, as
    ``write_secret`` writes it)."""
    root.mkdir(parents=True, exist_ok=True)
    (root / "service.json").write_text(json.dumps(dict(cfg)))
    if secret is not None:
        (root / "graphed-secret").write_text(secret.hex())
    return root


def announce_env(machine_ad: Path, **extra: str) -> dict[str, str]:
    """The job's environment: this process's (``COVERAGE_PROCESS_START`` included) plus a machine ad
    whose ``Machine`` names the identity announced."""
    return {**os.environ, "_CONDOR_MACHINE_AD": str(machine_ad), **extra}


def read_report(path: Path, timeout_s: float = 30.0) -> dict[str, Any]:
    """What ``m68b_child.py`` wrote at start."""
    wait_for(path.is_file, timeout_s)
    assert path.is_file(), f"the managed child never started: no report at {path}"
    report: dict[str, Any] = json.loads(path.read_text())
    return report


def child_starts(report: Path) -> int:
    """How many times a child writing ``report`` was started."""
    starts = Path(f"{report}.starts")
    return len(starts.read_text().splitlines()) if starts.is_file() else 0


class AnnounceRun:
    """``python -m graphed_executors.htcondor_backend.announce service.json`` with ``cwd=job``, as the job
    runs it. Output (stdout and stderr) is collected on a thread with the time each line arrived; leaving
    the ``with`` kills it and every child pid in ``reports`` still alive (a backstop: tests assert first)."""

    def __init__(self, job: Path, env: Mapping[str, str], reports: tuple[Path, ...] = ()) -> None:
        self.reports = reports
        self.started = time.monotonic()
        self.proc = subprocess.Popen(
            [sys.executable, "-m", ANNOUNCE_MODULE, "service.json"],
            cwd=job,
            env=dict(env),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        self.lines: list[tuple[float, str]] = []
        self._lock = threading.Lock()
        self.ended: float | None = None
        self._reader = threading.Thread(target=self._read, daemon=True)
        self._reader.start()

    def _read(self) -> None:
        assert self.proc.stdout is not None
        for line in self.proc.stdout:
            with self._lock:
                self.lines.append((time.monotonic(), line))

    def output(self) -> str:
        with self._lock:
            return "".join(line for _t, line in self.lines)

    def line_time(self, text: str, timeout_s: float = 60.0) -> float | None:
        """When the first line holding ``text`` arrived, or ``None`` within the bound."""

        def found() -> float | None:
            with self._lock:
                return next((t for t, line in self.lines if text in line), None)

        wait_for(lambda: found() is not None or self.proc.poll() is not None, timeout_s)
        return found()

    def report(self, path: Path, timeout_s: float = 60.0) -> dict[str, Any]:
        """What the child wrote at start; fails naming announce.py's output when none appears."""
        wait_for(lambda: path.is_file() or self.proc.poll() is not None, timeout_s)
        if not path.is_file():
            raise AssertionError(f"the managed child never started; announce.py output:\n{self.output()}")
        return read_report(path)

    def wait(self, timeout_s: float) -> int:
        """The exit code, or a failure naming the output when it does not exit within ``timeout_s``."""
        try:
            code = self.proc.wait(timeout_s)
        except subprocess.TimeoutExpired:
            raise AssertionError(
                f"announce.py did not exit within {timeout_s}s; output:\n{self.output()}"
            ) from None
        self.ended = time.monotonic()
        self._reader.join(5.0)
        return code

    def __enter__(self) -> AnnounceRun:
        return self

    def __exit__(self, *exc: object) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()  # announce.py's own SIGTERM path reaps a child that wrote no report
            try:
                self.proc.wait(REAP_S + MARGIN_S)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(30)
        for report in self.reports:
            with contextlib.suppress(OSError, ValueError, KeyError):
                kill_quietly(int(json.loads(report.read_text())["pid"]))


# ---- specs ---------------------------------------------------------------------------------------------


def web_spec(
    name: str = "web",
    *,
    argv: tuple[str, ...] = ("{python}", "-m", "http.server", "{port}"),
    check: str = "http:/",
    timeout_s: float = 60.0,
    **launch: Any,
) -> ServiceSpec:
    """A requirement whose recipe is ``argv`` (``launch`` fields: image, inputs, env, resources)."""
    return ServiceSpec(
        name,
        "http",
        check=check,
        ports=(40000, 40010),
        launch=Launch(argv=argv, **launch),
        timeout_s=timeout_s,
    )


# ---- plan parts pilots unpickle (the live file) --------------------------------------------------------


def mem_partitions(n: int, tag: str) -> tuple[Partition, ...]:
    return tuple(Partition(f"mem://{tag}/{i}", "", i, i + 1) for i in range(n))


def pair_concat(a: tuple[Any, ...], b: tuple[Any, ...]) -> tuple[Any, ...]:
    return a + b


def empty_tuple() -> tuple[Any, ...]:
    return ()


def _as_http(endpoint: str) -> str:
    scheme, _, rest = endpoint.partition("://")
    assert scheme in ("http", "tcp"), endpoint
    return f"http://{rest}"


@dataclass(frozen=True)
class TimedGet:
    """A plan process bound to ``service``: each task GETs ``/`` and ``/service.json`` on it and returns
    ``((time.time() at the GET, body of /, status of /service.json),)``."""

    service: str = "web"
    endpoint: str | None = None

    def bind_services(self, endpoints: Mapping[str, str]) -> TimedGet:
        endpoint = endpoints.get(self.service, self.endpoint)
        if endpoint is None:
            raise UnboundService(self.service)
        return replace(self, endpoint=endpoint)

    def __call__(self, partition: Partition, resources: object) -> tuple[tuple[float, str, int], ...]:
        assert self.endpoint is not None, "process called unbound"
        t = time.time()
        body = http_get(_as_http(self.endpoint) + "/")
        return ((t, body, http_status(_as_http(self.endpoint) + "/service.json")),)


@dataclass(frozen=True)
class GatedGet:
    """A plan process that polls ``gate`` (an HTTP URL) until it answers ``open``, then GETs ``/`` on its
    bound ``service`` and returns ``((time.time(), body),)``: the test decides when the task finishes."""

    gate: str
    service: str = "web"
    endpoint: str | None = None

    def bind_services(self, endpoints: Mapping[str, str]) -> GatedGet:
        endpoint = endpoints.get(self.service, self.endpoint)
        if endpoint is None:
            raise UnboundService(self.service)
        return replace(self, endpoint=endpoint)

    def __call__(self, partition: Partition, resources: object) -> tuple[tuple[float, str], ...]:
        assert self.endpoint is not None, "process called unbound"
        deadline = time.monotonic() + 300.0
        while time.monotonic() < deadline:
            with contextlib.suppress(OSError):
                if http_get(self.gate) == "open":
                    break
            time.sleep(0.2)
        return ((time.time(), http_get(_as_http(self.endpoint) + "/")),)


def service_plan(process: Any, n: int, tag: str, specs: tuple[ServiceSpec, ...]) -> Plan[Any]:
    tasks = tuple(Task(i, p) for i, p in enumerate(mem_partitions(n, tag)))
    return Plan(process=process, combine=pair_concat, empty=empty_tuple, tasks=tasks, services=specs)
