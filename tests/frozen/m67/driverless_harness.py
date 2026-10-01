"""Shared harness for the m67 frozen suite: driverless runs (lanes/htcondor/plan-services.md §2, D5, D6).

It copies the m66 ``htcondor_harness.py`` pieces the suite uses (frozen suites never import across
directories) and adds ``RecordingSchedd``/``FakeHTCondor``, a stand-in for the ``htcondor2`` module that
records every bindings call, and ``job_dir``, which lays out a job's scratch dir from a recorded submit
description the way the schedd's input transfer would.

Everything a pilot or the driver unpickles is module-level here, and the suite ships this file as a
``user_modules`` entry, so the job side imports it from its scratch dir by name.

The implementation under test is reached only through the ``*_api()`` accessors, inside test bodies,
so the suite collects before ``driver.py``/``driverless.py`` exist.
"""

from __future__ import annotations

import importlib
import json
import os
import shutil
import sys
import sysconfig
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from graphed.core.execution import Partition, Plan, Task
from graphed.debug import SourceFrame, StageError

if TYPE_CHECKING:
    import pytest

HARNESS_DIR = str(Path(__file__).resolve().parent)
HARNESS_FILE = str(Path(__file__).resolve())
DATA_DIR = Path(HARNESS_DIR) / "data"

RUN_TIMEOUT_S = 240.0

FAKE_POOL = "cm.m67.example:9618"
FAKE_SCHEDD = "schedd.m67.example"
FAKE_CLUSTER = 4242

# ---- deferred accessors for the implementation under test ---------------------------------------


def htcondor_api() -> Any:
    """``graphed_executors.htcondor_backend``: submit_driverless, RunHandle, SITES, SiteProfile, ..."""
    return importlib.import_module("graphed_executors.htcondor_backend")


def launch_api() -> Any:
    """``graphed_executors.htcondor_backend.launch``: every bindings call goes through its ``_htcondor()``."""
    return importlib.import_module("graphed_executors.htcondor_backend.launch")


def driver_api() -> Any:
    """``graphed_executors.htcondor_backend.driver``: ``main([dir])``."""
    return importlib.import_module("graphed_executors.htcondor_backend.driver")


DRIVER_MODULE = "graphed_executors.htcondor_backend.driver"

# ---- bounds (copied from m66) --------------------------------------------------------------------


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


def wait_for(predicate: Callable[[], bool], timeout_s: float = 30.0) -> None:
    """Poll a predicate within a bound; the assertion on the state comes after."""
    deadline = time.monotonic() + timeout_s
    while not predicate() and time.monotonic() < deadline:
        time.sleep(0.05)


# ---- plans (copied from m66, plus the job-site plan) ---------------------------------------------


def mem_partitions(n: int, tag: str) -> tuple[Partition, ...]:
    return tuple(Partition(f"mem://{tag}/{i}", "", i, i + 1) for i in range(n))


def leaf_text(partition: Partition) -> str:
    return f"[{partition.uri}:{partition.entry_start}-{partition.entry_stop}]"


def concat_process(partition: Partition, resources: object) -> str:
    return leaf_text(partition)


def concat(a: str, b: str) -> str:
    return a + b


def empty_text() -> str:
    return ""


def concat_plan(n: int, tag: str) -> Plan[str]:
    """Associative, non-commutative concat: any grouping over key-ordered leaves matches
    ``SequentialRunner`` bit-for-bit; an arrival-ordered fold does not."""
    tasks = tuple(Task(i, p) for i, p in enumerate(mem_partitions(n, tag)))
    return Plan(process=concat_process, combine=concat, empty=empty_text, tasks=tasks)


def expected_concat(n: int, tag: str) -> str:
    return "".join(leaf_text(p) for p in mem_partitions(n, tag))


def job_cluster() -> str:
    """``ClusterId`` from the file ``$_CONDOR_JOB_AD`` names; empty outside a job."""
    path = os.environ.get("_CONDOR_JOB_AD")
    if not path or not os.path.isfile(path):
        return ""
    for line in Path(path).read_text().splitlines():
        name, _, value = line.partition("=")
        if name.strip() == "ClusterId":
            return value.strip()
    return ""


@dataclass(frozen=True)
class JobLeaf:
    """Concat text plus, per leaf, (pid, job ClusterId, the url the pilot was started with)."""

    text: str
    sites: frozenset[tuple[int, str, str]]


def job_process(partition: Partition, resources: object) -> JobLeaf:
    """The leaf text and where it ran; the sleep lets every pilot pull a leaf (scenario construction)."""
    time.sleep(0.2)
    url = sys.argv[1] if len(sys.argv) > 1 else ""  # a pilot runs as ``-m ...pilot <url> <secret>``
    return JobLeaf(leaf_text(partition), frozenset({(os.getpid(), job_cluster(), url)}))


def job_combine(a: JobLeaf, b: JobLeaf) -> JobLeaf:
    return JobLeaf(a.text + b.text, a.sites | b.sites)


def job_empty() -> JobLeaf:
    return JobLeaf("", frozenset())


def job_plan(n: int, tag: str) -> Plan[JobLeaf]:
    tasks = tuple(Task(i, p) for i, p in enumerate(mem_partitions(n, tag)))
    return Plan(process=job_process, combine=job_combine, empty=job_empty, tasks=tasks)


USER_FRAME = SourceFrame(filename="user_analysis.py", lineno=42, function="my_cut", source="pt > 30")


def make_stage_error() -> StageError:
    return StageError(
        op="mul",
        frames=(USER_FRAME,),
        input_forms=("float64[]",),
        partition="mem://boom/0:0-1",
        cause_type="ValueError",
        cause_message="negative pt",
        opt_level=2,
    )


def raise_stage_error(partition: Partition, resources: object) -> str:
    raise make_stage_error()


def stage_error_plan(n: int, tag: str) -> Plan[str]:
    """A plan error: every leaf raises the same user-code ``StageError`` on its pilot."""
    tasks = tuple(Task(i, p) for i, p in enumerate(mem_partitions(n, tag)))
    return Plan(process=raise_stage_error, combine=concat, empty=empty_text, tasks=tasks)


def assert_intact_stage_error(err: BaseException) -> None:
    assert type(err) is StageError, repr(err)
    assert err.op == "mul"
    assert err.user_frame is not None and err.user_frame.filename == "user_analysis.py"
    assert err.user_frame.lineno == 42
    assert err.cause_message == "negative pt"


# ---- a shippable venv (copied from m66 test_htcondor_sites) --------------------------------------


def fake_venv(root: Path) -> Path:
    """A directory shaped like a non-editable venv with one dist-info."""
    root.mkdir()
    (root / "pyvenv.cfg").write_text("home = /usr/bin\ninclude-system-site-packages = false\n")
    purelib = Path(
        sysconfig.get_path("purelib", scheme="venv", vars={"base": str(root), "platbase": str(root)})
    )
    dist = purelib / "probedist-0.1.dist-info"
    dist.mkdir(parents=True)
    (dist / "direct_url.json").write_text(json.dumps({"url": "file:///src/probedist", "dir_info": {}}))
    return root


# ---- recorded bindings ---------------------------------------------------------------------------


class SubmitResult:
    def __init__(self, cluster: int) -> None:
        self._cluster = cluster

    def cluster(self) -> int:
        return self._cluster


class RecordingSchedd:
    """A stand-in ``htcondor2.Schedd`` that appends every call to ``log``.

    ``queue`` is the successive answers of ``query`` (the last one repeats), ``history`` the answer of
    ``history``. ``retrieve`` writes ``retrieved`` (``{file name: bytes}``) into ``retrieve_to``, as the
    schedd's output transfer of a spooled job would."""

    def __init__(
        self,
        queue: list[list[dict[str, Any]]] | None = None,
        history: list[dict[str, Any]] | None = None,
        retrieved: dict[str, bytes] | None = None,
        retrieve_to: Path | None = None,
    ) -> None:
        self.log: list[tuple[Any, ...]] = []
        self.queue = [list(answer) for answer in (queue or [[]])]
        self.history_ads = list(history or [])
        self.retrieved = dict(retrieved or {})
        self.retrieve_to = retrieve_to

    def query(self, constraint: str = "true", projection: Any = None, *args: Any, **kwargs: Any) -> list[Any]:
        self.log.append(("query", str(constraint), tuple(projection or ())))
        return self.answer_query()

    def answer_query(self) -> list[Any]:
        return self.queue.pop(0) if len(self.queue) > 1 else list(self.queue[0])

    def history(self, constraint: Any = None, projection: Any = None, *args: Any, **kwargs: Any) -> list[Any]:
        self.log.append(("history", str(constraint), tuple(projection or ())))
        return list(self.history_ads)

    def submit(self, description: Any, count: int = 0, spool: bool = False, **kwargs: Any) -> SubmitResult:
        self.log.append(("submit", dict(description), count, spool))
        return SubmitResult(FAKE_CLUSTER)

    def spool(self, result: Any, *args: Any, **kwargs: Any) -> None:
        self.log.append(("spool",))

    def retrieve(self, constraint: Any = None, *args: Any, **kwargs: Any) -> None:
        self.log.append(("retrieve", str(constraint)))
        if self.retrieve_to is not None:
            for name, data in self.retrieved.items():
                (self.retrieve_to / name).write_bytes(data)

    def act(self, action: Any, constraint: Any = None, *args: Any, **kwargs: Any) -> None:
        self.log.append(("act", str(action), str(constraint)))


class PilotSchedd(RecordingSchedd):
    """A ``RecordingSchedd`` whose ``submit`` starts the submitted pilots as local subprocesses (the
    m66 ``LocalPilots``), reading the url from ``arguments`` and the secret from ``initialdir``."""

    def __init__(self) -> None:
        super().__init__()
        self.pilots: Any = None

    def submit(self, description: Any, count: int = 0, spool: bool = False, **kwargs: Any) -> SubmitResult:
        result = super().submit(description, count, spool, **kwargs)
        desc = dict(description)
        url, secret_name = str(desc["arguments"]).split()
        secret = bytes.fromhex(Path(desc["initialdir"], secret_name).read_text().strip())
        self.pilots = htcondor_api().LocalPilots(pythonpath=[HARNESS_DIR])
        self.pilots.start(url, secret, max(count, 1))
        return result

    def answer_query(self) -> list[Any]:
        alive = self.pilots.alive() if self.pilots is not None else 0
        return [{"JobStatus": 2}] * alive

    def act(self, action: Any, constraint: Any = None, *args: Any, **kwargs: Any) -> None:
        super().act(action, constraint, *args, **kwargs)
        if self.pilots is not None:
            self.pilots.stop()
            self.pilots = None


class _Collector:
    def __init__(self, fake: FakeHTCondor, pool: str | None) -> None:
        self.fake = fake
        self.pool = pool

    def locate(self, daemon_type: Any, name: str | None = None, *args: Any, **kwargs: Any) -> dict[str, Any]:
        self.fake.log.append(("locate", self.pool, str(daemon_type), name))
        return {"Name": name, "MyAddress": "<127.0.0.1:9618>", "located": True}

    def query(
        self, ad_type: Any = None, constraint: Any = None, projection: Any = None, **kwargs: Any
    ) -> Any:
        self.fake.log.append(("collector-query", self.pool))
        return list(self.fake.schedd_ads)


class _Credd:
    """A credd that holds the user's Kerberos credential, so a submit stores none."""

    def query_user_cred(self, *args: Any, **kwargs: Any) -> int:
        return 1790802437


class FakeHTCondor:
    """What ``launch._htcondor()`` returns under the recorder: ``param``, ``Collector``, ``Schedd``,
    ``Submit`` and the enums the backend names. ``Schedd()`` with no location is logged as
    ``("Schedd", None)``; with a located ad as ``("Schedd", <its Name>)``."""

    class DaemonType:
        Schedd = "Schedd"

    class AdType:
        Schedd = "Schedd"

    class JobAction:
        Remove = "Remove"

    def __init__(self, schedd: RecordingSchedd) -> None:
        self.schedd = schedd
        self.log = schedd.log
        self.param = {
            "COLLECTOR_HOST": FAKE_POOL,
            "SCHEDD_HOST": FAKE_SCHEDD,
            "FERMIHTC_REMOTE_POOL": FAKE_POOL,
        }
        self.schedd_ads: list[dict[str, Any]] = [
            {
                "Name": FAKE_SCHEDD,
                "RecentDaemonCoreDutyCycle": 0.1,
                "ShadowsRunning": 1,
                "MaxJobsRunning": 10,
                "TotalIdleJobs": 1,
            }
        ]

    class CredType:
        Kerberos = "Kerberos"

    def Credd(self, *args: Any, **kwargs: Any) -> _Credd:
        return _Credd()

    def Collector(self, pool: str | None = None, *args: Any, **kwargs: Any) -> _Collector:
        self.log.append(("Collector", pool))
        return _Collector(self, pool)

    def Schedd(self, location: Any = None, *args: Any, **kwargs: Any) -> RecordingSchedd:
        self.log.append(("Schedd", None if location is None else location["Name"]))
        return self.schedd

    def Submit(self, description: Any = None, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return dict(description or {})


def record_bindings(monkeypatch: pytest.MonkeyPatch, schedd: RecordingSchedd) -> FakeHTCondor:
    """Patch ``launch._htcondor`` to log ``("_htcondor",)`` and return a ``FakeHTCondor`` over ``schedd``."""
    fake = FakeHTCondor(schedd)

    def _htcondor() -> FakeHTCondor:
        fake.log.append(("_htcondor",))
        return fake

    monkeypatch.setattr(launch_api(), "_htcondor", _htcondor)
    return fake


def submits(log: list[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
    return [entry for entry in log if entry[0] == "submit"]


def schedd_locations(log: list[tuple[Any, ...]]) -> list[Any]:
    """The location of every ``Schedd(...)`` call: ``None`` is a bare ``Schedd()``."""
    return [entry[1] for entry in log if entry[0] == "Schedd"]


# ---- the job side --------------------------------------------------------------------------------


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


def write_machine_ad(path: Path, host: str = "127.0.0.1") -> Path:
    """A machine ad file in the classad text form of ``$_CONDOR_MACHINE_AD``."""
    path.write_text(f'Machine = "{host}"\nName = "slot1@{host}"\nCpus = 2\n')
    return path
