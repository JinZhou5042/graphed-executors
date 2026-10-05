"""Shared harness for the m66 frozen suite: the direct HTCondor backend (lanes/htcondor/plan.md §2-§3).

The m42 ``submit_backends.py`` helpers that the copied conformance bodies use are copied here (D1):
frozen suites never import each other across directories, and no second ``submit_backends`` basename
may exist. ``where()`` reads the backend-installed worker identity (the m46 idiom), because a pilot is
not a dask worker.

Everything a pilot unpickles is module-level here. Pilots import this module by name: ``LocalPilots``
gets this directory on its ``PYTHONPATH``, and the live-pool file ships this file in ``user_modules``.

The implementation under test is reached only through the ``*_api()`` accessors, inside test bodies,
so the suite collects before ``graphed_executors.htcondor_backend`` exists and each test then fails at
the accessor with an ImportError/AttributeError that names the missing module or symbol.
"""

from __future__ import annotations

import os
import sys
import threading
import time
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from graphed.core.execution import ExecContext, Partition, Plan, Task
from graphed.debug import SourceFrame, StageError

from graphed_executors.submit import current_env

HARNESS_DIR = str(Path(__file__).resolve().parent)
HARNESS_FILE = str(Path(__file__).resolve())
DATA_DIR = Path(HARNESS_DIR) / "data"
# never parents[3]: a pilot imports this file from a scratch dir that may be only one level deep
REPO_ROOT = (Path(HARNESS_DIR) / ".." / ".." / "..").resolve()

PILOT_WAIT_S = 120.0  # local pilots register in about a second; the bound turns a hang into a failure
RUN_TIMEOUT_S = 240.0

# ---- deferred accessors for the implementation under test ---------------------------------------


def htcondor_api() -> Any:
    """``graphed_executors.htcondor_backend``: HTCondorBackend, HTCondorRunner, htcondor_runner,
    CondorPilots, LocalPilots, PilotLauncher, SiteProfile, SITES, WorkerLost."""
    import graphed_executors.htcondor_backend as mod  # noqa: PLC0415  (deferred: module under test)

    return mod


def sites_api() -> Any:
    """``graphed_executors.htcondor_backend.sites``: schedd_weight, choose_schedd, counts_as_alive."""
    import graphed_executors.htcondor_backend.sites as mod  # noqa: PLC0415  (deferred: module under test)

    return mod


def launch_api() -> Any:
    """``graphed_executors.htcondor_backend.launch``: the lazy ``_htcondor()`` accessor lives here."""
    import graphed_executors.htcondor_backend.launch as mod  # noqa: PLC0415  (deferred: module under test)

    return mod


def server_api() -> Any:
    """``graphed_executors.htcondor_backend.server``: LEASE_S, read at call time."""
    import graphed_executors.htcondor_backend.server as mod  # noqa: PLC0415  (deferred: module under test)

    return mod


# ---- pilot pools ---------------------------------------------------------------------------------


class RecordingLauncher:
    """A ``PilotLauncher`` that records every ``start(url, secret, n)`` and delegates to ``inner``
    (none: no pilots, ``alive() == 0``). It is how a test learns the server's url and secret without
    naming the backend's private attributes."""

    def __init__(self, inner: Any = None) -> None:
        self.inner = inner
        self.starts: list[tuple[str, bytes, int]] = []

    def start(self, url: str, secret: bytes, n: int) -> None:
        self.starts.append((url, secret, n))
        if self.inner is not None:
            self.inner.start(url, secret, n)

    def alive(self) -> int:
        return int(self.inner.alive()) if self.inner is not None else 0

    def stop(self) -> None:
        if self.inner is not None:
            self.inner.stop()


def local_pilots() -> Any:
    return htcondor_api().LocalPilots(pythonpath=[HARNESS_DIR])


def local_backend(n: int, launcher: Any = None) -> Any:
    """``HTCondorBackend`` over ``n`` local pilots on 127.0.0.1, returned once all ``n`` said hello,
    so ``n_workers() == n`` cannot race pilot start-up."""
    backend = htcondor_api().HTCondorBackend(
        launcher if launcher is not None else local_pilots(), n, host="127.0.0.1"
    )
    try:
        backend.wait_for_pilots(n, timeout=PILOT_WAIT_S)
    except BaseException:
        backend.close()
        raise
    return backend


def make_runner(backend: Any, **kwargs: Any) -> Any:
    """``HTCondorRunner(backend, *, min_pilots=1, monitor=None, retries=3, max_in_flight=2)``."""
    return htcondor_api().HTCondorRunner(backend, **kwargs)


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
    """Poll a driver-side predicate within a bound; the assertion on the state comes after."""
    deadline = time.monotonic() + timeout_s
    while not predicate() and time.monotonic() < deadline:
        time.sleep(0.05)


# ---- where work ran ------------------------------------------------------------------------------


def where() -> tuple[int, str]:
    """(pid, worker identity) at the point of call, from the backend-installed worker env."""
    return os.getpid(), current_env().worker


def double(x: int) -> int:
    return 2 * x


def double_with_site(x: int) -> tuple[int, int, str]:
    """Doubles a resolved upstream value and reports where it ran."""
    pid, worker = where()
    return 2 * x, pid, worker


def payload_len(x: object) -> int:
    """The broadcast contract probe: the task fn must receive the raw bytes on the worker."""
    assert isinstance(x, (bytes, bytearray)), f"broadcast handle must resolve to raw bytes, got {type(x)}"
    return len(x)


def pilot_prefix() -> tuple[str, str]:
    """(sys.prefix, $_CONDOR_SCRATCH_DIR) on the pilot."""
    return sys.prefix, os.environ.get("_CONDOR_SCRATCH_DIR", "")


def harness_origin() -> tuple[str, str | None, str]:
    """(this module's file, $PYTHONPATH, $_CONDOR_SCRATCH_DIR) on the pilot."""
    return HARNESS_FILE, os.environ.get("PYTHONPATH"), os.environ.get("_CONDOR_SCRATCH_DIR", "")


# ---- the unpickle marker (auth witness) ----------------------------------------------------------


def touch_marker(path: str) -> str:
    Path(path).touch()
    return path


@dataclass(frozen=True)
class MarkerBomb:
    """Unpickling this object creates ``path``: the file's existence shows ``pickle.loads`` ran."""

    path: str

    def __reduce__(self) -> tuple[Any, tuple[str]]:
        return touch_marker, (self.path,)


# ---- scenario partitions + pure payloads (copied from m42) ---------------------------------------


def mem_partitions(n: int, tag: str) -> tuple[Partition, ...]:
    return tuple(Partition(f"mem://{tag}/{i}", "", i, i + 1) for i in range(n))


def part_index(uri: str) -> int:
    return int(uri.rsplit("/", 1)[1])


def leaf_text(partition: Partition) -> str:
    return f"[{partition.uri}:{partition.entry_start}-{partition.entry_stop}]"


def leaf_int(partition: Partition) -> int:
    return 7 * partition.entry_start + 3


def concat_process(partition: Partition, resources: object) -> str:
    return leaf_text(partition)


def staggered_concat_process(partition: Partition, resources: object) -> str:
    """Later keys complete first; the sleep is scenario construction, never an assertion."""
    time.sleep(0.01 * max(0, 17 - partition.entry_start))
    return leaf_text(partition)


def concat(a: str, b: str) -> str:
    return a + b


def empty_text() -> str:
    return ""


def concat_plan(n: int, tag: str, *, staggered: bool = False, reverse_tasks: bool = False) -> Plan[str]:
    """Associative, non-commutative concat: any grouping over key-ordered leaves matches
    ``SequentialRunner`` bit-for-bit; an arrival-ordered fold does not."""
    tasks = tuple(Task(i, p) for i, p in enumerate(mem_partitions(n, tag)))
    if reverse_tasks:
        tasks = tuple(reversed(tasks))
    process = staggered_concat_process if staggered else concat_process
    return Plan(process=process, combine=concat, empty=empty_text, tasks=tasks)


def expected_concat(n: int, tag: str) -> str:
    return "".join(leaf_text(p) for p in mem_partitions(n, tag))


# ---- provenance-carrying partials (copied from m42) ----------------------------------------------


@dataclass(frozen=True)
class Prov:
    payload: str
    leaves: frozenset[tuple[str, int, str]]
    combine_sites: frozenset[tuple[int, str]]
    moves: frozenset[tuple[str, str]]
    marks: frozenset[str]
    made_on: tuple[int, str]


def prov_process(partition: Partition, resources: object) -> Prov:
    pid, worker = where()
    return Prov(
        payload=leaf_text(partition),
        leaves=frozenset({(partition.uri, pid, worker)}),
        combine_sites=frozenset(),
        moves=frozenset(),
        marks=frozenset({uuid.uuid4().hex}),
        made_on=(pid, worker),
    )


def paced_prov_process(partition: Partition, resources: object) -> Prov:
    """``prov_process`` after a short sleep, so two pilots both pull leaves (scenario construction)."""
    time.sleep(0.2)
    return prov_process(partition, resources)


def prov_combine(a: Prov, b: Prov) -> Prov:
    pid, worker = where()
    return Prov(
        payload=a.payload + b.payload,
        leaves=a.leaves | b.leaves,
        combine_sites=a.combine_sites | b.combine_sites | {(pid, worker)},
        moves=a.moves | b.moves | {(a.made_on[1], worker), (b.made_on[1], worker)},
        marks=a.marks | b.marks | {uuid.uuid4().hex},
        made_on=(pid, worker),
    )


def prov_empty() -> Prov:
    return Prov("", frozenset(), frozenset(), frozenset(), frozenset(), where())


def prov_plan(n: int, tag: str, process: Callable[[Partition, object], Prov] = prov_process) -> Plan[Prov]:
    tasks = tuple(Task(i, p) for i, p in enumerate(mem_partitions(n, tag)))
    return Plan(process=process, combine=prov_combine, empty=prov_empty, tasks=tasks)


# ---- failure scenarios (copied from m42, plus PoisonUriProcess) ----------------------------------

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
    tasks = tuple(Task(i, p) for i, p in enumerate(mem_partitions(n, tag)))
    return Plan(process=raise_stage_error, combine=concat, empty=empty_text, tasks=tasks)


def exit_process(partition: Partition, resources: object) -> str:
    """Hard worker death (segfault/OOM stand-in): kills the pilot process, not just the task."""
    os._exit(1)


@dataclass(frozen=True)
class DieOnceProcess:
    """The first attempt at ``poison_uri`` writes its pid to ``marker_path`` and kills its pilot; the
    re-run (marker present) succeeds."""

    marker_path: str
    poison_uri: str

    def __call__(self, partition: Partition, resources: object) -> Prov:
        if partition.uri == self.poison_uri and not os.path.exists(self.marker_path):
            tmp = f"{self.marker_path}.{os.getpid()}"
            with open(tmp, "w") as f:
                f.write(str(os.getpid()))
            os.replace(tmp, self.marker_path)
            os._exit(1)
        return prov_process(partition, resources)


@dataclass(frozen=True)
class PoisonUriProcess:
    """``exit_process`` for ``poison_uri`` only, ``concat_process`` otherwise. Each poisoned attempt
    first creates ``attempts_dir/<pid>``, so the directory counts the pilots the uri killed."""

    poison_uri: str
    attempts_dir: str | None = None

    def __call__(self, partition: Partition, resources: object) -> str:
        if partition.uri == self.poison_uri:
            if self.attempts_dir is not None:
                Path(self.attempts_dir, str(os.getpid())).touch()
            return exit_process(partition, resources)
        return concat_process(partition, resources)


# ---- adaptive scenario (copied from m42) ---------------------------------------------------------


@dataclass(frozen=True)
class FoldPart:
    value: int
    uris: frozenset[str]


def fold_oracle(uris: frozenset[str]) -> int:
    return sum(7 * part_index(u) + 3 for u in uris)


def fold_process(partition: Partition, resources: object) -> FoldPart:
    return FoldPart(leaf_int(partition), frozenset({partition.uri}))


def fold_combine(a: FoldPart, b: FoldPart) -> FoldPart:
    return FoldPart(a.value + b.value, a.uris | b.uris)


def fold_empty() -> FoldPart:
    return FoldPart(0, frozenset())


class Feed:
    """Driver-side ``next_tasks``: hands out the given batches, then ``None``."""

    def __init__(self, batches: Sequence[Sequence[Task]]) -> None:
        self.batches = [list(b) for b in batches]
        self.calls: list[int] = []

    def __call__(self, ctx: ExecContext) -> list[Task] | None:
        self.calls.append(ctx.events_done)
        return self.batches.pop(0) if self.batches else None


# ---- RecordingMonitor (copied from m42) ----------------------------------------------------------


class RecordingMonitor:
    """A passive driver-side Monitor that appends every TaskEvent."""

    def __init__(self) -> None:
        self.events: list[Any] = []
        self.combines: list[int] = []
        self.profiles: list[tuple[str, bytes]] = []

    def on_task(self, event: Any) -> None:
        self.events.append(event)

    def on_profile(self, worker: str, payload: bytes) -> None:
        self.profiles.append((worker, payload))

    def on_combine(self, leaves_done: int) -> None:
        self.combines.append(leaves_done)

    def worker_profiler_factory(self) -> None:
        return None


def events_by_key(events: Sequence[Any]) -> dict[int, list[Any]]:
    out: dict[int, list[Any]] = {}
    for ev in events:
        out.setdefault(ev.key, []).append(ev)
    return out
