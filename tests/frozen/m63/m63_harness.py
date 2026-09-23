"""Shared m63 harness: spawn-safe plan callables and the scenario checks every executor runs.

Everything a worker may unpickle is module level. The checks take a built executor (or a factory)
and use only the public contract: ``submit``, ``run``, ``max_in_flight``, ``monitor``, ``close``.
"""

from __future__ import annotations

import os
import struct
import threading
import time
from collections import Counter
from collections.abc import Callable, Iterator
from concurrent.futures import Future
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from functools import reduce
from typing import Any

import pytest
from graphed.core import Partition, Plan, Task, TaskEvent, TaskPhase
from graphed.core.execution import ExecContext, ExecResult, StopCondition, StopReason
from graphed.debug import SourceFrame, StageError

HARNESS_DIR = os.path.dirname(os.path.abspath(__file__))
GATE_TIMEOUT_S = 60.0
OVERLAP_TIMEOUT_S = 10.0
BLOCKED_JOIN_S = 1.0
N_LEAVES = 6

# ---- plan callables ------------------------------------------------------------------------------


def leaf_value(plan_id: int, k: int) -> float:
    # a huge first leaf (which encodes the plan) makes float addition order-sensitive
    return 1e16 + 4 * plan_id if k == 0 else 1.0 + k * 1e-3


def add(a: float, b: float) -> float:
    return a + b


def zero() -> float:
    return 0.0


def fbytes(x: float) -> bytes:
    return struct.pack("<d", x)


@dataclass(frozen=True)
class FloatLeaf:
    plan_id: int

    def __call__(self, partition: Partition, resources: object) -> float:
        return leaf_value(self.plan_id, partition.entry_start)


_INTERVALS: list[tuple[int, float, float]] = []
_INTERVALS_LOCK = threading.Lock()


def intervals() -> list[tuple[int, float, float]]:
    with _INTERVALS_LOCK:
        return list(_INTERVALS)


def clear_intervals() -> None:
    with _INTERVALS_LOCK:
        _INTERVALS.clear()


@dataclass(frozen=True)
class GatedLeaf:
    """Blocks until ``<partition.uri>/open`` exists, then returns its value; records its interval."""

    plan_id: int

    def __call__(self, partition: Partition, resources: object) -> float:
        start = time.monotonic()
        gate = os.path.join(partition.uri, "open")
        while not os.path.exists(gate):
            if time.monotonic() - start > GATE_TIMEOUT_S:
                raise TimeoutError(f"gate {gate} never opened")
            time.sleep(0.005)
        with _INTERVALS_LOCK:
            _INTERVALS.append((self.plan_id, start, time.monotonic()))
        return leaf_value(self.plan_id, partition.entry_start)


def fixed_plan(plan_id: int, n: int = N_LEAVES, *, gate_dir: str | None = None) -> Plan[float]:
    uri = gate_dir if gate_dir is not None else f"mem://m63/{plan_id}"
    process = FloatLeaf(plan_id) if gate_dir is None else GatedLeaf(plan_id)
    tasks = tuple(Task(k, Partition(uri, "t", k, k + 1)) for k in range(n))
    return Plan(process=process, combine=add, empty=zero, tasks=tasks)


@dataclass(frozen=True)
class SleepyLeaf:
    plan_id: int

    def __call__(self, partition: Partition, resources: object) -> float:
        time.sleep(0.01)
        return leaf_value(self.plan_id, partition.entry_start)


def sleepy_plan(plan_id: int) -> Plan[float]:
    """Plan ``plan_id`` has ``9 + plan_id`` slow leaves, so concurrent plans differ in shape and value."""
    tasks = tuple(Task(k, Partition(f"mem://m63/sleepy/{plan_id}", "t", k, k + 1)) for k in range(9 + plan_id))
    return Plan(process=SleepyLeaf(plan_id), combine=add, empty=zero, tasks=tasks)


def open_gate(gate_dir: str) -> None:
    with open(os.path.join(gate_dir, "open"), "w"):
        pass


def new_gate(tmp: str, name: str) -> str:
    d = os.path.join(tmp, name)
    os.makedirs(d)
    return d


# adaptive: one explicit 10-entry partition per next_tasks call, keyed by ctx.n_done
ADAPTIVE_TOTAL = 40
ADAPTIVE_TARGET = 150


def adaptive_leaf(partition: Partition, resources: object) -> float:
    return 1.0 / (partition.entry_start + 3)


@dataclass(frozen=True)
class OnePerCall:
    n_total: int

    def __call__(self, ctx: ExecContext) -> list[Task] | None:
        i = ctx.n_done
        if i >= self.n_total:
            return None
        return [Task(i, Partition("mem://m63/adaptive", "t", 10 * i, 10 * i + 10))]


def adaptive_plan() -> Plan[float]:
    return Plan(
        process=adaptive_leaf,
        combine=add,
        empty=zero,
        tasks=(),
        next_tasks=OnePerCall(ADAPTIVE_TOTAL),
        stop=StopCondition(target_events=ADAPTIVE_TARGET),
    )


USER_FRAME = SourceFrame(filename="user_analysis_m63.py", lineno=63, function="my_cut", source="pt > 63")


def make_stage_error() -> StageError:
    return StageError(
        op="mul",
        frames=(USER_FRAME,),
        input_forms=("float64[]",),
        partition="mem://m63/boom:3-4",
        cause_type="ValueError",
        cause_message="negative pt",
        opt_level=2,
    )


def boom_on_three(partition: Partition, resources: object) -> float:
    if partition.entry_start == 3:
        raise make_stage_error()
    return leaf_value(9, partition.entry_start)


def failing_plan() -> Plan[float]:
    tasks = tuple(Task(k, Partition("mem://m63/boom", "t", k, k + 1)) for k in range(N_LEAVES))
    return Plan(process=boom_on_three, combine=add, empty=zero, tasks=tasks)


def flaky_once(partition: Partition, resources: object) -> float:
    """Leaf 1 fails on its first attempt only; the marker path is the partition uri."""
    if partition.entry_start == 1 and not os.path.exists(partition.uri):
        with open(partition.uri, "w"):
            pass
        raise ValueError("m63 flaky leaf, first attempt")
    return leaf_value(5, partition.entry_start)


def flaky_plan(marker: str) -> Plan[float]:
    tasks = tuple(Task(k, Partition(marker, "t", k, k + 1)) for k in range(N_LEAVES))
    return Plan(process=flaky_once, combine=add, empty=zero, tasks=tasks)


def overlap_a(partition: Partition, resources: object) -> float:
    """Marks its start, then returns 1.0 iff plan B's leaf opens the gate while this one waits."""
    d = partition.uri
    with open(os.path.join(d, "a_started"), "w"):
        pass
    start = time.monotonic()
    while time.monotonic() - start < OVERLAP_TIMEOUT_S:
        if os.path.exists(os.path.join(d, "gate")):
            return 1.0
        time.sleep(0.01)
    return 0.0


def overlap_b(partition: Partition, resources: object) -> float:
    with open(os.path.join(partition.uri, "gate"), "w"):
        pass
    return 0.0


def overlap_plan(process: Callable[[Partition, object], float], d: str) -> Plan[float]:
    return Plan(process=process, combine=add, empty=zero, tasks=(Task(0, Partition(d, "t", 0, 1)),))


# ---- driver-side helpers -------------------------------------------------------------------------


class RecordingMonitor:
    def __init__(self) -> None:
        self.events: list[TaskEvent] = []
        self._lock = threading.Lock()

    def on_task(self, event: TaskEvent) -> None:
        with self._lock:
            self.events.append(event)

    def on_profile(self, worker: str, payload: bytes) -> None:
        pass

    def on_combine(self, leaves_done: int) -> None:
        pass

    def worker_profiler_factory(self) -> None:
        return None

    def triples(self) -> Counter[tuple[TaskPhase, int, str]]:
        with self._lock:
            return Counter((e.phase, e.key, e.partition) for e in self.events)


def wait_for(predicate: Callable[[], bool], timeout_s: float = 30.0) -> None:
    deadline = time.monotonic() + timeout_s
    while not predicate() and time.monotonic() < deadline:
        time.sleep(0.05)


class Call:
    """Runs ``fn`` on a daemon thread so a call that blocks is observed by a join, never a hang."""

    def __init__(self, fn: Callable[[], Any]) -> None:
        self.value: Any = None
        self.error: BaseException | None = None
        self.thread = threading.Thread(target=self._run, args=(fn,), daemon=True)
        self.thread.start()

    def _run(self, fn: Callable[[], Any]) -> None:
        try:
            self.value = fn()
        except BaseException as e:
            self.error = e

    def returned_within(self, timeout_s: float) -> bool:
        self.thread.join(timeout_s)
        return not self.thread.is_alive()

    def get(self, timeout_s: float = GATE_TIMEOUT_S) -> Any:
        assert self.returned_within(timeout_s), "call did not return"
        if self.error is not None:
            raise self.error
        return self.value


def fingerprint(r: ExecResult[float]) -> tuple[bytes, int, int, StopReason | None]:
    return fbytes(r.value), r.n_partitions, r.n_combines, r.stopped


@contextmanager
def opened_on_exit(*gates: str) -> Iterator[None]:
    """Opens every gate on the way out so a failed assertion never leaves workers blocked."""
    try:
        yield
    finally:
        for g in gates:
            with suppress(FileExistsError, FileNotFoundError):
                open_gate(g)


# ---- scenario checks -----------------------------------------------------------------------------


def check_max_in_flight_ctor(make: Callable[..., Any]) -> None:
    ex = make()
    try:
        assert ex.max_in_flight == 2
    finally:
        ex.close()
    ex = make(max_in_flight=5)
    try:
        assert ex.max_in_flight == 5
    finally:
        ex.close()
    for bad in (0, -1):
        with pytest.raises(ValueError, match="max_in_flight must be at least 1"):
            make(max_in_flight=bad)


def check_fixed_equality(ex: Any) -> None:
    plan = fixed_plan(1)
    leaves = [leaf_value(1, k) for k in range(N_LEAVES)]
    # the fixture discriminates reduction order
    assert fbytes(reduce(add, leaves)) != fbytes(reduce(add, reversed(leaves)))
    ran = ex.run(plan)
    fut = ex.submit(plan)
    assert isinstance(fut, Future)
    got = fut.result(timeout=GATE_TIMEOUT_S)
    assert isinstance(got, ExecResult)
    assert fingerprint(got) == fingerprint(ran)
    again = [ex.submit(plan) for _ in range(3)]
    assert {fingerprint(f.result(timeout=GATE_TIMEOUT_S)) for f in again} == {fingerprint(ran)}


def check_adaptive_equality(ex: Any) -> None:
    plan = adaptive_plan()
    ran = ex.run(plan)
    assert ran.stopped is StopReason.TARGET_EVENTS
    assert ran.n_partitions < ADAPTIVE_TOTAL
    got = [ex.submit(plan).result(timeout=GATE_TIMEOUT_S) for _ in range(2)]
    assert all(g.stopped is StopReason.TARGET_EVENTS for g in got)
    assert {fingerprint(g) for g in got} == {fingerprint(ran)}


def check_asynchronous(ex: Any, tmp: str) -> None:
    gate = new_gate(tmp, "async")
    with opened_on_exit(gate):
        call = Call(lambda: ex.submit(fixed_plan(2, gate_dir=gate)))
        assert call.returned_within(GATE_TIMEOUT_S), "submit blocked on a gated plan"
        fut = call.get()
        assert not fut.done()
        open_gate(gate)
        assert fbytes(fut.result(timeout=GATE_TIMEOUT_S).value) == fbytes(ex.run(fixed_plan(2)).value)


def check_bound(ex: Any, tmp: str) -> None:
    """``ex`` has ``max_in_flight=2``."""
    g1, g2, g3 = (new_gate(tmp, f"bound{i}") for i in range(3))
    with opened_on_exit(g1, g2, g3):
        f1 = Call(lambda: ex.submit(fixed_plan(1, gate_dir=g1))).get()
        f2 = Call(lambda: ex.submit(fixed_plan(2, gate_dir=g2))).get()
        third = Call(lambda: ex.submit(fixed_plan(3, gate_dir=g3)))
        assert not third.returned_within(BLOCKED_JOIN_S), "a third submit did not wait for a slot"
        open_gate(g1)
        assert third.returned_within(GATE_TIMEOUT_S)
        f3 = third.get()
        open_gate(g2)
        open_gate(g3)
        values = [fbytes(f.result(timeout=GATE_TIMEOUT_S).value) for f in (f1, f2, f3)]
    assert values == [fbytes(ex.run(fixed_plan(i)).value) for i in (1, 2, 3)]


def check_cancel_frees_slot(ex: Any, tmp: str) -> None:
    """``ex`` has ``max_in_flight=2``: one plan runs, one queues; cancelling the queued one frees its slot."""
    g1, g2, g3 = (new_gate(tmp, f"cancel{i}") for i in range(3))
    with opened_on_exit(g1, g2, g3):
        f1 = Call(lambda: ex.submit(fixed_plan(1, gate_dir=g1))).get()
        f2 = Call(lambda: ex.submit(fixed_plan(2, gate_dir=g2))).get()
        assert f2.cancel() is True
        after = Call(lambda: ex.submit(fixed_plan(3, gate_dir=g3)))
        assert after.returned_within(BLOCKED_JOIN_S * 5), "the cancelled future kept its slot"
        f3 = after.get()
        open_gate(g1)
        open_gate(g3)
        v1 = f1.result(timeout=GATE_TIMEOUT_S).value
        v3 = f3.result(timeout=GATE_TIMEOUT_S).value
    assert f2.cancelled()
    assert fbytes(v1) == fbytes(ex.run(fixed_plan(1)).value)
    assert fbytes(v3) == fbytes(ex.run(fixed_plan(3)).value)


def check_order(ex: Any, tmp: str, *, in_process: bool) -> None:
    """A long (gated) plan submitted first completes before a short one submitted after it; in
    process, no leaf of the second plan starts before every leaf of the first has finished."""
    long_gate, short_gate = new_gate(tmp, "order_long"), new_gate(tmp, "order_short")
    open_gate(short_gate)
    clear_intervals()
    done: list[int] = []
    lock = threading.Lock()

    def record(plan_id: int) -> Callable[[Future[Any]], None]:
        def cb(_: Future[Any]) -> None:
            with lock:
                done.append(plan_id)

        return cb

    with opened_on_exit(long_gate):
        # one leaf leaves a worker free, so a plan running beside it would finish first
        long_fut = Call(lambda: ex.submit(fixed_plan(10, n=1, gate_dir=long_gate))).get()
        long_fut.add_done_callback(record(10))
        short_fut = Call(lambda: ex.submit(fixed_plan(11, gate_dir=short_gate))).get()
        short_fut.add_done_callback(record(11))
        with suppress(TimeoutError):
            short_fut.result(timeout=BLOCKED_JOIN_S)
        open_gate(long_gate)
        long_fut.result(timeout=GATE_TIMEOUT_S)
        short_fut.result(timeout=GATE_TIMEOUT_S)
    wait_for(lambda: len(done) == 2)
    assert done == [10, 11]
    if in_process:
        ivl = intervals()
        first = [e for pid, _, e in ivl if pid == 10]
        second = [s for pid, s, _ in ivl if pid == 11]
        assert (len(first), len(second)) == (1, N_LEAVES)
        assert max(first) <= min(second), "two plans' process calls overlapped"


def check_errors(ex: Any) -> None:
    with pytest.raises(StageError) as via_run:
        ex.run(failing_plan())
    bad = ex.submit(failing_plan())
    good = ex.submit(fixed_plan(4))
    with pytest.raises(StageError) as via_submit:
        bad.result(timeout=GATE_TIMEOUT_S)
    assert type(via_submit.value) is type(via_run.value)
    assert str(via_submit.value) == str(via_run.value)
    assert via_submit.value.__dict__ == via_run.value.__dict__
    assert [(f.filename, f.lineno) for f in via_submit.value.frames] == [("user_analysis_m63.py", 63)]
    assert fbytes(good.result(timeout=GATE_TIMEOUT_S).value) == fbytes(ex.run(fixed_plan(4)).value)


def _leg(make: Callable[..., Any], monitor: RecordingMonitor, via_submit: bool) -> None:
    ex = make(monitor=monitor)
    try:
        if via_submit:
            ex.submit(fixed_plan(6)).result(timeout=GATE_TIMEOUT_S)
        else:
            ex.run(fixed_plan(6))
    finally:
        ex.close()


def check_monitor_parity(make: Callable[..., Any], *, exact: bool) -> None:
    """``make(monitor=m)`` builds a fresh executor per leg (a kept pool's collector would leak
    one leg's trailing events into the next)."""
    n = N_LEAVES
    via_run, via_submit = RecordingMonitor(), RecordingMonitor()
    _leg(make, via_run, via_submit=False)
    _leg(make, via_submit, via_submit=True)
    if exact:
        for rec in (via_run, via_submit):
            wait_for(lambda rec=rec: sum(rec.triples().values()) >= 3 * n)
        time.sleep(0.2)  # a late duplicate would land here
        run_t, sub_t = via_run.triples(), via_submit.triples()
        assert run_t == sub_t
        assert Counter(p for p, _, _ in sub_t.elements()) == {
            TaskPhase.SUBMITTED: n,
            TaskPhase.STARTED: n,
            TaskPhase.FINISHED: n,
        }
        assert {k for _, k, _ in sub_t} == set(range(n))
        return
    submitted = []
    for rec in (via_run, via_submit):
        t = rec.triples()
        submitted.append(Counter({x: c for x, c in t.items() if x[0] is TaskPhase.SUBMITTED}))
        assert sorted(k for p, k, _ in t.elements() if p is TaskPhase.SUBMITTED) == list(range(n))
        for phase in (TaskPhase.STARTED, TaskPhase.FINISHED):
            keys = [k for p, k, _ in t.elements() if p is phase]
            assert len(keys) == len(set(keys)) and set(keys) <= set(range(n))
    assert submitted[0] == submitted[1]


def _submitted(rec: RecordingMonitor) -> int:
    return sum(1 for p, _, _ in rec.triples().elements() if p is TaskPhase.SUBMITTED)


def check_monitor_attach(ex: Any) -> None:
    """Assigning ``ex.monitor`` after construction reaches the next plan, for run and for submit."""
    first, second = RecordingMonitor(), RecordingMonitor()
    ex.monitor = first
    ex.run(fixed_plan(7))
    wait_for(lambda: _submitted(first) >= N_LEAVES)
    assert _submitted(first) == N_LEAVES
    ex.monitor = second
    ex.submit(fixed_plan(7)).result(timeout=GATE_TIMEOUT_S)
    wait_for(lambda: _submitted(second) >= N_LEAVES)
    assert _submitted(second) == N_LEAVES
    assert _submitted(first) == N_LEAVES


def check_close_drains(ex: Any, tmp: str) -> None:
    ref = [fbytes(ex.run(fixed_plan(20 + i)).value) for i in range(2)]
    gate = new_gate(tmp, "drain")
    with opened_on_exit(gate):
        futs = [Call(lambda i=i: ex.submit(fixed_plan(20 + i, gate_dir=gate))).get() for i in range(2)]
        closing = Call(ex.close)
        assert not closing.returned_within(BLOCKED_JOIN_S), "close returned while a plan was running"
        open_gate(gate)
        closing.get()
    assert all(f.done() and not f.cancelled() for f in futs)
    assert [fbytes(f.result(timeout=0).value) for f in futs] == ref
