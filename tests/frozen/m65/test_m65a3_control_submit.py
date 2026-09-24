"""m65 A3: ``SubmitRunner(ThreadBackend(2))`` honours a ``RunControl`` per A-2, on the fixed and adaptive paths."""

from __future__ import annotations

import dataclasses
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import m65a3_bodies as b
import m65a_probe as mp
import pytest
from graphed.core import Partition, RunControl, RunState, StopReason, TaskPhase

from graphed_executors.submit import SubmitFuture, SubmitRunner, ThreadBackend

LEGS = {"fixed": False, "adaptive": True}


def _leg(name: str) -> b.Leg:
    def factory(monitor: Any = None, **extra: Any) -> SubmitRunner:
        return SubmitRunner(ThreadBackend(2), monitor=monitor, **extra)

    return b.Leg(factory, adaptive=LEGS[name])


@pytest.mark.parametrize("leg", LEGS)
def test_unused_control_is_bit_identical(leg: str) -> None:
    b.unused_control_is_bit_identical(_leg(leg))


@pytest.mark.parametrize("leg", LEGS)
def test_pause_stops_dispatch_then_resume_completes(leg: str) -> None:
    b.pause_stops_dispatch_then_resume_completes(_leg(leg))


@pytest.mark.parametrize("leg", LEGS)
def test_paused_at_entry_holds_the_run(leg: str) -> None:
    b.paused_at_entry_holds_the_run(_leg(leg))


@pytest.mark.parametrize("leg", LEGS)
def test_cancel_drains_and_folds_exactly_the_completed_tasks(leg: str) -> None:
    b.cancel_drains_and_folds_exactly_the_completed_tasks(_leg(leg))


@pytest.mark.parametrize("leg", LEGS)
def test_failure_during_cancel_drain_raises(leg: str, tmp_path: Path) -> None:
    b.failure_during_cancel_drain_raises(_leg(leg), tmp_path)


@pytest.mark.parametrize("leg", LEGS)
def test_cancel_no_check_saw_is_reset(leg: str) -> None:
    b.cancel_no_check_saw_is_reset(_leg(leg), f"a3-15c-{leg}", hub=not LEGS[leg])


@pytest.mark.parametrize("empty_plan", [False, True], ids=["40-tasks", "no-tasks"])
@pytest.mark.parametrize("leg", LEGS)
def test_cancelled_control_does_no_work(leg: str, empty_plan: bool) -> None:
    b.cancelled_control_does_no_work(_leg(leg), empty_plan)


def test_stop_condition_does_not_end_a_cancel_drain(tmp_path: Path) -> None:
    b.stop_condition_does_not_end_a_cancel_drain(_leg("adaptive"), tmp_path)


def test_submit_runner_control_attribute() -> None:
    bare = SubmitRunner(ThreadBackend())
    assert bare.monitor is None and bare.control is None
    by_kwarg = RunControl()
    kwargs: dict[str, Any] = {"control": by_kwarg}
    ex = SubmitRunner(ThreadBackend(), **kwargs)
    assert ex.control is by_kwarg
    by_kwarg.cancel()
    assert ex.run(mp.make_plan(mp.Probe())).stopped is StopReason.CANCELLED
    assert by_kwarg.state is RunState.RUNNING

    ex = SubmitRunner(ThreadBackend())
    ex.control = by_attribute = RunControl()
    by_attribute.cancel()
    assert ex.run(mp.make_plan(mp.Probe())).stopped is StopReason.CANCELLED
    assert by_attribute.state is RunState.RUNNING


class _Slots(ThreadBackend):
    """A ``ThreadBackend(8)`` whose ``task_slots()`` answers ``reads(i)`` on its ``i``-th call."""

    def __init__(self, reads: Callable[[int], int]) -> None:
        super().__init__(8)
        self._reads = reads
        self._calls = 0
        self._lock = threading.Lock()

    def task_slots(self) -> int:
        with self._lock:
            i, self._calls = self._calls, self._calls + 1
        return self._reads(i)


@pytest.mark.parametrize("leg", LEGS)
def test_window_floors_at_one_and_widens_on_reread(leg: str) -> None:
    rec = mp.Recorder()
    ex = SubmitRunner(_Slots(lambda i: 0 if i == 0 else 4), monitor=rec)
    ex.control = RunControl()
    plan = mp.make_plan(mp.Probe(n=8, sleep_s=0.3), adaptive=LEGS[leg])
    res = mp.Background(lambda: ex.run(plan)).result(30)
    assert res.value == (1,) * 8
    assert b.started_before_first_finished(rec) == 4
    if plan.next_tasks is not None:
        assert plan.next_tasks.calls == res.n_partitions + 1


@dataclasses.dataclass(frozen=True)
class _Slow0:
    """Key 0 runs for 1 s, every other key for 0.05 s."""

    def __call__(self, partition: Partition, resources: object) -> Any:
        time.sleep(1.0 if partition.entry_start == 0 else 0.05)
        return mp.partial("onehot", mp.N, partition.entry_start)


@pytest.mark.parametrize("leg", LEGS)
def test_timed_wake_sees_resume_and_new_slots(leg: str) -> None:
    joined = threading.Event()
    t_flag: list[float] = []

    def join() -> None:
        t_flag.append(time.perf_counter())
        joined.set()

    rec = mp.Recorder()
    ex = SubmitRunner(_Slots(lambda i: 8 if joined.is_set() else 1), monitor=rec)
    ex.control = RunControl()
    plan = mp.make_plan(mp.Probe(n=8, sleep_s=1.0), adaptive=LEGS[leg])
    threading.Timer(0.2, join).start()
    res = mp.Background(lambda: ex.run(plan)).result(30)
    assert res.value == (1,) * 8
    t2 = sorted(e.t for e in rec.events if e.phase is TaskPhase.STARTED)[1]
    assert 0 <= t2 - t_flag[0] < 0.3

    ctl = RunControl()
    rec = mp.Recorder(on_first_finished=ctl.pause)
    ex = SubmitRunner(ThreadBackend(2), monitor=rec)
    ex.control = ctl
    plan = dataclasses.replace(mp.make_plan(mp.Probe(), adaptive=LEGS[leg]), process=_Slow0())
    run = mp.Background(lambda: ex.run(plan))
    assert rec.first_finished.wait(10)
    time.sleep(0.2)
    resumed = time.perf_counter()
    ctl.resume()
    res = run.result()
    started = [e.t for e in rec.events if e.phase is TaskPhase.STARTED]
    later = [t for t in started if t > resumed]
    assert res.stopped is StopReason.EXHAUSTED
    assert len(started) - len(later) == 2
    assert later and min(later) - resumed < 0.5


class _Recording(ThreadBackend):
    """A ``ThreadBackend(2)`` recording, per ``submit``, (an argument is a future, one is unfinished)."""

    def __init__(self) -> None:
        super().__init__(2)
        self.submits: list[tuple[bool, bool]] = []

    def submit(self, fn: Callable[..., object], /, *args: object, **kw: Any) -> SubmitFuture:
        futures = [a for a in args if isinstance(a, SubmitFuture)]
        self.submits.append((bool(futures), any(not f.done() for f in futures)))
        return super().submit(fn, *args, **kw)


def test_combines_submit_after_inputs_complete() -> None:
    probe = mp.Probe(n=8, variant="float", sleep_s=0.05)
    backend = _Recording()
    ex = SubmitRunner(backend)
    ex.control = RunControl()
    res = ex.run(mp.make_plan(probe))
    bare = SubmitRunner(ThreadBackend(2)).run(mp.make_plan(probe))
    assert sum(fut for fut, _ in backend.submits) == 7
    assert not any(unfinished for _, unfinished in backend.submits)
    assert res.value == bare.value


def test_cancel_after_last_leaf_changes_nothing() -> None:
    token = "a3-23"
    ctl = mp.CONTROLS[token] = RunControl()
    backend = _Recording()
    ex = SubmitRunner(backend)
    ex.control = ctl
    res = ex.run(mp.make_plan(mp.Probe(n=8, sleep_s=0.05, cancel_key=7, token=token)))
    bare = SubmitRunner(ThreadBackend(2)).run(mp.make_plan(mp.Probe(n=8, sleep_s=0.05)))
    assert sum(fut for fut, _ in backend.submits) == 7
    assert res.stopped is bare.stopped is StopReason.EXHAUSTED
    assert res.value == (1,) * 8
    assert ctl.state is RunState.RUNNING
