"""The frozen A2 control bodies (``test_m65a2_control_routes.py`` at ``freeze-m65a2``), each with its
route replaced by a :class:`Leg` and nothing else changed."""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import m65a_probe as mp
import pytest
from graphed.core import RunControl, RunState, StopCondition, StopReason, TaskPhase

ONES = (1,) * mp.N
ZEROS = (0,) * mp.N


@dataclass(frozen=True)
class Leg:
    """A ``SubmitRunner`` factory ``(monitor=None, **extra) -> runner`` and the plan path it runs."""

    factory: Callable[..., Any]
    adaptive: bool = False

    def make(self, monitor: Any = None, **extra: Any) -> Any:
        return self.factory(monitor=monitor, **extra)


def _plan(leg: Leg, probe: mp.Probe, **kw: Any) -> Any:
    return mp.make_plan(probe, adaptive=leg.adaptive, **kw)


def _controlled(leg: Leg, control: RunControl, monitor: Any = None) -> Any:
    ex = leg.make(monitor)
    ex.control = control
    return ex


def _assert_exact_cancel(res: Any, rec: mp.Recorder) -> None:
    assert res.stopped is StopReason.CANCELLED
    assert set(res.value) <= {0, 1}
    assert sum(res.value) == res.n_partitions < mp.N
    folded = {k for k, v in enumerate(res.value) if v}
    assert rec.keys(TaskPhase.STARTED) <= folded
    assert res.n_combines == res.n_partitions - 1


def unused_control_is_bit_identical(leg: Leg) -> None:
    probe = mp.Probe(variant="int" if leg.adaptive else "float")
    bare = leg.make().run(_plan(leg, probe))
    ctl = RunControl()
    res = leg.make(control=ctl).run(_plan(leg, probe))
    assert (res.value, res.n_partitions, res.n_combines, res.stopped) == (
        bare.value,
        bare.n_partitions,
        bare.n_combines,
        bare.stopped,
    )
    assert ctl.state is RunState.RUNNING


def pause_stops_dispatch_then_resume_completes(leg: Leg) -> None:
    ctl = RunControl()
    rec = mp.Recorder(on_first_finished=ctl.pause)
    ex = _controlled(leg, ctl, rec)
    run = mp.Background(lambda: ex.run(_plan(leg, mp.Probe(sleep_s=0.05))))
    assert rec.first_finished.wait(30)
    time.sleep(0.4)
    s1 = rec.count(TaskPhase.STARTED)
    time.sleep(0.6)
    s2 = rec.count(TaskPhase.STARTED)
    ctl.resume()
    res = run.result()
    assert s1 == s2 < mp.N
    assert rec.submitted_at_first_finished == mp.N
    assert res.value == ONES
    assert res.stopped is StopReason.EXHAUSTED


def paused_at_entry_holds_the_run(leg: Leg) -> None:
    ctl = RunControl()
    ctl.pause()
    rec = mp.Recorder()
    ex = _controlled(leg, ctl, rec)
    run = mp.Background(lambda: ex.run(_plan(leg, mp.Probe())))
    time.sleep(0.5)
    assert rec.count(TaskPhase.STARTED) == 0
    ctl.resume()
    res = run.result()
    assert res.value == ONES
    assert res.stopped is StopReason.EXHAUSTED

    ctl.pause()
    rec = mp.Recorder()
    ex = _controlled(leg, ctl, rec)
    run = mp.Background(lambda: ex.run(_plan(leg, mp.Probe())))
    time.sleep(0.5)
    ctl.cancel()
    res = run.result()
    assert res.stopped is StopReason.CANCELLED
    assert (res.n_partitions, res.value) == (0, ZEROS)
    assert rec.count(TaskPhase.STARTED) == 0
    assert ctl.state is RunState.RUNNING


def cancel_drains_and_folds_exactly_the_completed_tasks(leg: Leg) -> None:
    ctl = RunControl()
    rec = mp.Recorder(on_first_finished=ctl.cancel)
    ex = _controlled(leg, ctl, rec)
    res = mp.Background(lambda: ex.run(_plan(leg, mp.Probe(sleep_s=0.05)))).result()
    _assert_exact_cancel(res, rec)
    assert ctl.state is RunState.RUNNING


def failure_during_cancel_drain_raises(leg: Leg, tmp_path: Path) -> None:
    entered, release = str(tmp_path / "entered"), str(tmp_path / "release")
    ctl = RunControl()

    def cancel_while_key0_runs() -> None:
        mp._await_file(entered)
        ctl.cancel()
        mp.touch(release)

    rec = mp.Recorder(on_first_finished=cancel_while_key0_runs)
    ex = _controlled(leg, ctl, rec)
    probe = mp.Probe(sleep_s=0.05, fail_key=0, entered_path=entered, release_path=release)
    run = mp.Background(lambda: ex.run(_plan(leg, probe)))
    with pytest.raises(RuntimeError, match="m65 fail 0"):
        run.result()
    assert os.path.exists(release)
    assert ctl.state is RunState.RUNNING


def cancel_no_check_saw_is_reset(leg: Leg, token: str, *, hub: bool) -> None:
    ctl = mp.CONTROLS[token] = RunControl()
    variant = "float" if hub else "onehot"
    ex = _controlled(leg, ctl)
    res = ex.run(_plan(leg, mp.Probe(variant=variant, cancel_key=39, token=token)))
    assert ctl.state is RunState.RUNNING
    full = ex.run(_plan(leg, mp.Probe(variant=variant)))
    bare = leg.make().run(_plan(leg, mp.Probe(variant=variant)))
    assert (full.value, full.n_partitions, full.stopped) == (bare.value, mp.N, StopReason.EXHAUSTED)
    if hub:
        assert (res.value, res.stopped) == (bare.value, StopReason.EXHAUSTED)


def stop_condition_does_not_end_a_cancel_drain(leg: Leg, tmp_path: Path) -> None:
    entered, release = str(tmp_path / "entered"), str(tmp_path / "release")
    ctl = RunControl()
    rec = mp.Recorder()
    ex = _controlled(leg, ctl, rec)
    probe = mp.Probe(sleep_s=0.05, hold_key=0, entered_path=entered, release_path=release)
    # Key 0 alone reaches the target, so the stop condition can only fire once it completes: in the drain.
    plan = _plan(leg, probe, batch=mp.tasks(entries={0: 1000}), stop=StopCondition(target_events=1000))
    run = mp.Background(lambda: ex.run(plan))
    assert mp._await_file(entered)
    ctl.cancel()
    time.sleep(0.5)
    mp.touch(release)
    res = run.result()
    _assert_exact_cancel(res, rec)
    assert res.value[0] == 1
    assert ctl.state is RunState.RUNNING


def cancelled_control_does_no_work(leg: Leg, empty_plan: bool) -> None:
    ctl = RunControl()
    ctl.cancel()
    rec = mp.Recorder()
    ex = _controlled(leg, ctl, rec)
    plan = _plan(leg, mp.Probe(), batch=[] if empty_plan else None)
    res = ex.run(plan)
    assert res.stopped is StopReason.CANCELLED
    assert (res.value, res.n_partitions, res.n_combines) == (ZEROS, 0, 0)
    assert rec.events == []
    assert ctl.state is RunState.RUNNING
    if plan.next_tasks is not None:
        assert plan.next_tasks.calls == 0


def started_before_first_finished(rec: mp.Recorder) -> int:
    """How many tasks started before any task finished (every event carries one clock: ``perf_counter``)."""
    first = min(e.t for e in rec.events if e.phase is TaskPhase.FINISHED)
    return sum(e.phase is TaskPhase.STARTED and e.t < first for e in rec.events)
