"""m65 A2: every local executor route honours a ``RunControl`` (pause, resume, cancel) per A-2."""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any

import m65a_probe as mp
import pytest
from graphed.core import RunControl, RunState, StopCondition, StopReason, TaskPhase

ALL = list(mp.ROUTES)
THREAD = [name for name, r in mp.ROUTES.items() if r.thread]
ADAPTIVE = [name for name, r in mp.ROUTES.items() if r.adaptive]
ONES = (1,) * mp.N
ZEROS = (0,) * mp.N


def _plan(route: str, probe: mp.Probe, **kw: Any) -> Any:
    return mp.make_plan(probe, adaptive=mp.ROUTES[route].adaptive, **kw)


def _controlled(route: str, control: RunControl, monitor: Any = None) -> Any:
    ex = mp.ROUTES[route].make(monitor)
    ex.control = control
    return ex


def _assert_exact_cancel(res: Any, rec: mp.Recorder) -> None:
    assert res.stopped is StopReason.CANCELLED
    assert set(res.value) <= {0, 1}
    assert sum(res.value) == res.n_partitions < mp.N
    folded = {k for k, v in enumerate(res.value) if v}
    assert rec.keys(TaskPhase.STARTED) <= folded
    assert res.n_combines == res.n_partitions - 1


@pytest.mark.parametrize("route", ALL)
def test_unused_control_is_bit_identical(route: str) -> None:
    probe = mp.Probe(variant="int" if mp.ROUTES[route].adaptive else "float")
    bare = mp.ROUTES[route].make().run(_plan(route, probe))
    ctl = RunControl()
    res = mp.ROUTES[route].make(control=ctl).run(_plan(route, probe))
    assert (res.value, res.n_partitions, res.n_combines, res.stopped) == (
        bare.value,
        bare.n_partitions,
        bare.n_combines,
        bare.stopped,
    )
    assert ctl.state is RunState.RUNNING


@pytest.mark.parametrize("route", ALL)
def test_pause_stops_dispatch_then_resume_completes(route: str) -> None:
    ctl = RunControl()
    rec = mp.Recorder(on_first_finished=ctl.pause)
    ex = _controlled(route, ctl, rec)
    run = mp.Background(lambda: ex.run(_plan(route, mp.Probe(sleep_s=0.05))))
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


@pytest.mark.parametrize("route", ALL)
def test_paused_at_entry_holds_the_run(route: str) -> None:
    ctl = RunControl()
    ctl.pause()
    rec = mp.Recorder()
    ex = _controlled(route, ctl, rec)
    run = mp.Background(lambda: ex.run(_plan(route, mp.Probe())))
    time.sleep(0.5)
    assert rec.count(TaskPhase.STARTED) == 0
    ctl.resume()
    res = run.result()
    assert res.value == ONES
    assert res.stopped is StopReason.EXHAUSTED

    ctl.pause()
    rec = mp.Recorder()
    ex = _controlled(route, ctl, rec)
    run = mp.Background(lambda: ex.run(_plan(route, mp.Probe())))
    time.sleep(0.5)
    ctl.cancel()
    res = run.result()
    assert res.stopped is StopReason.CANCELLED
    assert (res.n_partitions, res.value) == (0, ZEROS)
    assert rec.count(TaskPhase.STARTED) == 0
    assert ctl.state is RunState.RUNNING


@pytest.mark.parametrize("route", ALL)
def test_cancel_drains_and_folds_exactly_the_completed_tasks(route: str) -> None:
    ctl = RunControl()
    rec = mp.Recorder(on_first_finished=ctl.cancel)
    ex = _controlled(route, ctl, rec)
    res = mp.Background(lambda: ex.run(_plan(route, mp.Probe(sleep_s=0.05)))).result()
    _assert_exact_cancel(res, rec)
    assert ctl.state is RunState.RUNNING


@pytest.mark.parametrize("route", ALL)
def test_failure_during_cancel_drain_raises(route: str, tmp_path: Path) -> None:
    entered, release = str(tmp_path / "entered"), str(tmp_path / "release")
    ctl = RunControl()

    def cancel_while_key0_runs() -> None:
        mp._await_file(entered)
        ctl.cancel()
        mp.touch(release)

    rec = mp.Recorder(on_first_finished=cancel_while_key0_runs)
    ex = _controlled(route, ctl, rec)
    probe = mp.Probe(sleep_s=0.05, fail_key=0, entered_path=entered, release_path=release)
    run = mp.Background(lambda: ex.run(_plan(route, probe)))
    with pytest.raises(RuntimeError, match="m65 fail 0"):
        run.result()
    assert os.path.exists(release)
    assert ctl.state is RunState.RUNNING


@pytest.mark.parametrize("route", THREAD)
def test_cancel_no_check_saw_is_reset(route: str) -> None:
    token = f"a2-15c-{route}"
    ctl = mp.CONTROLS[token] = RunControl()
    hub = mp.ROUTES[route].kwargs.get("comms", "ipc") is None
    variant = "float" if hub else "onehot"
    ex = _controlled(route, ctl)
    res = ex.run(_plan(route, mp.Probe(variant=variant, cancel_key=39, token=token)))
    assert ctl.state is RunState.RUNNING
    full = ex.run(_plan(route, mp.Probe(variant=variant)))
    bare = mp.ROUTES[route].make().run(_plan(route, mp.Probe(variant=variant)))
    assert (full.value, full.n_partitions, full.stopped) == (bare.value, mp.N, StopReason.EXHAUSTED)
    if hub:
        assert (res.value, res.stopped) == (bare.value, StopReason.EXHAUSTED)


@pytest.mark.parametrize("route", ["thread-ipc", "thread-http"])
def test_late_node_reaches_a_cancelled_worker(route: str, tmp_path: Path) -> None:
    entered, release = str(tmp_path / "entered"), str(tmp_path / "release")
    ctl = RunControl()
    rec = mp.Recorder()
    ex = _controlled(route, ctl, rec)
    probe = mp.Probe(sleep_s=0.05, hold_key=23, entered_path=entered, release_path=release)
    run = mp.Background(lambda: ex.run(_plan(route, probe)))
    assert mp._await_file(entered)
    ctl.cancel()
    time.sleep(0.5)
    mp.touch(release)
    res = run.result()
    assert res.stopped is StopReason.CANCELLED
    assert set(res.value) <= {0, 1}
    assert res.value[20:24] == (1, 1, 1, 1)
    assert sum(res.value) == res.n_partitions


@pytest.mark.parametrize("route", ADAPTIVE)
def test_stop_condition_does_not_end_a_cancel_drain(route: str, tmp_path: Path) -> None:
    entered, release = str(tmp_path / "entered"), str(tmp_path / "release")
    ctl = RunControl()
    rec = mp.Recorder()
    ex = _controlled(route, ctl, rec)
    probe = mp.Probe(sleep_s=0.05, hold_key=0, entered_path=entered, release_path=release)
    # Key 0 alone reaches the target, so the stop condition can only fire once it completes: in the drain.
    plan = _plan(route, probe, batch=mp.tasks(entries={0: 1000}), stop=StopCondition(target_events=1000))
    run = mp.Background(lambda: ex.run(plan))
    assert mp._await_file(entered)
    ctl.cancel()
    time.sleep(0.5)
    mp.touch(release)
    res = run.result()
    _assert_exact_cancel(res, rec)
    assert res.value[0] == 1
    assert ctl.state is RunState.RUNNING


@pytest.mark.parametrize("empty_plan", [False, True], ids=["40-tasks", "no-tasks"])
@pytest.mark.parametrize("route", ALL)
def test_cancelled_control_does_no_work(route: str, empty_plan: bool) -> None:
    ctl = RunControl()
    ctl.cancel()
    rec = mp.Recorder()
    ex = _controlled(route, ctl, rec)
    plan = _plan(route, mp.Probe(), batch=[] if empty_plan else None)
    res = ex.run(plan)
    assert res.stopped is StopReason.CANCELLED
    assert (res.value, res.n_partitions, res.n_combines) == (ZEROS, 0, 0)
    assert rec.events == []
    assert ctl.state is RunState.RUNNING
    if plan.next_tasks is not None:
        assert plan.next_tasks.calls == 0


@pytest.mark.parametrize("route", ALL)
def test_control_attribute(route: str) -> None:
    assert mp.ROUTES[route].make().control is None
    by_kwarg = RunControl()
    ex = mp.ROUTES[route].make(control=by_kwarg)
    assert ex.control is by_kwarg
    by_kwarg.cancel()
    assert ex.run(_plan(route, mp.Probe())).stopped is StopReason.CANCELLED
    assert by_kwarg.state is RunState.RUNNING

    ex = mp.ROUTES[route].make()
    ex.control = by_attribute = RunControl()
    by_attribute.cancel()
    assert ex.run(_plan(route, mp.Probe())).stopped is StopReason.CANCELLED
    assert by_attribute.state is RunState.RUNNING
