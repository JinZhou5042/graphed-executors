"""A fixed run with a monitor raises well within the drain timeout whatever leaves complete after it (C1's
drain constraint)."""

from __future__ import annotations

import sys
import time
from pathlib import Path

import pytest
from graphed.core.execution import Plan, Task

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "frozen" / "m68d"))
from m68d_harness import (
    HARNESS_DIR,
    LEAF_ERROR,
    SERVICE,
    Background,
    EventLog,
    Web,
    empty_tuple,
    gated_plan,
    given_spec,
    pair_concat,
    partitions,
    raised_by,
    run_bounded,
    stop_leaf,
    wait_for,
)

from graphed_executors.htcondor_backend import HTCondorBackend, HTCondorRunner, LocalPilots
from graphed_executors.submit import SubmitRunner, ThreadBackend
from graphed_executors.submit.engine import _DRAIN_TIMEOUT_S
from graphed_executors.submit.services import ServiceUnreachable


@pytest.mark.parametrize("kind", ["thread", "htcondor"])
def test_a_two_leaf_fixed_run_with_a_monitor_raises_within_the_drain_timeout(
    kind: str, tmp_path: Path
) -> None:
    monitor = EventLog()
    if kind == "thread":
        runner = SubmitRunner(ThreadBackend(1), monitor=monitor)
    else:
        backend = HTCondorBackend(
            LocalPilots(pythonpath=[HARNESS_DIR]), 1, host="127.0.0.1", port_range=(0, 0)
        )
        runner = HTCondorRunner(backend, min_pilots=1, monitor=monitor)
        run_bounded(runner.wait_for_pilots, 120.0)
    mark = tmp_path / "mark"
    mark.mkdir()
    plan = Plan(
        stop_leaf, pair_concat, empty_tuple, tuple(Task(i, p) for i, p in enumerate(partitions(mark, 2)))
    )
    try:
        begun = time.monotonic()
        err = raised_by(lambda: runner.run(plan))
        took = time.monotonic() - begun
    finally:
        run_bounded(runner.close, 120.0)
    assert type(err) is ValueError and str(err) == LEAF_ERROR, repr(err)
    assert len(monitor.phases("submitted")) == 2, "the monitor was not attached to the run"
    assert took < _DRAIN_TIMEOUT_S / 2, f"raised after {took:.1f}s"


def test_a_task_whose_service_is_gone_raises_within_the_drain_timeout_with_a_monitor(tmp_path: Path) -> None:
    web = Web()
    backend = HTCondorBackend(LocalPilots(pythonpath=[HARNESS_DIR]), 1, host="127.0.0.1", port_range=(0, 0))
    runner = HTCondorRunner(backend, min_pilots=1, monitor=EventLog(), services={SERVICE: web.endpoint})
    mark = tmp_path / "mark"
    try:
        run_bounded(runner.wait_for_pilots, 120.0)
        run = Background(lambda: runner.run(gated_plan(mark, given_spec())))
        assert wait_for((mark / "t0.done").exists, 120.0), "task 0 never finished"
        web.close()
        killed = time.monotonic()
        (mark / "killed").touch()
        err = run.error()
        took = time.monotonic() - killed
    finally:
        run_bounded(runner.close, 120.0)
    assert isinstance(err, ServiceUnreachable), repr(err)
    assert took < _DRAIN_TIMEOUT_S / 2, f"raised {took:.1f}s after the service died"
