"""A fixed run with a monitor raises well within the drain timeout, with a control attached (the windowed path)
or without."""

from __future__ import annotations

import sys
import time
from pathlib import Path

import pytest
from graphed.core import RunControl
from graphed.core.execution import Plan, Task

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "frozen" / "m68d"))
from m68d_harness import (
    HARNESS_DIR,
    LEAF_ERROR,
    EventLog,
    empty_tuple,
    pair_concat,
    partitions,
    raised_by,
    run_bounded,
    stop_leaf,
)

from graphed_executors.htcondor_backend import HTCondorBackend, HTCondorRunner, LocalPilots
from graphed_executors.submit import SubmitRunner, ThreadBackend
from graphed_executors.submit.engine import _DRAIN_TIMEOUT_S


@pytest.mark.parametrize("controlled", [False, True])
@pytest.mark.parametrize("kind", ["thread", "htcondor"])
def test_a_fixed_run_that_raises_with_a_monitor_does_not_wait_out_the_drain(
    kind: str, controlled: bool, tmp_path: Path
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
    if controlled:
        runner.control = RunControl()
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
    assert took < _DRAIN_TIMEOUT_S / 2, f"raised after {took:.1f}s"
