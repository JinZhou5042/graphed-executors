"""m65 A3 fixup: a controlled adaptive ``SubmitRunner`` ends at its ``StopCondition`` like the bare run."""

from __future__ import annotations

from typing import Any

import m65a_probe as mp
from graphed.core import RunControl, RunState, StopCondition, StopReason

from graphed_executors.submit import SubmitRunner, ThreadBackend

TARGET = 5


def _plan() -> Any:
    probe = mp.Probe(variant="int", sleep_s=0.01)
    return mp.make_plan(probe, adaptive=True, stop=StopCondition(target_events=TARGET))


def test_unused_control_ends_at_the_adaptive_stop_condition() -> None:
    bare = SubmitRunner(ThreadBackend(2)).run(_plan())
    ctl = RunControl()
    res = SubmitRunner(ThreadBackend(2), control=ctl).run(_plan())
    assert (res.stopped, res.n_partitions) == (bare.stopped, bare.n_partitions)
    assert (res.stopped, res.n_partitions) == (StopReason.TARGET_EVENTS, TARGET)
    assert res.n_partitions < mp.N
    assert ctl.state is RunState.RUNNING
