"""m65 B: a kept hub pool built for an unmonitored run is respawned when a later run needs events."""

from __future__ import annotations

import time

import m65a_probe as mp
from graphed.core import TaskPhase

from graphed_executors.local import ProcessPoolExecutor


def test_a_monitor_attached_after_an_unmonitored_run_sees_every_worker_event() -> None:
    plan = mp.make_plan(mp.Probe())
    with ProcessPoolExecutor(max_workers=2, comms=None, persistent=True) as ex:
        ex.run(plan)
        rec = mp.Recorder()
        ex.monitor = rec
        ex.run(plan)
        deadline = time.monotonic() + 10.0
        while rec.count(TaskPhase.FINISHED) < mp.N and time.monotonic() < deadline:
            time.sleep(0.02)
    assert rec.keys(TaskPhase.FINISHED) == set(range(mp.N))
