"""m65 A2.1: a paused hub window refills on resume, and held time never counts as run time."""

from __future__ import annotations

import dataclasses
import threading
import time
from typing import Any

import m65a_probe as mp
from graphed.core import ExecContext, Partition, RunControl, StopReason, Task, TaskPhase

from graphed_executors.local import ThreadExecutor


@dataclasses.dataclass(frozen=True)
class _Slow0:
    """Key 0 runs for 1 s, every other key for 0.05 s."""

    def __call__(self, partition: Partition, resources: object) -> Any:
        time.sleep(1.0 if partition.entry_start == 0 else 0.05)
        return mp.partial("onehot", mp.N, partition.entry_start)


def test_resume_refills_while_a_task_is_still_running() -> None:
    ctl = RunControl()
    rec = mp.Recorder(on_first_finished=ctl.pause)
    ex = ThreadExecutor(max_workers=2, comms=None, monitor=rec, control=ctl)
    plan = dataclasses.replace(mp.make_plan(mp.Probe()), process=_Slow0())
    run = mp.Background(lambda: ex.run(plan))
    assert rec.first_finished.wait(10)
    time.sleep(0.2)
    resumed = time.perf_counter()
    ctl.resume()
    res = run.result()
    later = [e.t for e in rec.events if e.phase is TaskPhase.STARTED and e.t > resumed]
    assert res.stopped is StopReason.EXHAUSTED
    assert later and min(later) - resumed < 0.5  # key 0 still runs until ~1 s: only the timed wake refills


def test_held_time_is_not_run_time() -> None:
    ctl = RunControl()
    seen: list[ExecContext] = []
    batch = [*mp.tasks()]

    def next_tasks(ctx: ExecContext) -> list[Task] | None:
        seen.append(ctx)
        return batch if len(seen) == 1 else None

    def pause_half_a_second() -> None:
        ctl.pause()
        threading.Timer(0.5, ctl.resume).start()

    rec = mp.Recorder(on_first_finished=pause_half_a_second)
    ex = ThreadExecutor(max_workers=2, comms=None, monitor=rec, control=ctl)
    plan = dataclasses.replace(mp.make_plan(mp.Probe(sleep_s=0.05), adaptive=True), next_tasks=next_tasks)
    res = ex.run(plan)
    assert res.stopped is StopReason.EXHAUSTED and res.n_partitions == mp.N
    durations = seen[-1].last_durations
    assert len(durations) == mp.N
    assert max(durations.values()) < 0.05 + 0.25
