"""m65 C: a complete_events monitor holds each run's events on a fresh (non-persistent) process hub pool."""

from __future__ import annotations

import threading
import time

from graphed.core import Partition, Plan, Task, TaskEvent, TaskPhase

from graphed_executors.local import ProcessPoolExecutor


def _good(p: Partition, _r: object) -> float:
    time.sleep(0.02)
    return 1.0


def _add(a: float, b: float) -> float:
    return a + b


def _zero() -> float:
    return 0.0


class Rec:
    complete_events = True

    def __init__(self) -> None:
        self.events: list[TaskEvent] = []
        self.lock = threading.Lock()

    def on_task(self, event: TaskEvent) -> None:
        with self.lock:
            self.events.append(event)

    def on_profile(self, worker: str, payload: bytes) -> None:
        pass

    def on_combine(self, leaves_done: int) -> None:
        pass

    def worker_profiler_factory(self) -> None:
        return None

    def take(self) -> tuple[int, int, int]:
        with self.lock:
            got, self.events = self.events, []
        n = [
            sum(e.phase is ph for e in got)
            for ph in (TaskPhase.SUBMITTED, TaskPhase.STARTED, TaskPhase.FINISHED)
        ]
        return n[0], n[1], n[2]


def test_non_persistent_hub_runs_hold_their_events() -> None:
    m = Rec()
    plan = Plan(
        process=_good,
        combine=_add,
        empty=_zero,
        tasks=[Task(k, Partition.blind("u", "t", k, 4)) for k in range(4)],
    )
    ex = ProcessPoolExecutor(2, comms=None, persistent=False, monitor=m)
    try:
        counts = []
        for _ in range(5):
            ex.run(plan)
            counts.append(m.take())
    finally:
        ex.close()
    assert counts == [(4, 4, 4)] * 5
