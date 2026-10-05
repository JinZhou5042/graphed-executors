"""m65 C: a complete_events hub run keeps no leaf result alive beyond what a plain run does."""

from __future__ import annotations

import gc
import threading
import time
import weakref
from typing import Any

import pytest
from graphed.core import Partition, Plan, RunControl, Task, TaskEvent

from graphed_executors.local import ThreadExecutor

N = 200


class Counter:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.alive = 0
        self.peak = 0

    def blob(self) -> object:
        b = Blob()
        with self.lock:
            self.alive += 1
            self.peak = max(self.peak, self.alive)
        weakref.finalize(b, self._dead)
        return b

    def _dead(self) -> None:
        with self.lock:
            self.alive -= 1


class Blob:
    pass


class Mon:
    def __init__(self, complete: bool) -> None:
        self.complete_events = complete

    def on_task(self, event: TaskEvent) -> None:
        pass

    def on_profile(self, worker: str, payload: bytes) -> None:
        pass

    def on_combine(self, leaves_done: int) -> None:
        pass

    def worker_profiler_factory(self) -> None:
        return None


def _run(complete: bool, **kw: Any) -> tuple[int, int]:
    c = Counter()

    def proc(p: Partition, _r: object) -> object:
        time.sleep(0.001)
        return c.blob()

    plan = Plan(
        process=proc,
        combine=lambda a, b: c.blob(),
        empty=c.blob,
        tasks=[Task(k, Partition.blind("u", "t", k, N)) for k in range(N)],
    )
    ex = ThreadExecutor(2, comms=None, persistent=True, monitor=Mon(complete), **kw)
    try:
        r = ex.run(plan)
        del r
        gc.collect()
        return c.peak, c.alive
    finally:
        ex.close()


@pytest.mark.parametrize(
    "kw",
    [{}, {"pooled_combines": True}, {"control": RunControl()}],
    ids=["fixed", "pooled", "window"],
)
def test_no_leaf_result_alive_after_run(kw: dict[str, Any]) -> None:
    assert _run(True, **kw)[1] == 0


def test_windowed_peak_matches_plain_monitor() -> None:
    plain_peak = _run(False, control=RunControl())[0]
    complete_peak = _run(True, control=RunControl())[0]
    assert plain_peak < N // 4
    assert complete_peak <= 2 * plain_peak
