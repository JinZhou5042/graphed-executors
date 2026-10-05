"""A process worker's exit flush delivers the batch its drain thread already took, within a bound."""

from __future__ import annotations

import atexit
import functools
import threading
import time
from typing import Any

import pytest
from graphed.core import Partition, Plan, Task, TaskEvent, TaskPhase

import graphed_executors.local.executors as executors
from graphed_executors.local import ProcessPoolExecutor


class _SlowDrainSend:
    """Delays every put made off the worker's main thread, i.e. the drain thread's batch sends."""

    def __init__(self, q: Any, delay: float) -> None:
        self._q = q
        self._delay = delay

    def put_nowait(self, item: object) -> None:
        if threading.current_thread() is not threading.main_thread():
            time.sleep(self._delay)
        self._q.put_nowait(item)


def _boom_after_drain_takes_it(delay: float, partition: Partition, resources: object) -> int:
    executors._proc_event_q = _SlowDrainSend(executors._proc_event_q, delay)
    # runs before the exit flush (LIFO), outlasting one drain tick so the drain thread holds ERRORED
    atexit.register(time.sleep, 3 * executors._PROC_DRAIN_INTERVAL)
    raise ValueError("boom")


def _add(a: int, b: int) -> int:
    return a + b


def _zero() -> int:
    return 0


class _Rec:
    def __init__(self) -> None:
        self.events: list[TaskEvent] = []

    def on_task(self, event: TaskEvent) -> None:
        self.events.append(event)

    def on_profile(self, worker: str, payload: bytes) -> None:
        pass

    def on_combine(self, leaves_done: int) -> None:
        pass

    def worker_profiler_factory(self) -> None:
        return None


def _run(delay: float) -> tuple[_Rec, float]:
    rec = _Rec()
    plan = Plan(
        process=functools.partial(_boom_after_drain_takes_it, delay),
        combine=_add,
        empty=_zero,
        tasks=[Task(0, Partition("bad.root", "Events", 0, 10))],
    )
    t0 = time.monotonic()
    with pytest.raises(ValueError, match="boom"), ProcessPoolExecutor(1, monitor=rec, comms=None) as ex:
        ex.run(plan)
    return rec, time.monotonic() - t0


def test_errored_taken_by_the_drain_thread_reaches_the_monitor() -> None:
    rec, _ = _run(delay=0.5)
    errored = [e for e in rec.events if e.phase is TaskPhase.ERRORED]
    assert len(errored) == 1
    assert "boom" in (errored[0].error or "")


def test_a_hung_drain_send_does_not_hang_the_run() -> None:
    _, elapsed = _run(delay=60.0)
    assert elapsed < 30.0
