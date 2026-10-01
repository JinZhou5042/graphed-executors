"""m68c ``SubmitRunner`` on a hand-built ``DurablePlanV2`` over ``ThreadBackend``: a failed stage task stops
the run at its barrier, and a cancel during the last stage returns no value."""

from __future__ import annotations

import pickle
from collections.abc import Callable, Sequence
from typing import Any

import pytest
from graphed.core import DurablePlanV2, OpSpec, Partition, RunControl, RunState, StageSpec, StopReason, Task
from graphed.shuffle import split

from graphed_executors.submit import SubmitFuture, SubmitRunner, ThreadBackend

CONTROL = RunControl()


def _map(task: Task, inputs: Sequence[bytes], resources: object) -> bytes:
    return pickle.dumps({0: pickle.dumps(task.key)})


def _boom(task: Task, inputs: Sequence[bytes], resources: object) -> bytes:
    raise ValueError(f"map {task.key} failed")


def _gather(task: Task, inputs: Sequence[bytes], resources: object) -> bytes:
    return pickle.dumps(sorted(pickle.loads(split(p)[task.key]) for p in inputs))


def _gather_then_cancel(task: Task, inputs: Sequence[bytes], resources: object) -> bytes:
    CONTROL.cancel()
    return _gather(task, inputs, resources)


def _plan(map_fn: Callable[..., bytes], gather_fn: Callable[..., bytes]) -> DurablePlanV2:
    def spec(fn: Callable[..., bytes]) -> OpSpec:
        return OpSpec(kind="ref", ref=f"{__name__}:{fn.__name__}", live=fn)

    maps = tuple(Task(i, Partition("src", "t", i, i + 1)) for i in range(3))
    return DurablePlanV2(
        ir=b"m68c-toy",
        stages=(
            StageSpec(kind="map_write", process=spec(map_fn), tasks=maps),
            StageSpec(
                kind="gather",
                inputs=(0,),
                process=spec(gather_fn),
                tasks=(Task(0, Partition("d", "t", 0, 1)),),
            ),
        ),
    )


class _Keys(ThreadBackend):
    def __init__(self) -> None:
        super().__init__(1)
        self.keys: list[str] = []

    def submit(self, fn: Callable[..., object], /, *args: object, key: str, **hints: Any) -> SubmitFuture:
        self.keys.append(key)
        return super().submit(fn, *args, key=key, **hints)


def test_a_failed_map_task_raises_at_its_barrier_before_any_gather() -> None:
    backend = _Keys()
    with SubmitRunner(backend) as runner, pytest.raises(ValueError, match="failed"):
        runner.run(_plan(_boom, _gather))
    assert backend.keys and not [k for k in backend.keys if "-gather" in k or "-pick." in k], backend.keys


def test_a_cancel_during_the_last_stage_returns_no_value() -> None:
    with SubmitRunner(ThreadBackend(1), control=CONTROL) as runner:
        got = runner.run(_plan(_map, _gather_then_cancel))
    assert (got.value, got.stopped, got.n_partitions, got.n_combines) == (None, StopReason.CANCELLED, 3, 1)
    assert CONTROL.state is RunState.RUNNING
    with SubmitRunner(ThreadBackend(1)) as runner:
        assert runner.run(_plan(_map, _gather)).value == ([0, 1, 2],)
