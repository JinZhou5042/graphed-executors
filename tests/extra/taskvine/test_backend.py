"""Smoke test for the optional TaskVine executor package interface."""

from pathlib import Path

import pytest
from graphed.core import Partition, Plan, Task
from graphed.core.execution import Executor, StopReason

from graphed_executors.taskvine_backend import TaskVineExecutor

pytest.importorskip("ndcctools.taskvine.vine_graph")


def count(partition: Partition, resources: object) -> int:
    return partition.n_entries


def add(left: int, right: int) -> int:
    return left + right


def zero() -> int:
    return 0


def test_local_plan_uses_public_taskvine_executor(tmp_path: Path) -> None:
    plan = Plan(
        process=count,
        combine=add,
        empty=zero,
        tasks=tuple(Task(i, Partition("demo", "", i, i + 1)) for i in range(4)),
    )
    with TaskVineExecutor(local=True, port=0, work_dir=tmp_path) as executor:
        assert isinstance(executor, Executor)
        result = executor.run(plan)
    assert (result.value, result.n_partitions, result.n_combines, result.stopped) == (
        4,
        4,
        3,
        StopReason.EXHAUSTED,
    )
