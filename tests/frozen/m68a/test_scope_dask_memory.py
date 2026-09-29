"""m68a §3.1 engine: the per-run wrapper holds only futures not yet done, because a held dask future pins
its result in cluster memory. Runs in the ``test-dask`` job (``importorskip("distributed")``, in the fixture, so collecting the file
imports nothing: ``distributed`` installs a process-wide exception pickler).

One batch of 40 leaves through ``SubmitRunner(DaskBackend(LocalCluster 2x1), control=RunControl())``,
the windowed adaptive path, which bounds outstanding leaves by ``task_slots()``. At each ``next_tasks``
call the plan counts this run's ``-leaf-`` keys that ``client.who_has()`` lists with a non-empty holder
list (a queued or processing key has an empty one): the count never exceeds ``backend.task_slots()``.
A wrapper that records every future it submits keeps all 40 results held.

Discriminates: a consumed result held until the run ends.
"""

from __future__ import annotations

import importlib
import threading
import time
from collections.abc import Iterator
from typing import Any

import pytest
from graphed.core import RunControl
from graphed.core.execution import ExecContext, Partition, Plan, Task
from services_harness import run_bounded, submit_api

N_LEAVES = 40
SETTLE_S = 0.1  # >> distributed's batched-send interval (2 ms); a held future is never released


def leaf_payload(partition: Partition, resources: object) -> bytes:
    """A leaf result worth holding: 64 kB tagged by its partition."""
    return partition.uri.encode().ljust(1 << 16, b".")


def keep_count(a: int, b: int) -> int:
    return a + b


def zero() -> int:
    return 0


def count_leaf(partition: Partition, resources: object) -> int:
    return len(leaf_payload(partition, resources))


class HeldCounter:
    """``next_tasks``: the 40 leaves once, then done; at every call, the held ``-leaf-`` keys."""

    def __init__(self, client: Any) -> None:
        self.client = client
        self.held: list[int] = []
        self._sent = False
        self._lock = threading.Lock()

    def __call__(self, ctx: ExecContext) -> list[Task] | None:
        time.sleep(SETTLE_S)  # a dropped future's release reaches the scheduler on a batched stream
        who_has = self.client.who_has()
        held = sum(1 for key, holders in who_has.items() if "-leaf-" in str(key) and len(holders) > 0)
        with self._lock:
            self.held.append(held)
            if self._sent:
                return None
            self._sent = True
        return [Task(i, Partition(f"mem://m68a-dask/{i}", "", i, i + 1)) for i in range(N_LEAVES)]


@pytest.fixture(scope="module")
def client() -> Iterator[Any]:
    distributed = pytest.importorskip("distributed")
    with (
        distributed.LocalCluster(
            n_workers=2, threads_per_worker=1, processes=False, dashboard_address=None
        ) as cluster,
        distributed.Client(cluster) as c,
    ):
        yield c


def test_at_most_task_slots_leaf_results_are_held(client: Any) -> None:
    dask_backend = importlib.import_module("graphed_executors.dask_backend")
    plugin = importlib.import_module("graphed_executors.dask_backend.plugin")
    client.register_plugin(plugin.GraphedWorkerPlugin())
    backend = dask_backend.DaskBackend(client)
    counter = HeldCounter(client)
    plan = Plan(process=count_leaf, combine=keep_count, empty=zero, next_tasks=counter)
    runner = submit_api().SubmitRunner(backend, control=RunControl())
    try:
        result = run_bounded(lambda: runner.run(plan), 240.0)
    finally:
        run_bounded(runner.close, 240.0)
    slots = backend.task_slots()
    assert slots == 2
    assert result.n_partitions == N_LEAVES and result.value == N_LEAVES * (1 << 16)
    assert len(counter.held) >= N_LEAVES, counter.held  # one count per consumed leaf, plus the first call
    assert max(counter.held) >= 1, "the count never saw a held leaf result: the witness is blind"
    assert max(counter.held) <= slots, counter.held
