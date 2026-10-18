"""#30: a persistent ``PinnedPoolExecutor`` keeps its workers when a plan's task count changes; a merge
target outside a worker's spawn-time overlay is reached through the driver, so no worker gains a handle."""

from __future__ import annotations

import os
import queue
import time
from typing import Any

import pytest
from graphed.core import Partition, Plan, Task

import graphed_executors.local._peer as _peer
import graphed_executors.local.executors as executors
from graphed_executors.local._peer import DRIVER, make_bounds, worker_outbox_addresses
from graphed_executors.local._pinned_pool import PinnedProcessPool
from graphed_executors.local._reduce import tree_reduce
from graphed_executors.local.executors import PinnedPoolExecutor

W = 4
SPAWN_N = 9


@pytest.fixture(autouse=True)
def _bounded_root_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(executors, "_PEER_ROOT_TIMEOUT_S", 60.0)  # a lost node fails in a minute, not five


def _cat(a: str, b: str) -> str:
    return f"({a}+{b})"


def _empty() -> str:
    return "e"


def _leaf(part: Partition, _resources: object) -> str:
    return str(part.entry_start)


def _slow_tail_leaf(part: Partition, _resources: object) -> str:
    if part.entry_start >= 9:  # the last worker's range at n=13, so its idle peers steal from it
        time.sleep(0.2)
    return str(part.entry_start)


def _late_failing_leaf(part: Partition, _resources: object) -> str:
    if part.entry_start == 11:  # w3's last leaf at n=12, failing after w2 has relayed its node to w1
        time.sleep(0.5)
        raise ValueError("leaf 11")
    return str(part.entry_start)


def _plan(n: int, process: Any = _leaf) -> Plan[str]:
    tasks = tuple(Task(i, Partition(f"p{i}", "Events", i, i + 1)) for i in range(n))
    return Plan(process=process, combine=_cat, empty=_empty, tasks=tasks)


def _flat(n: int) -> str:
    return tree_reduce(n, [(i, str(i)) for i in range(n)], _cat, _empty)[0]


def _worker_state() -> tuple[int, tuple[str, ...]]:
    assert _peer._pinned is not None
    return os.getpid(), tuple(sorted(_peer._pinned[1].peers()))


def _states(pool: PinnedProcessPool) -> list[tuple[int, tuple[str, ...]]]:
    return [pool.submit(_worker_state, worker=i).result(timeout=30) for i in range(W)]


def test_a_persistent_pinned_pool_keeps_its_workers_across_task_counts() -> None:
    addrs = tuple(f"w{i}" for i in range(W))
    spawned = worker_outbox_addresses(SPAWN_N, make_bounds(SPAWN_N, W), addrs)
    values: dict[int, str] = {}
    relays: dict[int, int] = {}
    ex = PinnedPoolExecutor(max_workers=W, persistent=True, comms="ipc", steal=False)
    with ex:
        values[SPAWN_N] = ex.run(_plan(SPAWN_N)).value
        relays[SPAWN_N] = ex._last_peer_relays
        pool = ex._peer_pool
        assert isinstance(pool, PinnedProcessPool)
        before = _states(pool)
        for n in (10, 11, 12, 13, 14):
            values[n] = ex.run(_plan(n)).value
            assert ex._peer_pool is pool
            relays[n] = ex._last_peer_relays
        after = _states(pool)
    assert after == before  # same processes, and none gained an outbox
    assert [peers for _, peers in before] == [tuple(sorted(spawned[a])) for a in addrs]
    assert values == {n: _flat(n) for n in values}
    assert relays[SPAWN_N] == 0
    assert sum(relays.values()) > 0


def test_steals_and_relays_together_keep_the_grouping() -> None:
    ex = PinnedPoolExecutor(max_workers=W, persistent=True, comms="ipc", steal=True)
    with ex:
        ex.run(_plan(SPAWN_N))
        pool = ex._peer_pool
        got = ex.run(_plan(13, _slow_tail_leaf)).value
        assert ex._peer_pool is pool
        relays = ex._last_peer_relays
        given = sum(w["given"] for w in ex._last_peer_witness)
    assert got == _flat(13)
    assert relays > 0 and given > 0


def test_a_run_failing_after_a_relay_raises_the_worker_error() -> None:
    ex = PinnedPoolExecutor(max_workers=W, persistent=True, comms="ipc", steal=False)
    with ex:
        ex.run(_plan(SPAWN_N))
        with pytest.raises(ValueError, match="leaf 11"):
            ex.run(_plan(12, _late_failing_leaf))
        assert ex._last_peer_relays > 0
        assert ex.run(_plan(12)).value == _flat(12)


def test_only_a_node_for_a_peer_outside_the_overlay_goes_through_the_driver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_peer, "_pinned", None)
    inbox: queue.Queue[Any] = queue.Queue()
    driver: queue.Queue[Any] = queue.Queue()
    _peer.pinned_peer_init("w2", inbox, {DRIVER: driver}, ())
    assert _peer._pinned is not None
    transport = _peer._pinned[1]
    assert transport.send("w1", ("node", 0, 3, "3"))
    assert not transport.send("w1", ("steal_req", "w2"))
    assert driver.get_nowait() == ("w2", ("via", "w1", ("node", 0, 3, "3")))
    assert driver.empty()
