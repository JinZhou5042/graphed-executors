"""m63 contract 9: a local executor runs one plan at a time, whether plans arrive by run or submit."""

from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from graphed.core import Plan
from m63_harness import (
    GATE_TIMEOUT_S,
    Call,
    fbytes,
    overlap_a,
    overlap_b,
    overlap_plan,
    sleepy_plan,
    wait_for,
)

from graphed_executors.local import PinnedPoolExecutor, ProcessPoolExecutor, ThreadExecutor

RunFn = Callable[[Plan[float]], float]


def overlap_seen(run_a: RunFn, run_b: RunFn, d: str) -> float:
    """A's value: 1.0 iff B's leaf ran while A's leaf was still waiting."""
    a = Call(lambda: run_a(overlap_plan(overlap_a, d)))
    started = os.path.join(d, "a_started")
    wait_for(lambda: os.path.exists(started) or not a.thread.is_alive(), timeout_s=GATE_TIMEOUT_S)
    if not os.path.exists(started):
        a.get()  # re-raises why A never started
    assert os.path.exists(started)
    b = Call(lambda: run_b(overlap_plan(overlap_b, d)))
    seen = float(a.get())
    assert b.get() == 0.0
    assert os.path.exists(os.path.join(d, "gate"))
    return seen


@pytest.mark.parametrize("cls", [ThreadExecutor, ProcessPoolExecutor, PinnedPoolExecutor])
def test_run_beside_run_does_not_overlap(cls: type[Any], tmp_path: Path) -> None:
    with cls(2) as ex:
        seen = overlap_seen(lambda p: ex.run(p).value, lambda p: ex.run(p).value, str(tmp_path))
    assert seen == 0.0


def test_run_beside_a_submitted_plan_does_not_overlap(tmp_path: Path) -> None:
    with ThreadExecutor(2) as ex:
        seen = overlap_seen(
            lambda p: ex.submit(p).result(timeout=GATE_TIMEOUT_S).value,
            lambda p: ex.run(p).value,
            str(tmp_path),
        )
    assert seen == 0.0


@pytest.mark.parametrize("cls", [ProcessPoolExecutor, PinnedPoolExecutor])
def test_concurrent_runs_on_a_persistent_pool_keep_their_values(cls: type[Any]) -> None:
    plans = [sleepy_plan(k) for k in range(6)]
    with cls(4, persistent=True) as ex:
        ref = [fbytes(ex.run(p).value) for p in plans]
        drivers = [
            Call(lambda idx=idx: [(i, fbytes(ex.run(plans[i]).value)) for i in idx])
            for idx in ((0, 3), (1, 4), (2, 5))
        ]
        got = dict(pair for d in drivers for pair in d.get(timeout_s=120.0))
    assert len(set(ref)) == len(plans)
    assert [got[i] for i in range(len(plans))] == ref
