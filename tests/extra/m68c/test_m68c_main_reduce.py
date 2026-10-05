"""m68c: a ``join_plan`` whose ``reduce``/``combine`` live in ``__main__`` (a notebook's) runs through
``SubmitRunner(ThreadBackend)`` to ``SequentialRunner``'s value, with and without a bound service."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import graphed
import pytest
from graphed import Session
from graphed.awkward import AwkwardBackend
from graphed.core import SequentialRunner
from graphed.preserve import record_external

from graphed_executors.submit import SubmitRunner, ThreadBackend

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "frozen" / "m68c"))
from m68c_harness import LEFT, RIGHT, SCALE, StandIn, _source, cat, given_spec, no_rows, rows, sequential


def _main_fold(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    def main_rows(values: list[Any]) -> Any:
        return rows(values)

    def main_cat(a: list[str], b: list[str]) -> Any:
        return cat(a, b)

    for fn in (main_rows, main_cat):
        fn.__module__, fn.__qualname__ = "__main__", f"m68c_{fn.__name__}"
        monkeypatch.setattr(sys.modules["__main__"], fn.__qualname__, fn, raising=False)
    return {"reduce": main_rows, "combine": main_cat, "empty": no_rows}


def test_a_main_reduce_join_plan_runs_through_submit(monkeypatch: pytest.MonkeyPatch) -> None:
    fold = _main_fold(monkeypatch)
    s = Session(AwkwardBackend())
    joined = graphed.join(
        _source(s, "left", "mem://m68c-main/l", LEFT, None),
        _source(s, "right", "mem://m68c-main/r", RIGHT, None),
        on=["k"],
    )
    plan = graphed.join_plan(joined, steps_per_file=2, **fold)
    want = SequentialRunner().run(plan).value
    assert want
    with SubmitRunner(ThreadBackend(2)) as runner:
        assert runner.run(plan).value == want


def test_a_main_reduce_join_plan_with_a_bound_service_runs_through_submit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fold = _main_fold(monkeypatch)
    s = Session(AwkwardBackend())
    s.declare_service(given_spec())
    left = record_external(
        s,
        SCALE,
        b"m68c-scale",
        [_source(s, "left", "mem://m68c-main-bound/l", LEFT, None)],
        params={"service": "sf"},
    )
    right = _source(s, "right", "mem://m68c-main-bound/r", RIGHT, None)
    plan = graphed.join_plan(graphed.join(left, right, on=["k"]), steps_per_file=2, **fold)
    with StandIn() as server:
        endpoints = {"sf": server.endpoint()}
        want = sequential(plan, endpoints).value
        with SubmitRunner(ThreadBackend(2), services=endpoints) as runner:
            got = runner.run(plan).value
        assert server.calls() > 0
    assert want and got == want
