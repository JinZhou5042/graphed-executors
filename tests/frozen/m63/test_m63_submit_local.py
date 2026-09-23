"""m63 contracts 1-6 and 8 on the local executors (thread, process pool, pinned pool)."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
from m63_harness import (
    GATE_TIMEOUT_S,
    check_adaptive_equality,
    check_asynchronous,
    check_bound,
    check_cancel_frees_slot,
    check_close_drains,
    check_errors,
    check_fixed_equality,
    check_max_in_flight_ctor,
    check_monitor_attach,
    check_monitor_parity,
    check_order,
    fbytes,
    fixed_plan,
)

from graphed_executors.local import PinnedPoolExecutor, ProcessPoolExecutor, ThreadExecutor

CLASSES: dict[str, type[Any]] = {
    "thread": ThreadExecutor,
    "process": ProcessPoolExecutor,
    "pinned": PinnedPoolExecutor,
}
NAMES = list(CLASSES)


def factory(name: str) -> Callable[..., Any]:
    cls = CLASSES[name]
    return lambda **kw: cls(2, persistent=name != "thread", **kw)


@pytest.fixture(params=NAMES)
def name(request: pytest.FixtureRequest) -> str:
    return str(request.param)


@pytest.fixture
def ex(name: str) -> Iterator[Any]:
    executor = factory(name)()
    try:
        yield executor
    finally:
        executor.close()


@pytest.fixture
def bounded(name: str) -> Iterator[Any]:
    executor = factory(name)(max_in_flight=2)
    try:
        yield executor
    finally:
        executor.close()


def test_max_in_flight_is_a_validated_constructor_argument(name: str) -> None:
    check_max_in_flight_ctor(factory(name))


def test_submit_equals_run_on_an_order_sensitive_fixed_plan(ex: Any) -> None:
    check_fixed_equality(ex)


def test_submit_equals_run_on_an_adaptive_plan(ex: Any) -> None:
    check_adaptive_equality(ex)


def test_submit_returns_before_the_plan_finishes(ex: Any, tmp_path: Path) -> None:
    check_asynchronous(ex, str(tmp_path))


def test_max_in_flight_bounds_outstanding_submits(bounded: Any, tmp_path: Path) -> None:
    check_bound(bounded, str(tmp_path))


def test_cancelling_a_queued_plan_frees_its_slot(bounded: Any, tmp_path: Path) -> None:
    check_cancel_frees_slot(bounded, str(tmp_path))


def test_plans_complete_in_submit_order(ex: Any, name: str, tmp_path: Path) -> None:
    check_order(ex, str(tmp_path), in_process=name == "thread")


def test_errors_propagate_as_under_run(ex: Any) -> None:
    check_errors(ex)


def test_monitor_events_match_run(name: str) -> None:
    cls = CLASSES[name]
    # non-persistent: each leg's pool (and its event collector) ends with the leg
    check_monitor_parity(lambda **kw: cls(2, **kw), exact=name == "thread")


def test_assigned_monitor_reaches_the_next_plan(ex: Any) -> None:
    check_monitor_attach(ex)


def test_close_drains_and_a_closed_executor_still_accepts_submit(ex: Any, tmp_path: Path) -> None:
    check_close_drains(ex, str(tmp_path))
    with ThreadExecutor(2) as reference:
        ref = fbytes(reference.run(fixed_plan(30)).value)
    assert fbytes(ex.submit(fixed_plan(30)).result(timeout=GATE_TIMEOUT_S).value) == ref
