"""m63 contracts 1-6 and 8 on ``SubmitRunner(ThreadBackend(...))``."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from m63_harness import (
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
)

from graphed_executors.submit import SubmitRunner, ThreadBackend


def make(**kw: Any) -> SubmitRunner:
    return SubmitRunner(ThreadBackend(2), **kw)


@pytest.fixture
def ex() -> Iterator[Any]:
    runner = make()
    try:
        yield runner
    finally:
        runner.close()


@pytest.fixture
def bounded() -> Iterator[Any]:
    runner = make(max_in_flight=2)
    try:
        yield runner
    finally:
        runner.close()


def test_max_in_flight_is_a_validated_constructor_argument() -> None:
    check_max_in_flight_ctor(make)


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


def test_plans_complete_in_submit_order(ex: Any, tmp_path: Path) -> None:
    check_order(ex, str(tmp_path), in_process=True)


def test_errors_propagate_as_under_run(ex: Any) -> None:
    check_errors(ex)


def test_monitor_events_match_run() -> None:
    check_monitor_parity(make, exact=True)


def test_assigned_monitor_reaches_the_next_plan(ex: Any) -> None:
    check_monitor_attach(ex)


def test_close_drains_submitted_plans(ex: Any, tmp_path: Path) -> None:
    check_close_drains(ex, str(tmp_path))
