"""m63 contracts 1-6 and 8 on ``parsl_runner`` over a started HighThroughputExecutor."""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("parsl")

from m63_harness import (
    HARNESS_DIR,
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

from graphed_executors.parsl_backend import parsl_runner, start_htex, stop_htex


@pytest.fixture(scope="module")
def htex(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Any]:
    # HTEX workers start from the process_worker_pool script: they need the venv on PATH and this
    # directory on PYTHONPATH to import the harness.
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("PATH", os.path.dirname(sys.executable) + os.pathsep + os.environ.get("PATH", ""))
        mp.setenv("PYTHONPATH", HARNESS_DIR + os.pathsep + os.environ.get("PYTHONPATH", ""))
        executor = start_htex(workers=2, run_dir=str(tmp_path_factory.mktemp("htex")))
        try:
            yield executor
        finally:
            stop_htex(executor)


@pytest.fixture
def ex(htex: Any) -> Iterator[Any]:
    runner = parsl_runner(htex)
    try:
        yield runner
    finally:
        runner.close()


@pytest.fixture
def bounded(htex: Any) -> Iterator[Any]:
    runner = parsl_runner(htex, **{"max_in_flight": 2})
    try:
        yield runner
    finally:
        runner.close()


def test_max_in_flight_is_a_validated_constructor_argument(htex: Any) -> None:
    check_max_in_flight_ctor(lambda **kw: parsl_runner(htex, **kw))


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
    check_order(ex, str(tmp_path), in_process=False)


def test_stage_error_from_a_worker_propagates_as_under_run(ex: Any) -> None:
    check_errors(ex)


def test_monitor_events_match_run(htex: Any) -> None:
    check_monitor_parity(lambda **kw: parsl_runner(htex, **kw), exact=True)


def test_assigned_monitor_reaches_the_next_plan(ex: Any) -> None:
    check_monitor_attach(ex)


def test_close_drains_submitted_plans(ex: Any, tmp_path: Path) -> None:
    check_close_drains(ex, str(tmp_path))
