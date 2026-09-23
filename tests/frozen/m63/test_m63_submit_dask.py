"""m63 contracts 1-8 on ``dask_runner`` over a process-based ``LocalCluster``."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("distributed")

import distributed
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
    flaky_plan,
)

from graphed_executors.dask_backend import dask_runner


@pytest.fixture(scope="module")
def client() -> Iterator[Any]:
    with (
        distributed.LocalCluster(
            n_workers=2, threads_per_worker=1, processes=True, dashboard_address=":0"
        ) as cluster,
        distributed.Client(cluster) as c,
    ):
        yield c


@pytest.fixture
def ex(client: Any) -> Iterator[Any]:
    runner = dask_runner(client)
    try:
        yield runner
    finally:
        runner.close()


@pytest.fixture
def bounded(client: Any) -> Iterator[Any]:
    runner = dask_runner(client, **{"max_in_flight": 2})
    try:
        yield runner
    finally:
        runner.close()


def test_max_in_flight_is_a_validated_constructor_argument(client: Any) -> None:
    check_max_in_flight_ctor(lambda **kw: dask_runner(client, **kw))


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


def test_monitor_events_match_run(client: Any) -> None:
    check_monitor_parity(lambda **kw: dask_runner(client, **kw), exact=True)


def test_assigned_monitor_reaches_the_next_plan(ex: Any) -> None:
    check_monitor_attach(ex)


def test_retries_behave_as_under_run(client: Any, tmp_path: Path) -> None:
    with dask_runner(client) as clean:
        ref = fbytes(clean.run(fixed_plan(5)).value)
    got = {}
    with dask_runner(client, retries=1) as runner:
        for leg in ("run", "submit"):
            marker = tmp_path / f"retry-{leg}"
            assert not marker.exists()
            plan = flaky_plan(str(marker))
            result = runner.run(plan) if leg == "run" else runner.submit(plan).result(timeout=GATE_TIMEOUT_S)
            assert marker.exists()
            got[leg] = fbytes(result.value)
    assert got == {"run": ref, "submit": ref}
    errors = {}
    with dask_runner(client, retries=0) as runner:
        for leg in ("run", "submit"):
            marker = tmp_path / f"noretry-{leg}"
            plan = flaky_plan(str(marker))
            with pytest.raises(ValueError, match="m63 flaky leaf") as caught:
                runner.run(plan) if leg == "run" else runner.submit(plan).result(timeout=GATE_TIMEOUT_S)
            assert marker.exists()
            errors[leg] = type(caught.value)
    assert errors["run"] is errors["submit"]


def test_close_drains_submitted_plans(ex: Any, tmp_path: Path) -> None:
    check_close_drains(ex, str(tmp_path))
