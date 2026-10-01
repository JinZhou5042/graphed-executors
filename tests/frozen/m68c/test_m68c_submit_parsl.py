"""m68c §6 on parsl: ``SubmitRunner(ParslBackend)`` over a started HighThroughputExecutor runs a service
join plan with the driver-side (map, dest) edge; the peer transport runner refuses a join plan."""

from __future__ import annotations

import dataclasses
import os
import sys
from collections.abc import Iterator
from typing import Any

import pytest

pytest.importorskip("parsl")

from m68c_harness import (
    HARNESS_DIR,
    RecordingBackend,
    StandIn,
    closing,
    given_spec,
    join_v2,
    run_bounded,
    sequential,
)

from graphed_executors.parsl_backend import ParslBackend, start_htex, stop_htex
from graphed_executors.parsl_backend.transport_peer import parsl_run_plan
from graphed_executors.submit import SubmitRunner

PARTS = 8


@pytest.fixture(scope="module")
def htex(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Any]:
    # HTEX workers need the venv on PATH and this directory on PYTHONPATH to import the harness.
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("PATH", os.path.dirname(sys.executable) + os.pathsep + os.environ.get("PATH", ""))
        mp.setenv("PYTHONPATH", HARNESS_DIR + os.pathsep + os.environ.get("PYTHONPATH", ""))
        executor = start_htex(workers=2, run_dir=str(tmp_path_factory.mktemp("htex")))
        try:
            yield executor
        finally:
            stop_htex(executor)


def _measured_run(htex: Any, tag: str, peer: bool | None) -> tuple[int, int]:
    plan = join_v2(tag, spec=given_spec(), steps_per_file=PARTS)
    backend = RecordingBackend(ParslBackend(htex), wrap=False)
    assert backend.capabilities.peer_data_movement is False  # HTEX resolves future args on the driver
    if peer is not None:
        backend.capabilities = dataclasses.replace(backend.capabilities, peer_data_movement=peer)
    with StandIn() as server:
        with closing(SubmitRunner(backend, services={"sf": server.endpoint()})) as runner:
            got = run_bounded(lambda: runner.run(plan))
        calls = server.calls()
        ref = sequential(plan, {"sf": server.endpoint()})
    assert calls > 0, "no stage task called the server"
    assert got.value == ref.value and got.value, (got.value, ref.value)
    return backend.measure()


def test_parsl_htex_runs_a_service_join_moving_each_map_result_once(htex: Any) -> None:
    moved, maps = _measured_run(htex, "parsl-driver", peer=None)
    assert moved < 2 * maps, (moved, maps)


def test_parsl_htex_with_the_pick_task_edge_moves_each_map_result_per_dest(htex: Any) -> None:
    moved, maps = _measured_run(htex, "parsl-peer", peer=True)
    assert moved >= PARTS / 2 * maps, (moved, maps)


def test_the_parsl_peer_transport_refuses_a_join_plan_naming_submit_runner() -> None:
    plan = join_v2("refuse-parsl", spec=given_spec())
    with pytest.raises(TypeError) as excinfo:
        run_bounded(lambda: parsl_run_plan(plan, None))
    assert "SubmitRunner(" in str(excinfo.value) and "DurablePlanV2" in str(excinfo.value), excinfo.value
