"""The rerun trigger is ServiceUnreachable only (addendum "B2, the rerun trigger"): a run raising
ServiceUnavailable for a SERVICE node's service ends the try with exit 1 and no rerun."""

from __future__ import annotations

import pickle
import sys
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "frozen" / "m68d"))
from m68d_harness import (
    SERVICE,
    Background,
    Web,
    announce_node,
    driver_api,
    free_range,
    gated_plan,
    log_lines,
    node_spec,
    run_json,
    site_copy,
    write_driver_job,
    write_machine_ad,
)

from graphed_executors.htcondor_backend import HTCondorRunner
from graphed_executors.submit.services import ServiceUnavailable


def test_an_announced_service_s_unavailable_run_exits_1_without_a_rerun(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("_CONDOR_MACHINE_AD", str(write_machine_ad(tmp_path / "driver.ad", "127.0.0.1")))
    site_copy(monkeypatch, "m-dee-node", "generic", worker_ports=free_range(3))
    mark, job, dag = tmp_path / "mark", tmp_path / "job", tmp_path / "dag"
    dag.mkdir()
    write_driver_job(job, gated_plan(mark, node_spec(0.5), n=2), run_json("m-dee-node", dag_dir=dag))

    def unavailable(self: Any, plan: Any) -> Any:
        raise ServiceUnavailable(SERVICE, {"user": "m-the-node-died-after-the-driver-set"})

    monkeypatch.setattr(HTCondorRunner, "run", unavailable)
    with Web() as web:
        driver = Background(lambda: driver_api().main([str(job)]))
        assert announce_node(dag, web.port) == 200
        code = driver.result()
    ok, payload = pickle.loads((job / "result.pkl").read_bytes())
    assert (code, ok) == (1, False) and isinstance(payload, ServiceUnavailable), (code, payload)
    assert log_lines(job, "rerun:") == [], log_lines(job, "rerun:")
