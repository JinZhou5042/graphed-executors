"""m68b part B2 on the `test-htcondor` pool (plan-services.md §3.3 B2, the `test_driverless_dag_live.py`
row): a driverless run whose GPU service becomes a SERVICE node of the run's DAG.

Needs the `htcondor2` bindings and a personal HTCondor with one simulated GPU
(`probes/m68b/sim_gpu.config`): skipped where the bindings are not installed; where they are, a pool
that lists no schedd, or no GPU, is a failure naming what is missing.

(a) submitted from a cwd other than `log_dir`, the plan's tasks GET the service and its
`resolve_services` puts the body into the value: a universe-7 DAGMan, one SERVICE node `svc0` that
started once on the GPU, the body in `result()`, the node removed by `OtherJobRemoveRequirements` on
this success path, the queue empty. (b) the same service with a plan whose task SIGKILLs the driver:
three driver tries, each retried, never held, and `result()` raising the placeholder `RuntimeError`.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Any

import pytest
from m68b_dag_harness import (
    HARNESS_FILE,
    PLACEHOLDER_TEXT,
    BodyGet,
    DagResolved,
    htcondor_api,
    kill_driver,
    recipes_api,
    run_bounded,
    service_plan,
    wait_for,
)

POOL_PROBE_S = 30.0
LIVE_S = 600.0
LEAVE_S = 180.0  # DAGMan's exit, then the SERVICE node's removal within its job_max_vacate_time


def require_pool() -> Any:
    """The bindings, once a schedd and a GPU answer; a skip without bindings, a failure without a pool."""
    htcondor2: Any = pytest.importorskip(
        "htcondor2",
        reason="the htcondor bindings are not installed (Linux wheels only; the test-htcondor job)",
    )
    try:
        schedds = run_bounded(
            lambda: htcondor2.Collector().query(htcondor2.AdType.Schedd, projection=["Name"]), POOL_PROBE_S
        )
        startds = run_bounded(
            lambda: htcondor2.Collector().query(htcondor2.AdType.Startd, projection=["TotalGPUs"]),
            POOL_PROBE_S,
        )
    except Exception as exc:
        pytest.fail(f"htcondor2 is installed but no pool answers ({exc}): start a personal HTCondor")
    assert schedds, "htcondor2 is installed but the collector lists no schedd: start a personal HTCondor"
    assert any((ad.get("TotalGPUs") or 0) >= 1 for ad in startds), (
        "the pool has no GPU: add probes/m68b/sim_gpu.config's lines (one simulated GPU) to its config"
    )
    return htcondor2


def gpu_web() -> Any:
    """``recipes.http_server`` asking for one GPU, so a driver job cannot host it."""
    spec = recipes_api().http_server("web")
    return dataclasses.replace(
        spec, timeout_s=180.0, launch=dataclasses.replace(spec.launch, resources={"gpus": 1})
    )


def in_queue(schedd: Any, cluster: int) -> list[Any]:
    return list(
        schedd.query(
            constraint=f"ClusterId == {cluster} || DAGManJobId == {cluster}", projection=["ClusterId"]
        )
    )


def history(schedd: Any, constraint: str, projection: list[str]) -> list[Any]:
    return list(schedd.history(constraint, projection, match=10))


def diagnosis(handle: Any) -> str:
    """The run directory's logs, for a failure message."""
    root = Path(handle.log_dir)
    names = ["driver.log", "run.dag.dagman.out", "service-svc0/service.out", "service-svc0/service.err"]
    return "\n".join(f"--- {n}\n{(root / n).read_text()[-3000:]}" for n in names if (root / n).is_file())


def submit_dag(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, plan: Any) -> Any:
    monkeypatch.chdir(tmp_path)  # a cwd other than log_dir: node files resolve from the DAG dir
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    return run_bounded(
        lambda: htcondor_api().submit_driverless(
            plan,
            site="generic",
            n_pilots=1,
            pilots="local",
            request_memory_mb=1024,
            log_dir=log_dir,
            user_modules=[HARNESS_FILE],
        )
    )


def remove_leftovers(htcondor2: Any, schedd: Any, cluster: int) -> None:
    if in_queue(schedd, cluster):
        schedd.act(htcondor2.JobAction.Remove, f"ClusterId == {cluster}")


def test_a_gpu_service_runs_as_a_service_node_of_the_run_s_dag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    htcondor2 = require_pool()
    schedd = htcondor2.Schedd()
    handle = submit_dag(tmp_path, monkeypatch, service_plan((gpu_web(),), BodyGet()))
    cluster = handle.cluster
    try:
        assert handle.dag is True
        status = run_bounded(lambda: handle.wait(timeout=LIVE_S, poll_s=2), LIVE_S + 60)
        assert status == "done", diagnosis(handle)
        value = run_bounded(handle.result).value
        assert isinstance(value, DagResolved), f"the value was shipped unresolved: {value!r}"
        assert "Directory listing" in value.body and set(value.leaves) == {value.body}, value
        assert wait_for(lambda: not in_queue(schedd, cluster), LEAVE_S, poll_s=2), in_queue(schedd, cluster)
        (dagman,) = history(schedd, f"ClusterId == {cluster}", ["JobUniverse"])
        assert dagman["JobUniverse"] == 7
        attrs = ["DAGNodeName", "NumJobStarts", "AssignedGPUs", "RemoveReason"]
        services = [
            ad
            for ad in history(schedd, f"DAGManJobId == {cluster}", attrs)
            if ad.get("DAGNodeName") != "driver"
        ]
        assert [ad.get("DAGNodeName") for ad in services] == ["svc0"], services
        (node,) = services
        assert node["NumJobStarts"] == 1 and node.get("AssignedGPUs"), node
        assert "DAGManJobId" in str(node.get("RemoveReason")), node
    finally:
        remove_leftovers(htcondor2, schedd, cluster)


def test_a_killed_driver_is_retried_then_fails_with_the_placeholder(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    htcondor2 = require_pool()
    schedd = htcondor2.Schedd()
    handle = submit_dag(tmp_path, monkeypatch, service_plan((gpu_web(),), kill_driver, n=1))
    cluster = handle.cluster
    try:
        assert handle.dag is True
        status = run_bounded(lambda: handle.wait(timeout=LIVE_S, poll_s=2), LIVE_S + 60)
        assert status == "failed", diagnosis(handle)
        tries = history(schedd, f'DAGManJobId == {cluster} && DAGNodeName == "driver"', ["ClusterId"])
        assert len({ad["ClusterId"] for ad in tries}) == 3, tries
        with pytest.raises(RuntimeError, match=PLACEHOLDER_TEXT):
            run_bounded(handle.result)
        assert wait_for(lambda: not in_queue(schedd, cluster), LEAVE_S, poll_s=2), in_queue(schedd, cluster)
    finally:
        remove_leftovers(htcondor2, schedd, cluster)
