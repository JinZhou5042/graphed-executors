"""m68d on a pool (rows L1-L4 of §4): driverless DAGs whose GPU service is the SERVICE node svc0.

Needs the ``htcondor2`` bindings and a personal HTCondor with one simulated GPU whose jobs share this
host's ``/tmp`` (the ``test-htcondor`` job's pool): skipped without the bindings, a failure naming what is
missing when a pool does not answer. The node runs ``m68d_child.py`` (a recipe input), which records each
start in a report under ``tmp_path``; the plan's tasks mark their progress there too.
"""

from __future__ import annotations

import os
import re
import signal
from pathlib import Path
from typing import Any

import pytest
from graphed.core.execution import SequentialRunner
from m68d_harness import (
    HARNESS_FILE,
    NODE,
    GatedGet,
    SlowToLoad,
    Web,
    child_starts,
    gated_plan,
    htcondor_api,
    node_spec,
    run_bounded,
    wait_for,
)

POOL_PROBE_S = 30.0
LIVE_S = 600.0
LEAVE_S = 180.0
CHILD = Path(HARNESS_FILE).with_name("m68d_child.py")


def require_pool() -> Any:
    htcondor2: Any = pytest.importorskip(
        "htcondor2", reason="the htcondor bindings are not installed (Linux wheels only; the test-htcondor job)"
    )
    try:
        schedds = run_bounded(
            lambda: htcondor2.Collector().query(htcondor2.AdType.Schedd, projection=["Name"]), POOL_PROBE_S
        )
        startds = run_bounded(
            lambda: htcondor2.Collector().query(htcondor2.AdType.Startd, projection=["TotalGPUs"]), POOL_PROBE_S
        )
    except Exception as exc:
        pytest.fail(f"htcondor2 is installed but no pool answers ({exc}): start a personal HTCondor")
    assert schedds, "htcondor2 is installed but the collector lists no schedd: start a personal HTCondor"
    assert any((ad.get("TotalGPUs") or 0) >= 1 for ad in startds), "the pool has no GPU: add one simulated GPU"
    return htcondor2


def child_spec(
    report: Path, mode: str = "serve", arg: str = "", *, timeout_s: float, memory_mb: int | None = None
) -> Any:
    argv = ["{python}", CHILD.name, mode, "{port}", str(report), *([arg] if arg else [])]
    return node_spec(timeout_s, argv, memory_mb=memory_mb, inputs=(str(CHILD),))


def submit_dag(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, plan: Any) -> Any:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "logs").mkdir()
    return run_bounded(
        lambda: htcondor_api().submit_driverless(
            plan,
            site="generic",
            n_pilots=1,
            pilots="local",
            request_memory_mb=1024,
            log_dir=tmp_path / "logs",
            user_modules=[HARNESS_FILE],
        )
    )


def in_queue(schedd: Any, dag: int) -> list[Any]:
    return list(schedd.query(constraint=f"ClusterId == {dag} || DAGManJobId == {dag}", projection=["ClusterId"]))


def history(schedd: Any, constraint: str, projection: list[str]) -> list[Any]:
    return list(schedd.history(constraint, projection, match=20))


def driver_tries(schedd: Any, dag: int) -> list[Any]:
    tries = history(
        schedd, f'DAGManJobId == {dag} && DAGNodeName == "driver"', ["ClusterId", "ExitCode", "RemoteWallClockTime"]
    )
    return sorted(tries, key=lambda ad: int(ad["ClusterId"]))


def diagnosis(handle: Any) -> str:
    root = Path(handle.log_dir)
    names = ["driver.log", "run.dag.dagman.out", "service-svc0/service.out", "service-svc0/service.err"]
    return "\n".join(f"--- {n}\n{(root / n).read_text()[-3000:]}" for n in names if (root / n).is_file())


def finish(htcondor2: Any, schedd: Any, dag: int) -> None:
    """Remove what is left of the run and wait until none of it is queued."""
    if in_queue(schedd, dag):
        schedd.act(htcondor2.JobAction.Remove, f"ClusterId == {dag} || DAGManJobId == {dag}")
    assert wait_for(lambda: not in_queue(schedd, dag), LEAVE_S, poll_s=2), in_queue(schedd, dag)


def driver_log(handle: Any) -> str:
    return str(handle.logs().get("driver.log", ""))


def twin_value(tmp_path: Path) -> Any:
    mark = tmp_path / "twin"
    mark.mkdir()
    (mark / "killed").touch()
    with Web() as web:
        return SequentialRunner().run(gated_plan(mark, None, process=GatedGet(web.endpoint))).value


def test_a_killed_service_child_is_restarted_and_the_dag_completes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    htcondor2 = require_pool()
    schedd = htcondor2.Schedd()
    mark, report = tmp_path / "mark", tmp_path / "child"
    handle = submit_dag(tmp_path, monkeypatch, gated_plan(mark, child_spec(report, timeout_s=180.0)))
    try:
        assert wait_for((mark / "t0.done").exists, LIVE_S, poll_s=1), diagnosis(handle)
        os.kill(child_starts(report)[-1][0], signal.SIGKILL)
        assert wait_for(lambda: len(child_starts(report)) == 2, 60.0), child_starts(report)
        (mark / "killed").touch()
        status = run_bounded(lambda: handle.wait(timeout=LIVE_S, poll_s=2), LIVE_S + 60)
        assert status == "done", diagnosis(handle)
        value = run_bounded(handle.result).value
        assert value == twin_value(tmp_path), value
        assert len(re.findall(r"^rerun:", driver_log(handle), re.MULTILINE)) == 1, driver_log(handle)
        assert len({int(ad["ClusterId"]) for ad in driver_tries(schedd, handle.cluster)}) == 1
        finish(htcondor2, schedd, handle.cluster)
        (node,) = history(schedd, f'DAGManJobId == {handle.cluster} && DAGNodeName == "{NODE}"', ["NumJobStarts"])
        assert node["NumJobStarts"] == 1, node
    finally:
        finish(htcondor2, schedd, handle.cluster)


def test_a_removed_service_node_fails_each_driver_try_fast(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    htcondor2 = require_pool()
    schedd = htcondor2.Schedd()
    mark, report = tmp_path / "mark", tmp_path / "child"
    handle = submit_dag(tmp_path, monkeypatch, gated_plan(mark, child_spec(report, timeout_s=600.0)))
    dag = handle.cluster
    node = f'DAGManJobId == {dag} && DAGNodeName == "{NODE}"'
    try:
        assert wait_for((mark / "t0.done").exists, LIVE_S, poll_s=1), diagnosis(handle)
        schedd.act(htcondor2.JobAction.Remove, node)
        assert wait_for(lambda: not list(schedd.query(constraint=node, projection=["ClusterId"])), 120.0, poll_s=1)
        (mark / "killed").touch()
        status = run_bounded(lambda: handle.wait(timeout=LIVE_S, poll_s=2), LIVE_S + 60)
        assert status == "failed", diagnosis(handle)
        tries = driver_tries(schedd, dag)
        assert [ad.get("ExitCode") for ad in tries] == [1, 1, 1], (tries, diagnosis(handle))
        assert all(float(ad["RemoteWallClockTime"]) < 120 for ad in tries), tries
    finally:
        finish(htcondor2, schedd, dag)


def test_a_slow_binding_service_beside_the_driver_takes_another_port(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    htcondor2 = require_pool()
    schedd = htcondor2.Schedd()
    mark, report = tmp_path / "mark", tmp_path / "child"
    mark.mkdir()
    (mark / "killed").touch()
    # svc0 scans before the driver binds; its child binds well after
    spec = child_spec(report, "slow", "45", timeout_s=90.0)
    handle = submit_dag(tmp_path, monkeypatch, gated_plan(mark, spec, process=SlowToLoad(delay_s=20.0)))
    try:
        status = run_bounded(lambda: handle.wait(timeout=LIVE_S, poll_s=2), LIVE_S + 60)
        log = driver_log(handle)
        driver_port = re.findall(r" pilots on http://\S+:(\d+)", log)
        service_port = re.findall(r"managed leg \(cluster\) at \S+:(\d+)", log)
        assert status == "done", diagnosis(handle)
        assert driver_port and service_port and service_port[0] != driver_port[0], log
        assert child_starts(report)[0][1] == int(driver_port[0]), f"no race: {child_starts(report)} vs {driver_port}"
    finally:
        finish(htcondor2, schedd, handle.cluster)


def test_an_unmatchable_service_node_fails_the_driver_fast(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    htcondor2 = require_pool()
    schedd = htcondor2.Schedd()
    slots = htcondor2.Collector().query(htcondor2.AdType.Startd, projection=["TotalSlotMemory", "Memory"])
    largest = max(int(ad.get("TotalSlotMemory") or ad.get("Memory") or 0) for ad in slots)
    mark, report = tmp_path / "mark", tmp_path / "child"
    asked = largest + 100000
    spec = child_spec(report, timeout_s=600.0, memory_mb=asked)
    handle = submit_dag(tmp_path, monkeypatch, gated_plan(mark, spec))
    try:
        status = run_bounded(lambda: handle.wait(timeout=300.0, poll_s=2), 360.0)
        assert status == "failed", diagnosis(handle)
        tries = driver_tries(schedd, handle.cluster)
        assert tries and all(ad.get("ExitCode") == 1 for ad in tries), (tries, diagnosis(handle))
        assert all(float(ad["RemoteWallClockTime"]) < 60 for ad in tries), tries
        assert re.search(rf"RequestMemory\D{{0,16}}{asked}\b", driver_log(handle)), driver_log(handle)
    finally:
        finish(htcondor2, schedd, handle.cluster)
