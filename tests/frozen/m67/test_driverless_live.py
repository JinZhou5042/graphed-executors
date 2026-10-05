"""m67 D6: driverless runs on a real HTCondor pool (the personal pool of the ``test-htcondor`` CI job).

The module needs the ``htcondor2`` bindings and is skipped with that reason where they are not
installed. Where they are installed, each test first requires a schedd in the collector within a
bound and fails with the reason when there is none: a missing pool is reported, never passed over.

(a) ``pilots="local"`` on the generic profile, two pilots: ``wait()`` then ``result()`` is bit-for-bit,
the leaves ran on two pids inside the driver job, ``driver.log`` came back to ``log_dir``, the queue
is empty and the history row has ``ExitCode 0``; (c) that row carries ``JobMaxRetries == 2`` and an
``OnExitRemove`` that ends retries on exit code 3. (b) ``pilots="condor"``: the driver job submitted
its own pilot cluster, the leaves ran in jobs of that cluster (not the driver's), and the history
holds at least three rows. (d) a plan error: ``result()`` re-raises the ``StageError``, the status is
failed, and the history row has ``ExitCode 3`` after exactly one start.

Discriminates a driver that never runs pilots, a result that never comes back, a driver job that
cannot self-submit, a retried plan error, and a lost exception."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
from driverless_harness import (
    HARNESS_FILE,
    assert_intact_stage_error,
    expected_concat,
    htcondor_api,
    job_plan,
    run_bounded,
    stage_error_plan,
    wait_for,
)

htcondor2: Any = pytest.importorskip(
    "htcondor2", reason="the htcondor bindings are not installed (Linux wheels only; the test-htcondor job)"
)

POOL_PROBE_S = 30.0
LIVE_WAIT_S = 300.0
PILOTS_BATCH = 'regexp("^graphed-pilots-", JobBatchName)'
HISTORY_ATTRS = ["ClusterId", "ProcId", "ExitCode", "JobMaxRetries", "OnExitRemove", "NumJobStarts"]


def require_pool() -> Any:
    """The schedd of a reachable pool, or a failure naming what is missing."""
    ads = run_bounded(
        lambda: htcondor2.Collector().query(htcondor2.AdType.Schedd, projection=["Name"]), POOL_PROBE_S
    )
    assert ads, "htcondor2 is installed but the collector lists no schedd: start a personal HTCondor"
    return htcondor2.Schedd()


def in_queue(schedd: Any, cluster: int) -> list[Any]:
    return list(schedd.query(constraint=f"ClusterId == {cluster}", projection=["ProcId"]))


def history(schedd: Any, constraint: str) -> list[Any]:
    return list(schedd.history(constraint=constraint, projection=HISTORY_ATTRS))


def driver_row(schedd: Any, cluster: int) -> Any:
    constraint = f"ClusterId == {cluster}"
    wait_for(lambda: not in_queue(schedd, cluster) and bool(history(schedd, constraint)), 90.0)
    assert not in_queue(schedd, cluster)
    (row,) = history(schedd, constraint)
    return row


def submit_and_wait(tmp_path: Path, plan: Any, **kwargs: Any) -> tuple[Any, Path]:
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    handle = htcondor_api().submit_driverless(
        plan, site="generic", request_memory_mb=1024, log_dir=log_dir, user_modules=[HARNESS_FILE], **kwargs
    )
    run_bounded(lambda: handle.wait(timeout=LIVE_WAIT_S, poll_s=2), LIVE_WAIT_S + 60)
    return handle, log_dir


def test_fat_slot_run_and_its_retry_policy(tmp_path: Path) -> None:
    schedd = require_pool()
    handle, log_dir = submit_and_wait(
        tmp_path, job_plan(8, "m67livea"), n_pilots=2, pilots="local", min_pilots=2
    )
    value = handle.result().value
    assert value.text == expected_concat(8, "m67livea")
    assert len({pid for pid, _cluster, _url in value.sites}) >= 2, value.sites
    assert {cluster for _pid, cluster, _url in value.sites} == {str(handle.cluster)}, value.sites
    assert (log_dir / "driver.log").is_file(), sorted(p.name for p in log_dir.iterdir())
    row = driver_row(schedd, handle.cluster)
    assert row["ExitCode"] == 0
    assert row["JobMaxRetries"] == 2
    assert re.search(r"ExitCode\s*(==|=\?=|is)\s*3\b", str(row["OnExitRemove"])), row["OnExitRemove"]


def test_driver_job_submits_its_own_pilots(tmp_path: Path) -> None:
    schedd = require_pool()
    handle, _ = submit_and_wait(tmp_path, job_plan(8, "m67liveb"), n_pilots=2, pilots="condor", min_pilots=2)
    value = handle.result().value
    assert value.text == expected_concat(8, "m67liveb")
    assert len({pid for pid, _cluster, _url in value.sites}) >= 2, value.sites
    pilot_clusters = {cluster for _pid, cluster, _url in value.sites}
    assert pilot_clusters and "" not in pilot_clusters and str(handle.cluster) not in pilot_clusters, (
        value.sites
    )
    assert driver_row(schedd, handle.cluster)["ExitCode"] == 0
    rows = [row for row in history(schedd, PILOTS_BATCH) if str(row["ClusterId"]) in pilot_clusters]
    assert len(rows) + 1 >= 3, rows


def test_a_plan_error_is_not_retried(tmp_path: Path) -> None:
    schedd = require_pool()
    handle, _ = submit_and_wait(tmp_path, stage_error_plan(4, "m67lived"), n_pilots=1, pilots="local")
    assert handle.status() == "failed"
    with pytest.raises(BaseException) as excinfo:
        handle.result()
    assert_intact_stage_error(excinfo.value)
    row = driver_row(schedd, handle.cluster)
    assert (row["ExitCode"], row["NumJobStarts"]) == (3, 1)
