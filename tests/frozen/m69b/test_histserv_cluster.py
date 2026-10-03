"""m69b: histserv servers placed by ``htcondor_runner(service_hosts=...)`` on a real pool
(plan-services.md, the m69b ``test_histserv_cluster.py`` row, with "Placement" and "Schedulability").

The live tests need the ``htcondor2`` bindings and a pool (the ``test-htcondor`` job's personal HTCondor),
gated as ``tests/frozen/m68b/test_cluster_services_live.py`` gates: skipped where the bindings are not
installed, and, where they are, a missing schedd is a failure naming what is missing. Fills carry dyadic
weights, so pool pilots filling concurrently still sum exactly. The narrowing refusal needs no pool.
"""

from __future__ import annotations

import dataclasses
import logging
import re
import time
from pathlib import Path
from typing import Any

import pytest
from graphed.core import SequentialRunner
from m69b_harness import (
    HARNESS_FILE,
    MIB,
    histserv_api,
    predicted,
    require_histserv,
    run_bounded,
    same_values,
    served_plan,
    unique,
)
from services_harness import backend_api, htcondor_api, launch_api, status_records, wait_for

from graphed_executors.submit.services import ServiceUnavailable

POOL_PROBE_S = 30.0
LIVE_S = 600.0
GONE_S = 60.0  # job_max_vacate_time, plus the schedd's own pace
PENDING_S = 35.0
PILOTS = 2
SERVICE_BATCH = 'regexp("^graphed-service-", JobBatchName)'
#: a 4 MiB slot and a 1 MiB slot (Weight, flow included)
TWO = {"big": 2**18 - 2, "small": 2**16 - 2}
SMALL = {"h": 8}
ONE_SIZE_MB = 256
WAITING_SIZE_MB = 2048
PILOT_MB = 1024


def require_pool() -> Any:
    """The bindings and a schedd of a reachable pool, or a skip (no bindings) / failure (no pool)."""
    htcondor2: Any = pytest.importorskip(
        "htcondor2",
        reason="the htcondor bindings are not installed (Linux wheels only; the test-htcondor job)",
    )
    try:
        ads = run_bounded(
            lambda: htcondor2.Collector().query(htcondor2.AdType.Schedd, projection=["Name"]), POOL_PROBE_S
        )
    except Exception as exc:
        pytest.fail(f"htcondor2 is installed but no pool answers ({exc}): start a personal HTCondor")
    assert ads, "htcondor2 is installed but the collector lists no schedd: start a personal HTCondor"
    require_histserv()
    return htcondor2


def live_runner(tmp_path: Path, hosts: tuple[str, ...] | None, n_pilots: int = PILOTS, **kwargs: Any) -> Any:
    narrowed = {} if hosts is None else {"service_hosts": hosts}
    return run_bounded(
        lambda: backend_api().htcondor_runner(
            n_pilots=n_pilots,
            site="generic",
            log_dir=tmp_path / "logs",
            user_modules=[HARNESS_FILE],
            min_pilots=n_pilots,
            **narrowed,
            **kwargs,
        ),
        LIVE_S,
    )


def services_since(t0: int) -> str:
    return f"{SERVICE_BATCH} && QDate >= {t0}"


def queued(schedd: Any, constraint: str) -> list[Any]:
    return list(schedd.query(constraint=constraint, projection=["ClusterId", "JobStatus", "JobBatchName"]))


def history(schedd: Any, constraint: str) -> list[Any]:
    attrs = ["ClusterId", "JobStatus", "JobBatchName", "RequestMemory", "JobCurrentStartDate"]
    return list(schedd.history(constraint, attrs, match=10))


def named(number: int, text: str, *cut: str) -> bool:
    """``number`` stands alone in ``text`` once ``cut`` (the run's own paths) is removed from it."""
    for ident in cut:
        text = text.replace(ident, "")
    return re.search(rf"(?<!\d){number}(?!\d)", text) is not None


def two_sizes(workers: int) -> tuple[int, int]:
    """Sizes ``s < L`` (MiB): the small slot alone fits ``s``, the big one needs ``L``, and the two
    together exceed ``L``, so a context offering both opens one server of each."""
    big = predicted(TWO, ("big",), workers)
    small = predicted(TWO, ("small",), workers)
    both = predicted(TWO, ("big", "small"), workers)
    s, large = -(-small // MIB), -(-big // MIB)
    assert s * MIB < big and large * MIB < both, f"fixture: slots too close ({small}, {big}, {both} bytes)"
    return s, large


def run_live(runner: Any, plan: Any, caplog: pytest.LogCaptureFixture) -> tuple[Any, list[Any]]:
    with caplog.at_level(logging.INFO, logger="graphed_executors"):
        result = run_bounded(lambda: runner.run(plan), LIVE_S)
    return result, status_records(caplog.records)


def test_a_two_size_context_opens_one_cluster_job_of_each_size(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    htc = require_pool()
    schedd = htc.Schedd()
    s, large = two_sizes(PILOTS)
    ctx = histserv_api().Context(memory_mb=[s, large], workers=PILOTS, name=unique("m69b-two-sizes"))
    plan = served_plan(TWO, ctx)
    sizes = sorted(int(spec.launch.resources["memory_mb"]) for spec in plan.services)
    assert sizes == [s, large], (sizes, ctx.servers())
    twin = SequentialRunner().run(served_plan(TWO)).value
    runner = live_runner(tmp_path, ("cluster",))
    t0 = int(time.time())
    try:
        result, statuses = run_live(runner, plan, caplog)
        jobs = services_since(t0)
        assert wait_for(lambda: not queued(schedd, jobs), GONE_S), "a server outlived its run"
    finally:
        run_bounded(runner.close, LIVE_S)
    assert sorted(st.name for st in statuses) == sorted(spec.name for spec in plan.services)
    assert {(st.leg, st.host) for st in statuses} == {("managed", "cluster")}
    assert same_values(result.value, twin)
    assert wait_for(lambda: len(history(schedd, jobs)) == 2, GONE_S), history(schedd, jobs)
    assert sorted(int(ad["RequestMemory"]) for ad in history(schedd, jobs)) == sizes


def test_driver_placement_starts_no_cluster_job(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    htc = require_pool()
    schedd = htc.Schedd()
    s, large = two_sizes(PILOTS)
    ctx = histserv_api().Context(memory_mb=[s, large], workers=PILOTS, name=unique("m69b-driver"))
    plan = served_plan(TWO, ctx)
    twin = SequentialRunner().run(served_plan(TWO)).value
    runner = live_runner(tmp_path, ("driver",))
    t0 = int(time.time())
    try:
        assert tuple(runner.backend.service_hosts) == ("driver",)
        result, statuses = run_live(runner, plan, caplog)
    finally:
        run_bounded(runner.close, LIVE_S)
    assert sorted(st.name for st in statuses) == sorted(spec.name for spec in plan.services)
    assert {(st.leg, st.host) for st in statuses} == {("managed", "driver")}
    assert same_values(result.value, twin)
    jobs = services_since(t0)
    assert queued(schedd, jobs) == [] and history(schedd, jobs) == []


def test_a_host_the_profile_does_not_offer_is_refused_before_any_pilot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cluster_only = dataclasses.replace(htcondor_api().SITES["generic"], service_ports=None)
    assert cluster_only.service_hosts == ("cluster",)

    def started(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("pilots were submitted before the refusal")

    monkeypatch.setattr(launch_api().CondorPilots, "start", started)
    with pytest.raises(ValueError) as err:
        backend_api().htcondor_runner(
            n_pilots=1, site=cluster_only, service_hosts=("driver",), log_dir=tmp_path / "logs"
        )
    assert re.search(r"\bcluster\b", str(err.value)), str(err.value)


def test_a_server_the_driver_cannot_hold_falls_to_the_cluster(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    htc = require_pool()
    schedd = htc.Schedd()
    ctx = histserv_api().Context(memory_mb=ONE_SIZE_MB, workers=PILOTS, name=unique("m69b-falls"))
    plan = served_plan(SMALL, ctx)
    twin = SequentialRunner().run(served_plan(SMALL)).value
    runner = live_runner(tmp_path, None)
    t0 = int(time.time())
    try:
        assert tuple(runner.backend.service_hosts) == ("driver", "cluster")
        runner.backend.driver_memory_mb = ONE_SIZE_MB - 1
        result, statuses = run_live(runner, plan, caplog)
    finally:
        run_bounded(runner.close, LIVE_S)
    (status,) = statuses
    assert (status.leg, status.host) == ("managed", "cluster"), status
    run_path = str(tmp_path)
    assert named(ONE_SIZE_MB, status.detail, run_path) and named(ONE_SIZE_MB - 1, status.detail, run_path), (
        status.detail
    )
    assert same_values(result.value, twin)
    jobs = services_since(t0)
    assert wait_for(lambda: len(history(schedd, jobs)) == 1, GONE_S), history(schedd, jobs)
    assert [int(ad["RequestMemory"]) for ad in history(schedd, jobs)] == [ONE_SIZE_MB]


def largest_slot_mb(htc: Any) -> int:
    """The largest slot memory the collector advertises: partitionable ``TotalSlotMemory``, static
    ``Memory``, dynamic slots skipped."""
    ads = htc.Collector().query(htc.AdType.Startd, projection=["SlotType", "TotalSlotMemory", "Memory"])
    sizes = [
        int(ad["TotalSlotMemory"] if ad.get("SlotType") == "Partitionable" else ad["Memory"])
        for ad in ads
        if ad.get("SlotType") != "Dynamic"
    ]
    assert sizes, ads
    return max(sizes)


def unrun(schedd: Any, constraint: str) -> list[tuple[Any, bool]]:
    """``(NumJobStarts, JobCurrentStartDate present)`` of each history ad matching ``constraint``."""
    ads = schedd.history(constraint, ["ClusterId", "NumJobStarts", "JobCurrentStartDate"], match=10)
    return [(ad.get("NumJobStarts"), "JobCurrentStartDate" in ad) for ad in ads]


def refusal_text(exc: BaseException) -> str:
    return f"{exc} {getattr(exc, 'legs', '')}"


def test_a_server_no_slot_can_hold_is_removed_unrun_and_refused(tmp_path: Path) -> None:
    htc = require_pool()
    schedd = htc.Schedd()
    largest = largest_slot_mb(htc)
    ctx = histserv_api().Context(memory_mb=largest + 1024, workers=PILOTS, name=unique("m69b-no-slot"))
    plan = served_plan(SMALL, ctx)
    assert [s[1] for s in ctx.servers()] == [largest + 1024], ctx.servers()
    runner = live_runner(tmp_path, ("cluster",))
    t0 = int(time.time())
    jobs = services_since(t0)
    try:
        with pytest.raises(ServiceUnavailable) as err:
            run_bounded(lambda: runner.run(plan), LIVE_S)
        assert wait_for(lambda: not queued(schedd, jobs), GONE_S), "the refused job is still queued"
    finally:
        run_bounded(runner.close, LIVE_S)
    text = refusal_text(err.value)
    run_path = str(tmp_path)
    assert named(largest + 1024, text, run_path) and named(largest, text, run_path), (largest, text)
    assert wait_for(lambda: unrun(schedd, jobs) == [(0, False)], GONE_S), unrun(schedd, jobs)


def test_a_requirement_no_slot_meets_is_removed_unrun_and_refused(tmp_path: Path) -> None:
    htc = require_pool()
    schedd = htc.Schedd()
    ctx = histserv_api().Context(memory_mb=ONE_SIZE_MB, workers=PILOTS, name=unique("m69b-unmatched"))
    (spec,) = served_plan(SMALL, ctx).services
    runner = live_runner(tmp_path, ("cluster",))
    try:
        run_bounded(runner.wait_for_pilots, LIVE_S)
        launcher = runner.backend.launcher
        # set only now: extra_submit reaches the pilots' submit too
        launcher.extra_submit = {**launcher.extra_submit, "requirements": "(TARGET.OpSysMajorVer == 99)"}
        t0 = int(time.time())
        jobs = services_since(t0)
        with pytest.raises(ServiceUnavailable):
            run_bounded(lambda: runner.backend.host_service(spec, "unmatched"), LIVE_S)
        assert wait_for(lambda: not queued(schedd, jobs), GONE_S), "the refused job is still queued"
    finally:
        run_bounded(runner.close, LIVE_S)
    assert wait_for(lambda: unrun(schedd, jobs) == [(0, False)], GONE_S), unrun(schedd, jobs)


def test_a_server_behind_a_busy_pool_waits_and_its_timeout_counts_from_its_start(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    htc = require_pool()
    schedd = htc.Schedd()
    ctx = histserv_api().Context(
        memory_mb=WAITING_SIZE_MB, workers=1, timeout_s=20.0, name=unique("m69b-waits")
    )
    plan = served_plan(SMALL, ctx)
    twin = SequentialRunner().run(served_plan(SMALL)).value
    runner = live_runner(tmp_path, ("cluster",), n_pilots=1, request_memory_mb=PILOT_MB)
    blocker: int | None = None
    try:
        run_bounded(runner.wait_for_pilots, LIVE_S)
        hold = largest_slot_mb(htc) - PILOT_MB - WAITING_SIZE_MB // 2
        submitted = schedd.submit(
            htc.Submit(
                {
                    "executable": "/bin/sleep",
                    "arguments": "900",
                    "request_memory": str(hold),
                    "request_cpus": "1",
                    "JobBatchName": "m69b-blocker",
                }
            )
        )
        blocker = int(submitted.cluster())
        mine = f"ClusterId == {blocker}"
        assert wait_for(lambda: [ad["JobStatus"] for ad in queued(schedd, mine)] == [2], LIVE_S)
        t0 = int(time.time())
        with caplog.at_level(logging.INFO, logger="graphed_executors"):
            future = runner.submit(plan)
            assert not wait_for(future.done, PENDING_S), "the run ended while its server could not start"
            (job,) = queued(schedd, services_since(t0))
            assert job["JobStatus"] == 1, dict(job)
            key = str(job["JobBatchName"]).removeprefix("graphed-service-")
            lines = [r.getMessage() for r in caplog.records]
            assert any(key in line and re.search(r"JobStatus\W*1\b", line) for line in lines), lines
            removed_at = int(time.time())
            schedd.act(htc.JobAction.Remove, mine)
            blocker = None
            result = future.result(LIVE_S)
    finally:
        if blocker is not None:
            schedd.act(htc.JobAction.Remove, f"ClusterId == {blocker}")
        run_bounded(runner.close, LIVE_S)
    assert same_values(result.value, twin)
    jobs = services_since(t0)
    assert wait_for(lambda: len(history(schedd, jobs)) == 1, GONE_S), history(schedd, jobs)
    (ad,) = history(schedd, jobs)
    assert int(ad["JobCurrentStartDate"]) >= removed_at, (dict(ad), removed_at)
