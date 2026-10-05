"""m69b: servers that each fit the pool's largest slot (plan-services.md §5.2 "Ordering"), on the pool (the
``test-htcondor`` job): two of a set that fit it together both serve a first plan; of two that do not, the
later is refused naming the earlier, which keeps its slot to the run's end, and is removed before it runs;
two plans run from two threads, whose servers fit it one plan at a time, both end with their values. The
servers are sized from the slot, as ``test_service_order.py`` sizes its pilots."""

from __future__ import annotations

import dataclasses
import importlib
import sys
import threading
import time
from pathlib import Path
from typing import Any

from graphed.services import ServiceSpec

from graphed_executors.htcondor_backend import htcondor_runner
from graphed_executors.submit import recipes
from graphed_executors.submit.services import ServiceUnavailable

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "frozen" / "m69b"))
order = importlib.import_module("m69b_order")
harness = importlib.import_module("services_harness")

PLAN_S = 240.0  # a server that cannot start beside its set's earlier one waits for a slot unbounded
GONE_S = 60.0
SERVICE_JOBS = 'regexp("^graphed-service-", JobBatchName)'
RUN_JOBS = 'regexp("^graphed-(service|pilots)-", JobBatchName)'


def server(name: str, memory_mb: int) -> ServiceSpec:
    spec = recipes.http_server(name)
    assert spec.launch is not None
    launch = dataclasses.replace(spec.launch, resources={"memory_mb": memory_mb})
    return dataclasses.replace(spec, launch=launch, timeout_s=120.0)


def two_server_plan(tag: str, names: tuple[str, str], size: int) -> Any:
    return dataclasses.replace(harness.plain_plan(1, tag), services=tuple(server(n, size) for n in names))


def pool_runner(tmp_path: Path) -> Any:
    return harness.run_bounded(
        lambda: htcondor_runner(
            n_pilots=1,
            site="generic",
            service_hosts=("cluster",),
            log_dir=tmp_path / "logs",
            user_modules=[harness.HARNESS_FILE],
            request_memory_mb=512,
        ),
        PLAN_S,
    )


def run_two(tmp_path: Path, share: float, tag: str) -> tuple[Any, int, Any]:
    """Run a one-task plan over two servers of ``share`` of the largest slot each; the schedd, the run's
    start (a ``QDate``), and its value or the exception it raised."""
    htc = order.require_pool()
    plan = two_server_plan(tag, ("web1", "web2"), int(order.largest_slot_mb(htc) * share))
    runner = pool_runner(tmp_path)
    t0 = int(time.time())
    outcome: Any
    with order.closing(runner, PLAN_S):
        try:
            outcome = harness.run_bounded(lambda: runner.run(plan).value, PLAN_S)
        except ServiceUnavailable as exc:
            outcome = exc
    return htc.Schedd(), t0, outcome


def servers(schedd: Any, t0: int) -> list[int]:
    """The run's service jobs' clusters, in submit order."""
    ads = schedd.history(f"{SERVICE_JOBS} && QDate >= {t0}", ["ClusterId"], match=10)
    return sorted(int(ad["ClusterId"]) for ad in ads)


def test_two_servers_that_fit_the_slot_together_both_serve_the_run(tmp_path: Path) -> None:
    tag = "m69b-sibling-fit"
    schedd, t0, value = run_two(tmp_path, 3 / 8, tag)
    assert value == (f"mem://{tag}/0",), value
    assert harness.wait_for(lambda: not order.queued(schedd, f"{RUN_JOBS} && QDate >= {t0}"), GONE_S)
    web1, web2 = servers(schedd, t0)
    both = f"ClusterId == {web1} || ClusterId == {web2}"
    assert harness.wait_for(lambda: order.unrun(schedd, both) == [(1, True), (1, True)], GONE_S)


def test_a_server_that_fits_only_where_its_set_s_earlier_one_runs_is_refused_naming_it(
    tmp_path: Path,
) -> None:
    schedd, t0, refused = run_two(tmp_path, 9 / 16, "m69b-sibling-over")
    assert isinstance(refused, ServiceUnavailable) and refused.name == "web2", refused
    assert harness.wait_for(lambda: not order.queued(schedd, f"{RUN_JOBS} && QDate >= {t0}"), GONE_S)
    web1, web2 = servers(schedd, t0)
    managed = refused.legs["managed"]
    assert f"'web1' (cluster {web1}, whose slot stays taken until the run ends)" in managed, managed
    assert harness.wait_for(lambda: order.unrun(schedd, f"ClusterId == {web1}") == [(1, True)], GONE_S)
    assert harness.wait_for(lambda: order.unrun(schedd, f"ClusterId == {web2}") == [(0, False)], GONE_S)


def test_two_plans_whose_servers_fit_the_slot_one_plan_at_a_time_both_end_run_from_two_threads(
    tmp_path: Path,
) -> None:
    htc = order.require_pool()
    schedd = htc.Schedd()
    size = int(order.largest_slot_mb(htc) * 3 / 8)
    plans = {tag: two_server_plan(f"m69b-overlap-{tag}", (f"{tag}1", f"{tag}2"), size) for tag in ("x", "y")}
    runner = pool_runner(tmp_path)
    t0 = int(time.time())
    ended: dict[str, Any] = {}

    def run(tag: str) -> None:
        try:
            ended[tag] = runner.run(plans[tag]).value
        except Exception as exc:
            ended[tag] = exc

    threads = [threading.Thread(target=run, args=(tag,), daemon=True) for tag in plans]
    with order.closing(runner, PLAN_S):
        for thread in threads:
            thread.start()
        deadline = time.monotonic() + PLAN_S
        for thread in threads:
            thread.join(max(0.0, deadline - time.monotonic()))
        assert not any(t.is_alive() for t in threads), f"the plans waited on each other: {ended}"
    assert ended == {tag: (f"mem://m69b-overlap-{tag}/0",) for tag in plans}, ended
    assert harness.wait_for(lambda: not order.queued(schedd, f"{RUN_JOBS} && QDate >= {t0}"), GONE_S)
