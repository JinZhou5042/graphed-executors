"""m69b: a run's service jobs are submitted before its pilots (plan-services.md §5.2 "Ordering", the
``test_service_order.py`` rows).

The recorder rows run on every OS over m68a's bindings recorder (``m69b_order.OrderSchedd``): real pilot
processes dial the task server once their submit is recorded, and each service job's signed announce is
posted by the recorder. The pool rows need the ``htcondor2`` bindings and a pool (the ``test-htcondor``
job), gated as ``test_histserv_cluster.py`` gates them.
"""

from __future__ import annotations

import contextlib
import dataclasses
import json
import re
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

import pytest
from graphed.core import SequentialRunner
from m69b_harness import HARNESS_DIR, HARNESS_FILE, histserv_api, same_values, served_plan, unique
from m69b_order import (
    IDENTITY,
    Event,
    OrderSchedd,
    acts,
    between,
    closing,
    largest_slot_mb,
    of_kind,
    order_bindings,
    order_plan,
    queued,
    require_pool,
    twin,
    unrun,
)
from services_harness import (
    FAKE_POOL,
    FAKE_SCHEDD,
    CountingHTTPServer,
    backend_api,
    closed_port,
    hosted_spec,
    htcondor_api,
    launch_api,
    port_free,
    run_bounded,
    server_api,
    submits,
    wait_for,
)

from graphed_executors.submit.services import ServiceUnavailable

BOUND_S = 120.0
LIVE_S = 600.0
PLAN_S = 240.0  # a plan whose server cannot start beside its runner's pilots never ends: bounded here
GONE_S = 60.0
PILOT_LATE_S = 8.0
CLOSE_POLL_S = 0.5
LEAVE_S = 30.0
GONE = 4  # a service job answering JobStatus 4 ends any wait for it
CHILD_POLL_S = 5.0
SERVER_MB = 2048
QUANTUM_MB = 128
SMALL = {"h": 8}
RUN_JOBS = 'regexp("^graphed-(service|pilots)-", JobBatchName)'
SERVICE_JOBS = 'regexp("^graphed-service-", JobBatchName)'
PILOT_JOBS = 'regexp("^graphed-pilots-", JobBatchName)'
CHILD = str(Path(HARNESS_DIR) / "m69b_order_child.py")

posix = pytest.mark.skipif(sys.platform == "win32", reason="SIGINT to a child is POSIX only")


class LeftTheBlock(Exception):
    pass


def cluster_runner(log_dir: Path, **kwargs: Any) -> Any:
    return run_bounded(
        lambda: backend_api().htcondor_runner(
            n_pilots=1,
            site="generic",
            service_hosts=("cluster",),
            log_dir=log_dir,
            host="127.0.0.1",
            **kwargs,
        ),
        BOUND_S,
    )


def holds_named(event: Event, pilots: int) -> bool:
    return re.search(rf"ClusterId\s*==\s*{pilots}\b", event.text) is not None


def test_service_jobs_go_before_the_pilots_and_later_plans_hold_them(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with CountingHTTPServer() as service:
        schedd = OrderSchedd(service.port, pilots=True)
        order_bindings(monkeypatch, schedd)
        plans = [
            order_plan("m69b-order-1", "web"),
            order_plan("m69b-order-2", "web"),
            order_plan("m69b-order-3", "web", "web2"),
        ]
        runner = cluster_runner(tmp_path / "logs")
        values: list[Any] = []
        try:
            assert submits(schedd.log) == [], "a schedd.submit before any worker was needed"
            for i, plan in enumerate(plans, 1):
                schedd.mark(f"plan {i}")
                if i < 3:
                    values.append(run_bounded(lambda p=plan: runner.run(p), BOUND_S).value)
                else:
                    values.append(run_bounded(lambda p=plan: runner.submit(p).result(BOUND_S), BOUND_S).value)
            schedd.mark("end")
        finally:
            run_bounded(runner.close, BOUND_S)
            schedd.stop_pilots()
        first = between(schedd.events, "plan 1", "plan 2")
        kinds = [e.kind for e in first if e.kind in ("submit-service", "announce", "submit-pilots")]
        assert kinds == ["submit-service", "announce", "submit-pilots"], first
        assert acts(first, "Hold") == [] and acts(first, "Release") == [], first
        (pilots,) = of_kind(first, "submit-pilots")
        assert pilots.cluster is not None
        for i, stop in ((2, "plan 3"), (3, "end")):
            later = between(schedd.events, f"plan {i}", stop)
            assert of_kind(later, "submit-pilots") == [], f"plan {i} submitted pilots: {later}"
            services = of_kind(later, "submit-service")
            assert len(services) == len(plans[i - 1].services), later
            holds, releases = acts(later, "Hold"), acts(later, "Release")
            assert len(holds) == 1 and len(releases) == 1, f"plan {i}: holds {holds}, releases {releases}"
            hold, release = holds[0], releases[0]
            assert holds_named(hold, pilots.cluster) and re.search(r"JobStatus\s*==\s*1\b", hold.text), hold
            assert hold.reason, f"the pilots were held without a reason: {hold}"
            assert holds_named(release, pilots.cluster) and hold.reason in release.text, (hold, release)
            assert later.index(hold) < later.index(services[0]), (
                f"plan {i} held its pilots after its service submit: {later}"
            )
            assert later.index(release) > later.index(of_kind(later, "announce")[-1]), (
                f"plan {i} released before its announce: {later}"
            )
        for plan, value in zip(plans, values, strict=True):
            assert value == twin(plan, service.endpoint())


def test_the_first_need_waits_for_pilots_past_a_service_s_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with CountingHTTPServer() as service:
        schedd = OrderSchedd(service.port, pilots=True, pilot_after_s=PILOT_LATE_S)
        order_bindings(monkeypatch, schedd)
        plan = order_plan("m69b-order-late", "web", timeout_s=5.0)
        runner = cluster_runner(tmp_path / "logs")
        try:
            assert submits(schedd.log) == [], "a schedd.submit before any worker was needed"
            value = run_bounded(lambda: runner.run(plan), BOUND_S).value
        finally:
            run_bounded(runner.close, BOUND_S)
            schedd.stop_pilots()
        (announced,) = of_kind(schedd.events, "announce")
        (pilots,) = of_kind(schedd.events, "submit-pilots")
        (started,) = of_kind(schedd.events, "pilot-start")
        assert pilots.t >= announced.t, schedd.events
        assert started.t - pilots.t >= PILOT_LATE_S
        assert value == twin(plan, service.endpoint())


def test_only_a_backend_whose_services_are_jobs_defers_its_pilots(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    hb, launch = backend_api().HTCondorBackend, launch_api()
    with CountingHTTPServer() as service:
        schedd = OrderSchedd(service.port)
        order_bindings(monkeypatch, schedd)
        attached = launch.CondorPilots("generic", log_dir=tmp_path / "a")
        backend = run_bounded(lambda: hb(attached, 2, host="127.0.0.1"), BOUND_S)
        try:
            assert submits(schedd.log) == [], (
                "a backend whose services are jobs submitted pilots at construction"
            )
            run_bounded(lambda: backend.wait_for_pilots(0), BOUND_S)
            assert [(d["JobBatchName"][:15], n) for _, d, n, _ in submits(schedd.log)] == [
                ("graphed-pilots-", 2)
            ]
        finally:
            schedd.finish_pilots()
            run_bounded(backend.close, BOUND_S)

        mark = len(schedd.log)
        dag = launch.CondorPilots("generic", log_dir=tmp_path / "dag", schedd_locate=(FAKE_POOL, FAKE_SCHEDD))
        in_job = htcondor_api().SITES["generic"]
        backend = run_bounded(
            lambda: hb(dag, 1, host="127.0.0.1", in_job=in_job, announced={"web": "svc0"}), BOUND_S
        )
        try:
            assert submits(schedd.log[mark:]) == [], (
                "a driver job's DAG backend submitted pilots at construction"
            )
            secret = backend._server.announce_secret(["svc0"])
            (job_dir := tmp_path / "svc0").mkdir()
            (job_dir / "service.json").write_text(json.dumps({"key": "svc0", "url": backend._server.url}))
            (job_dir / "graphed-secret").write_text(secret.hex())
            assert schedd.announce(job_dir) == 200
            got = run_bounded(lambda: backend.host_service(hosted_spec("web"), "m69b-scope"), BOUND_S)
            assert tuple(got) == (service.endpoint(), IDENTITY, "svc0")
            assert submits(schedd.log[mark:]) == [], "the announced service submitted a job"
        finally:
            schedd.finish_pilots()
            run_bounded(backend.close, BOUND_S)

        mark = len(schedd.log)
        driver_only = launch.CondorPilots("generic", log_dir=tmp_path / "driver")
        backend = run_bounded(
            lambda: hb(driver_only, 1, host="127.0.0.1", service_hosts=("driver",)), BOUND_S
        )
        try:
            assert [d["JobBatchName"][:15] for _, d, _, _ in submits(schedd.log[mark:])] == [
                "graphed-pilots-"
            ]
        finally:
            schedd.finish_pilots()
            run_bounded(backend.close, BOUND_S)

    local = launch.LocalPilots()
    backend = run_bounded(lambda: hb(local, 1, host="127.0.0.1"), BOUND_S)
    try:
        assert local.alive() == 1, "LocalPilots did not start at construction"
    finally:
        run_bounded(backend.close, BOUND_S)


def test_a_deferred_spool_failure_removes_its_cluster_and_close_drops_the_secret(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    hb, launch = backend_api().HTCondorBackend, launch_api()
    schedd = OrderSchedd(0, spool_raises=RuntimeError("spool: connection reset (injected)"))
    order_bindings(monkeypatch, schedd)
    spooled = dataclasses.replace(htcondor_api().SITES["generic"], spool=True)
    pilots = launch.CondorPilots(spooled, log_dir=tmp_path / "pilots")
    port = closed_port()
    try:
        backend = run_bounded(lambda: hb(pilots, 2, host="127.0.0.1", port_range=(port, port)), BOUND_S)
    except RuntimeError as exc:
        pytest.fail(f"the pilots were submitted at construction, before any worker was needed: {exc}")
    try:
        with pytest.raises(RuntimeError, match="spool"):
            run_bounded(backend.n_workers, BOUND_S)
        (submitted,) = of_kind(schedd.events, "submit-pilots")
        removes = acts(schedd.events, "Remove")
        assert [e.cluster for e in removes] == [submitted.cluster], schedd.events
    finally:
        run_bounded(backend.close, BOUND_S)
    assert port_free(port)
    assert not (tmp_path / "pilots" / "graphed-secret").exists(), "close() left the pilots' secret"

    idle = OrderSchedd(0)
    order_bindings(monkeypatch, idle)
    unused = launch.CondorPilots("generic", log_dir=tmp_path / "unused")
    backend = run_bounded(lambda: hb(unused, 2, host="127.0.0.1"), BOUND_S)
    assert (tmp_path / "unused" / "graphed-secret").is_file()
    run_bounded(backend.close, BOUND_S)
    assert [e for e in idle.log if e[0] == "act"] == [], idle.log
    assert not (tmp_path / "unused" / "graphed-secret").exists(), "close() left the pilots' secret"


def test_close_waits_for_a_waiting_server_and_an_error_in_the_block_ends_the_wait(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(server_api(), "POLL_S", CLOSE_POLL_S)
    forgotten: list[str] = []
    task_server = server_api().TaskServer
    forget = task_server.forget_announce

    def spy_forget(self: Any, keys: list[str]) -> None:
        forgotten.extend(keys)
        forget(self, keys)

    monkeypatch.setattr(task_server, "forget_announce", spy_forget)
    with CountingHTTPServer() as service:
        plan = order_plan("m69b-order-close", "web")
        schedd = OrderSchedd(service.port, pilots=True, announce_after_s=None, service_status=1)
        order_bindings(monkeypatch, schedd)
        runner = cluster_runner(tmp_path / "close")
        closer = threading.Thread(target=runner.close, daemon=True)
        try:
            future = runner.submit(plan)
            assert wait_for(lambda: schedd.service_dirs() != [], BOUND_S), "the plan submitted no service job"
            closer.start()
            closer.join(3.0)
            assert closer.is_alive(), "close() returned while its plan's server waited for a slot"
            assert acts(schedd.events, "Remove") == [], schedd.events
            schedd.service_status = 2
            (job_dir,) = schedd.service_dirs()
            assert schedd.announce(job_dir) == 200
            closer.join(BOUND_S)
            assert not closer.is_alive(), "close() did not return once the server announced"
            assert future.result(0).value == twin(plan, service.endpoint())
        finally:
            schedd.service_status = GONE
            if not closer.ident:
                run_bounded(runner.close, BOUND_S)
            schedd.stop_pilots()

        schedd = OrderSchedd(service.port, pilots=True, announce_after_s=None, service_status=1)
        order_bindings(monkeypatch, schedd)
        futures: list[Any] = []
        raised_at: list[float] = []

        def leave_with_an_error() -> None:
            with cluster_runner(tmp_path / "error") as runner:
                futures.append(runner.submit(plan))
                assert wait_for(lambda: schedd.service_dirs() != [], BOUND_S), (
                    "the plan submitted no service job"
                )
                raised_at.append(time.monotonic())
                raise LeftTheBlock

        try:
            with pytest.raises(LeftTheBlock):
                run_bounded(leave_with_an_error, LEAVE_S)
            took = time.monotonic() - raised_at[0]
        finally:
            schedd.service_status = GONE
            schedd.stop_pilots()
        assert took <= CLOSE_POLL_S + 2.0, f"the block took {took:.1f}s to leave"
        (job_dir,) = schedd.service_dirs()
        key = job_dir.name.removeprefix("service-")
        error = futures[0].exception(0)
        assert error is not None and key in str(error), error
        (submitted,) = of_kind(schedd.events, "submit-service")
        assert submitted.cluster in [e.cluster for e in acts(schedd.events, "Remove")], schedd.events
        assert key in forgotten, forgotten


# ---- the pool ------------------------------------------------------------------------------------------


def live_runner(tmp_path: Path, n_pilots: int, **kwargs: Any) -> Any:
    return run_bounded(
        lambda: backend_api().htcondor_runner(
            n_pilots=n_pilots,
            site="generic",
            service_hosts=("cluster",),
            log_dir=tmp_path / "logs",
            user_modules=[HARNESS_FILE],
            **kwargs,
        ),
        LIVE_S,
    )


def since(t0: int, jobs: str = RUN_JOBS) -> str:
    return f"{jobs} && QDate >= {t0}"


def test_close_at_once_finishes_a_plan_on_a_free_slot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    htc = require_pool()
    schedd = htc.Schedd()
    monkeypatch.setattr(server_api(), "POLL_S", 2.0)
    ctx = histserv_api().Context(memory_mb=256, workers=1, name=unique("m69b-order-free"))
    plan = served_plan(SMALL, ctx)
    twin_value = SequentialRunner().run(served_plan(SMALL)).value
    runner = live_runner(tmp_path, 1)
    t0 = int(time.time())
    try:
        future = runner.submit(plan)
    finally:
        run_bounded(runner.close, LIVE_S)
    assert future.exception(0) is None, f"close() ended the plan: {future.exception(0)!r}"
    assert same_values(future.result(0).value, twin_value)
    jobs = since(t0, SERVICE_JOBS)

    def starts() -> list[Any]:
        return [ad.get("NumJobStarts") for ad in schedd.history(jobs, ["NumJobStarts"], match=10)]

    assert wait_for(lambda: starts() == [1], GONE_S), starts()
    assert wait_for(lambda: not queued(schedd, since(t0)), GONE_S), queued(schedd, since(t0))


def pilot_mb(total_mb: int) -> int:
    """One quantum above half of what the largest slot leaves beside the server: one pilot fits beside
    the server, two do not, and two fit once the server is gone."""
    half = (total_mb - SERVER_MB) / 2
    return -(-int(half) // QUANTUM_MB) * QUANTUM_MB + QUANTUM_MB


def names_cluster(text: str, cluster: int) -> bool:
    return re.search(rf"(?i)cluster\D{{0,16}}(?<!\d){cluster}(?!\d)", text) is not None


def test_a_later_server_is_refused_where_only_its_runner_s_pilots_hold_room(tmp_path: Path) -> None:
    htc = require_pool()
    schedd = htc.Schedd()
    sized = pilot_mb(largest_slot_mb(htc))

    def plan_of(name: str) -> Any:
        ctx = histserv_api().Context(memory_mb=SERVER_MB, workers=1, timeout_s=20.0, name=unique(name))
        return served_plan(SMALL, ctx)

    twin_value = SequentialRunner().run(served_plan(SMALL)).value
    runner = live_runner(tmp_path, 2, min_pilots=1, request_memory_mb=sized)
    t0 = int(time.time())
    with closing(runner, LIVE_S):
        future = runner.submit(plan_of("m69b-order-first"))
        assert wait_for(future.done, PLAN_S), (
            "plan 1's server never started: its runner's pilots hold the room"
        )
        first = future.result(0).value
        pilots = int(runner.backend.launcher.cluster[1])
        running = f"ClusterId == {pilots} && JobStatus == 2"
        assert wait_for(lambda: len(queued(schedd, running)) == 2, LIVE_S), queued(
            schedd, f"ClusterId == {pilots}"
        )
        t1 = int(time.time())
        with pytest.raises(ServiceUnavailable) as err:
            run_bounded(lambda: runner.run(plan_of("m69b-order-second")), PLAN_S)
        second = since(t1, SERVICE_JOBS)
        assert wait_for(lambda: not queued(schedd, second), GONE_S), "the refused server is still queued"
    assert same_values(first, twin_value)
    text = f"{err.value} {getattr(err.value, 'legs', '')}".replace(str(tmp_path), "")
    assert names_cluster(text, pilots), (pilots, text)
    assert wait_for(lambda: unrun(schedd, second) == [(0, False)], GONE_S), unrun(schedd, second)
    assert wait_for(lambda: not queued(schedd, since(t0)), GONE_S), queued(schedd, since(t0))
    (server,) = schedd.history(
        f"{since(t0, SERVICE_JOBS)} && QDate < {t1}", ["JobCurrentStartDate"], match=10
    )
    qdates = [int(ad["QDate"]) for ad in schedd.history(f"ClusterId == {pilots}", ["QDate"], match=10)]
    assert len(qdates) == 2 and int(server["JobCurrentStartDate"]) <= min(qdates), (dict(server), qdates)


@posix
@pytest.mark.parametrize("form", ["run", "result", "close"])
def test_ctrl_c_removes_a_waiting_server_and_submits_no_pilot(form: str, tmp_path: Path) -> None:
    htc = require_pool()
    schedd = htc.Schedd()
    hold = largest_slot_mb(htc) - SERVER_MB // 2
    blocker = int(
        schedd.submit(
            htc.Submit(
                {
                    "executable": "/bin/sleep",
                    "arguments": "900",
                    "request_memory": str(hold),
                    "request_cpus": "1",
                    "JobBatchName": "m69b-order-blocker",
                }
            )
        ).cluster()
    )
    child: subprocess.Popen[str] | None = None
    output: list[str] = []
    try:
        mine = f"ClusterId == {blocker}"
        assert wait_for(lambda: [ad["JobStatus"] for ad in queued(schedd, mine)] == [2], LIVE_S)
        t0 = int(time.time())
        argv = [
            sys.executable,
            "-u",
            CHILD,
            form,
            str(tmp_path / "logs"),
            str(SERVER_MB),
            str(CHILD_POLL_S),
            "512",
        ]
        child = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        waiting = threading.Event()

        def read(stream: Any) -> None:
            for line in stream:
                output.append(line)
                if "waits for a slot" in line:
                    waiting.set()

        threading.Thread(target=read, args=(child.stdout,), daemon=True).start()
        wait_for(lambda: waiting.is_set() or child.poll() is not None, LIVE_S)
        if waiting.is_set():
            child.send_signal(signal.SIGINT)
            with contextlib.suppress(subprocess.TimeoutExpired):
                child.wait(3 * CHILD_POLL_S)
        pilots = since(t0, PILOT_JOBS)
        submitted = queued(schedd, pilots) + list(schedd.history(pilots, ["ClusterId"], match=10))
        assert submitted == [], "the child submitted a pilot job"
        assert waiting.is_set(), (
            f"the {form} child never waited for a slot ({child.poll()}): {''.join(output)}"
        )
        assert child.poll() is not None, f"the {form} child still ran {3 * CHILD_POLL_S}s after SIGINT"
        assert wait_for(lambda: not queued(schedd, since(t0)), GONE_S), queued(schedd, since(t0))
        services = since(t0, SERVICE_JOBS)
        assert wait_for(lambda: unrun(schedd, services) == [(0, False)], GONE_S), unrun(schedd, services)
    finally:
        if child is not None and child.poll() is None:
            child.kill()
            child.wait(30.0)
        schedd.act(htc.JobAction.Remove, f"ClusterId == {blocker}")
