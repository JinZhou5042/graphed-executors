"""m68a D2/D10 and §3.1: the engine's three-leg service set, run on ``ThreadBackend`` and on a two-host fake
backend (``services_harness.HostFake``), on every OS.

What a submission acquires lives exactly as long as the submission: a managed child, a hosted service, a
probe task, the plan's queued tasks. Every leg witnesses the mechanism: pids reaped (``pid_gone``), ports
free (``port_free``), the recording backend's submits/cancels/task starts, the fake's ``host_service`` /
``release_service`` calls, the spies' ``bind_services``/``resolve_services`` records in one ordered
trace, and the ``ServiceStatus`` on each ``graphed_executors.services`` log record.

Discriminates: bind after submit, a same-host probe pass off the driver's machine, a service, record or
pending task outliving its submission or a warm set's ``with``, a cleanup failure replacing the refusal,
a raising release step skipping a later one, a returned value that needs a released service (a composed
plan's too), an unanswered probe that hangs, a replaced given endpoint, a connect-only probe.

The legs that resolve a value through ``collate`` and ``aggregate_plan`` need graphed's resolve walk
(plan §3.2), which graphed has not merged; they stay red until it lands.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import socket
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any

import pytest
from graphed import collate
from graphed.services import ServiceSpec, UnboundService
from m68a_triton import spy_aggregate_plan
from services_harness import (
    DRIVER_HOST,
    HARNESS_DIR,
    RUN_TIMEOUT_S,
    SERVICE_HOST,
    THIRD_HOST,
    CountingHTTPServer,
    HostFake,
    HostFaults,
    RecordingBackend,
    Resolved,
    SpyProcess,
    SpyReduce,
    bare_spec,
    bounded_set,
    child_spec,
    closed_port,
    closing_bounded,
    concat,
    empty_tuple,
    endpoint_port,
    free_range,
    gpu_spec,
    hold_port,
    hosted_spec,
    http_get,
    is_plan_key,
    is_probe_key,
    local_api,
    pid_gone,
    plain_plan,
    port_free,
    probe_scope,
    read_report,
    run_bounded,
    server_api,
    services_api,
    spy_events,
    spy_plan,
    status_records,
    statuses_named,
    submit_api,
    to_http,
    trace_snapshot,
    wait_for,
    write_machine_ad,
)
from services_harness import ON_CLOSE as ON_CLOSE_MODES

BOUND_S = 120.0


def thread_backend(n: int = 2) -> Any:
    return submit_api().ThreadBackend(n)


def runner_over(backend: Any, **kwargs: Any) -> Any:
    return submit_api().SubmitRunner(backend, **kwargs)


def ports_free(ports: tuple[int, int]) -> bool:
    return all(port_free(p) for p in range(ports[0], ports[1] + 1))


def body_pids(value: Any) -> set[int]:
    """The pids a spy's leaves read from a ``service_child.py`` (its GET answers its pid)."""
    assert isinstance(value, Resolved), value
    return {int(b) for b in value.value}


# ---- the whole path on the generic recipe ----------------------------------------------------------


def test_http_server_recipe_runs_the_whole_path_without_a_service_client(tmp_path: Path) -> None:
    """``recipes.http_server`` through ``SubmitRunner(ThreadBackend)`` in a fresh interpreter: resolve,
    probe, bind, a task GET, resolve the value, close; no Triton or histserv module is imported."""
    out = tmp_path / "whole.json"
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join([HARNESS_DIR, env.get("PYTHONPATH", "")]).rstrip(os.pathsep)
    code = f"import services_harness as h; h.whole_path_main({str(out)!r}, {str(tmp_path)!r})"
    proc = subprocess.run(
        [sys.executable, "-c", code],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=RUN_TIMEOUT_S,
    )
    assert proc.returncode == 0, proc.stderr
    got = json.loads(out.read_text())
    assert got["modules"] == [], got["modules"]
    assert got["endpoint"].startswith("http://"), got["endpoint"]
    assert got["value_is_resolved"] is True
    assert len(got["leaf_bodies"]) == 2 and all(got["leaf_bodies"]), got["leaf_bodies"]
    assert [alive for alive, _n in got["resolves"]] == [True], got["resolves"]
    assert got["port_free_after_run"] is True
    order = got["order"]
    kinds = [kind for kind, _ in order]
    probes = [i for i, (kind, key) in enumerate(order) if kind == "submit" and is_probe_key(key)]
    plans = [i for i, (kind, key) in enumerate(order) if kind == "submit" and is_plan_key(key)]
    (bind,) = [i for i, kind in enumerate(kinds) if kind == "bind"]
    assert probes and plans, order
    assert max(probes) < bind < min(plans), order


# ---- resolution order ------------------------------------------------------------------------------


def test_a_given_endpoint_wins_over_a_site_endpoint(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    with CountingHTTPServer() as user_srv, CountingHTTPServer() as site_srv:
        fake = HostFake(tmp_path / "ads", site_services={"kind-x": site_srv.endpoint()}, hosting=False)
        specs = [bare_spec("svc-1", kind="kind-x"), bare_spec("svc-2", kind="kind-x")]
        plan = spy_plan(SpyProcess("order", service="svc-1"), 2, specs)
        with (
            caplog.at_level(logging.INFO),
            closing_bounded(runner_over(fake, services={"svc-1": user_srv.endpoint()})) as runner,
        ):
            run_bounded(lambda: runner.run(plan), BOUND_S)
        (bind,) = spy_events("order", "bind")
        assert bind[1] == {"svc-1": user_srv.endpoint(), "svc-2": site_srv.endpoint()}
        (one,) = statuses_named(caplog.records, "svc-1")
        (two,) = statuses_named(caplog.records, "svc-2")
        assert (one.leg, one.endpoint, one.identity) == ("user", user_srv.endpoint(), None)
        assert (two.leg, two.endpoint, two.identity) == ("site", site_srv.endpoint(), None)
        assert user_srv.accepts >= 1 and site_srv.accepts >= 1


def test_a_dead_given_endpoint_refuses_naming_user_with_zero_plan_submits(
    tmp_path: Path, children: Any
) -> None:
    services = services_api()
    report = children.report()
    dead = f"http://127.0.0.1:{closed_port()}"
    backend = RecordingBackend(thread_backend())
    plan = spy_plan(SpyProcess("dead-user"), 2, [child_spec("web", report, ports=free_range())])
    with closing_bounded(runner_over(backend, services={"web": dead})) as runner:
        with pytest.raises(services.ServiceUnavailable) as excinfo:
            run_bounded(lambda: runner.run(plan), BOUND_S)
        assert backend.log.plan_keys() == []
    assert excinfo.value.name == "web"
    assert "user" in excinfo.value.legs and dead in str(excinfo.value), excinfo.value
    assert not report.exists(), "a given endpoint was replaced by a managed start"


def test_a_dead_site_endpoint_falls_through_to_managed(
    tmp_path: Path, children: Any, caplog: pytest.LogCaptureFixture
) -> None:
    dead = f"http://127.0.0.1:{closed_port()}"
    report = children.report()
    fake = HostFake(tmp_path / "ads", site_services={"http": dead})
    plan = spy_plan(SpyProcess("dead-site"), 2, [child_spec("web", report, ports=free_range())])
    with caplog.at_level(logging.INFO), closing_bounded(runner_over(fake)) as runner:
        result = run_bounded(lambda: runner.run(plan), BOUND_S)
    (status,) = statuses_named(caplog.records, "web")
    assert (status.leg, status.host) == ("managed", "driver")
    assert dead in str(status.detail), status
    assert body_pids(result.value) == {read_report(report)["pid"]}


def test_no_launch_and_no_endpoint_names_all_three_legs() -> None:
    services = services_api()
    plan = spy_plan(SpyProcess("nothing"), 2, [bare_spec("web")])
    with (
        closing_bounded(runner_over(thread_backend())) as runner,
        pytest.raises(services.ServiceUnavailable) as excinfo,
    ):
        run_bounded(lambda: runner.run(plan), BOUND_S)
    assert set(excinfo.value.legs) == {"user", "site", "managed"}
    assert all(excinfo.value.legs[leg] for leg in ("user", "site", "managed")), excinfo.value.legs
    assert all(leg in str(excinfo.value) for leg in ("user", "site", "managed")), str(excinfo.value)


def test_a_gpu_recipe_without_host_service_is_refused_naming_the_attribute() -> None:
    services = services_api()
    plan = spy_plan(SpyProcess("gpu"), 2, [gpu_spec("web")])
    with (
        closing_bounded(runner_over(thread_backend())) as runner,
        pytest.raises(services.ServiceUnavailable) as excinfo,
    ):
        run_bounded(lambda: runner.run(plan), BOUND_S)
    assert "host_service" in excinfo.value.legs["managed"], excinfo.value.legs


# ---- injected faults: the refusal surfaces, nothing is left ---------------------------------------

FAULTS = (
    "check-never-passes",
    "ready-child-then-refusal",
    "hosted-then-refusal",
    "host-service-raises",
    "probe-refuses",
    "release-raises-then-refusal",
    "port-range-held",
    "probe-submit-worker-lost",
    "probe-unanswered",
)


class FaultCase:
    """One injected fault: its specs, the fake's faults, the managed children's reports and port
    ranges, and the check on the exception that must surface."""

    def __init__(self, fault: str, tmp_path: Path, children: Any) -> None:
        self.fault = fault
        self.injected: BaseException | None = None
        self.release_error: BaseException | None = None
        self.reports: list[Path] = []
        self.ranges: list[tuple[int, int]] = []
        self.held: int | None = None
        faults = HostFaults()
        ready = self._child(children, "a-ready")
        if fault == "check-never-passes":
            self.reports, self.ranges = [], []
            never = self._child(children, "a-never", mode="never", timeout_s=2.0)
            self.specs = [never]
        elif fault == "ready-child-then-refusal":
            self.specs = [ready, bare_spec("b-missing")]
        elif fault == "hosted-then-refusal":
            self.reports, self.ranges = [], []
            self.specs = [hosted_spec("a-hosted"), bare_spec("b-missing")]
        elif fault == "host-service-raises":
            self.injected = faults.host_raises = RuntimeError("host_service fault injected")
            self.specs = [ready, hosted_spec("b-hosted")]
        elif fault == "probe-refuses":
            faults.dead_hosted = True
            self.specs = [ready, hosted_spec("b-hosted")]
        elif fault == "release-raises-then-refusal":
            self.release_error = faults.release_raises = RuntimeError("release_service fault injected")
            self.specs = [ready, hosted_spec("b-hosted"), bare_spec("c-missing")]
        elif fault == "port-range-held":
            self.held = closed_port()
            held = child_spec("b-held", children.report("b-held"), ports=(self.held, self.held))
            self.specs = [ready, held]
        elif fault == "probe-submit-worker-lost":
            self.injected = faults.probe_raises = server_api().WorkerLost("svc-probe", "pilot-lost")
            self.specs = [ready]
        elif fault == "probe-unanswered":
            faults.probe_silent = True
            self.specs = [self._replace_timeout(ready, 2.0)]
        else:  # pragma: no cover - the parametrization names every case
            raise AssertionError(fault)
        self.fake = HostFake(tmp_path / "ads", faults=faults, n_workers=2)

    def _child(
        self, children: Any, name: str, *, mode: str = "serve", timeout_s: float = 30.0
    ) -> ServiceSpec:
        report, ports = children.report(name), free_range()
        self.reports.append(report)
        self.ranges.append(ports)
        return child_spec(name, report, mode=mode, ports=ports, timeout_s=timeout_s)

    @staticmethod
    def _replace_timeout(spec: ServiceSpec, timeout_s: float) -> ServiceSpec:
        return ServiceSpec(spec.name, spec.kind, spec.check, spec.ports, spec.launch, timeout_s)

    def check_raised(self, exc: BaseException) -> None:
        services = services_api()
        if self.injected is not None:
            assert exc is self.injected, repr(exc)
        elif self.fault == "check-never-passes":
            assert isinstance(exc, services.ServiceUnavailable), repr(exc)
            assert exc.name == "a-never" and "http:/" in exc.legs["managed"], exc.legs
        elif self.fault in ("ready-child-then-refusal", "hosted-then-refusal"):
            assert isinstance(exc, services.ServiceUnavailable) and exc.name == "b-missing", repr(exc)
        elif self.fault == "release-raises-then-refusal":
            assert isinstance(exc, services.ServiceUnavailable) and exc.name == "c-missing", repr(exc)
        elif self.fault == "probe-refuses":
            assert isinstance(exc, services.ServiceUnreachable) and exc.name == "b-hosted", repr(exc)
            assert exc.reason, repr(exc)
        elif self.fault == "port-range-held":
            assert isinstance(exc, OSError), repr(exc)
        elif self.fault == "probe-unanswered":
            assert isinstance(exc, services.ServiceUnreachable), repr(exc)
            assert "no worker answered" in exc.reason, exc.reason
        else:  # pragma: no cover
            raise AssertionError(self.fault)

    def check_nothing_left(self, records: list[logging.LogRecord]) -> None:
        for report in self.reports:
            pid = read_report(report)["pid"]
            assert pid_gone(pid), f"managed child {pid} ({report.name}) outlived the refusal"
        for ports in self.ranges:
            assert ports_free(ports), f"a port of {ports} is still bound"
        minted = [key for _scope, key in self.fake.minted]
        assert sorted(self.fake.released_keys()) == sorted(minted), (self.fake.calls, minted)
        if self.release_error is not None:
            text = [r.getMessage() + str(r.exc_info[1] if r.exc_info else "") for r in records]
            assert any("release_service fault injected" in t for t in text), (
                "the raising release was not logged"
            )
        if self.fault == "probe-unanswered":
            probes = self.fake.inner.log.probe_keys()
            assert probes and set(probes) <= set(self.fake.inner.log.cancelled()), (
                probes,
                self.fake.inner.log.cancelled(),
            )
            assert all(f.cancelled() for f in self.fake.silent), "an unanswered probe was left pending"


@pytest.mark.parametrize("entry", ["run", "start"])
@pytest.mark.parametrize("fault", FAULTS)
def test_an_injected_fault_surfaces_and_leaves_nothing(
    fault: str, entry: str, tmp_path: Path, children: Any, caplog: pytest.LogCaptureFixture
) -> None:
    case = FaultCase(fault, tmp_path, children)
    services = services_api()
    runner = runner_over(case.fake)
    plan = spy_plan(SpyProcess(f"fault-{fault}-{entry}", service=None), 2, case.specs)
    try:
        with caplog.at_level(logging.INFO):
            if case.held is not None:
                with hold_port(case.held), pytest.raises(BaseException) as excinfo:
                    call = (
                        (lambda: runner.run(plan))
                        if entry == "run"
                        else services.ServiceSet(case.specs, case.fake).start
                    )
                    run_bounded(call, BOUND_S)
            else:
                with pytest.raises(BaseException) as excinfo:
                    call = (
                        (lambda: runner.run(plan))
                        if entry == "run"
                        else services.ServiceSet(case.specs, case.fake).start
                    )
                    run_bounded(call, BOUND_S)
        assert not isinstance(excinfo.value, AssertionError), excinfo.value
        case.check_raised(excinfo.value)
        case.check_nothing_left(caplog.records)  # the runner is still open
        assert spy_events(f"fault-{fault}-{entry}", "call") == []
    finally:
        run_bounded(runner.close, BOUND_S)


# ---- per-run lifetime -------------------------------------------------------------------------------


def test_a_managed_child_lives_exactly_as_long_as_its_run(children: Any) -> None:
    spec = child_spec("web", children.report(), ports=free_range())
    plan = spy_plan(SpyProcess("per-run"), 2, [spec])
    with closing_bounded(runner_over(thread_backend())) as runner:
        first = run_bounded(lambda: runner.run(plan), BOUND_S)
        (pid1,) = body_pids(first.value)
        children.add(pid1)
        assert pid1 != os.getpid()
        assert pid_gone(pid1), "the run's child outlived the run"
        second = run_bounded(lambda: runner.run(plan), BOUND_S)
        (pid2,) = body_pids(second.value)
        children.add(pid2)
        assert pid2 != pid1
        assert pid_gone(pid2)
        assert ports_free(spec.ports)


def test_two_sequential_runs_on_the_fake_each_release_only_their_own_key(tmp_path: Path) -> None:
    fake = HostFake(tmp_path / "ads")
    plan = spy_plan(SpyProcess("seq", service="web"), 2, [hosted_spec("web")])
    with closing_bounded(runner_over(fake)) as runner:
        run_bounded(lambda: runner.run(plan), BOUND_S)
        ((_, _, scope1),) = fake.host_service_calls()
        assert fake.released_keys() == fake.minted_under(scope1) and len(fake.minted_under(scope1)) == 1
        run_bounded(lambda: runner.run(plan), BOUND_S)
        (_, (_, _, scope2)) = fake.host_service_calls()
        assert scope2 != scope1
        assert fake.released_keys() == fake.minted_under(scope1) + fake.minted_under(scope2)
    nonces = set(fake.inner.log.plan_nonces())
    assert nonces == {scope1, scope2}, nonces


def test_overlapping_submissions_keep_their_own_scope(tmp_path: Path) -> None:
    """A (``run``) and B (``submit``, longer ``timeout_s``) on one runner over the fake, whose single
    worker a gate holds until A raises: A's probe goes unanswered and is cancelled, B completes."""
    services = services_api()
    gate = threading.Event()
    inner = RecordingBackend(thread_backend(1), gate=gate)
    fake = HostFake(tmp_path / "ads", inner=inner)
    plan_a = spy_plan(SpyProcess("overlap-a", service=None), 2, [hosted_spec("svc-a", timeout_s=2.0)])
    plan_b = spy_plan(SpyProcess("overlap-b", service=None), 2, [hosted_spec("svc-b", timeout_s=90.0)])
    runner = runner_over(fake)
    try:
        fut_b = runner.submit(plan_b)
        assert wait_for(lambda: any(c[1] == "svc-b" for c in fake.host_service_calls()), 30.0)
        with pytest.raises(services.ServiceUnreachable) as excinfo:
            run_bounded(lambda: runner.run(plan_a), BOUND_S)
        assert "no worker answered" in excinfo.value.reason
        scope = {name: s for _, name, s in fake.host_service_calls()}
        scope_a, scope_b = scope["svc-a"], scope["svc-b"]
        assert scope_a != scope_b
        assert fake.released_keys() == fake.minted_under(scope_a), "A's end released another run's key"
        probes_a = [k for k in inner.log.probe_keys() if probe_scope(k) == scope_a]
        assert probes_a and set(probes_a) <= set(inner.log.cancelled()), (probes_a, inner.log.cancelled())
        gate.set()
        result_b = fut_b.result(timeout=BOUND_S)
        assert result_b.n_partitions == 2
        inner.drain()
        ran = inner.log.ran_keys()
        assert not set(probes_a) & set(ran), f"A's cancelled probe ran: {ran}"
        events = trace_snapshot()
        cancel_at = max(i for i, e in enumerate(events) if e[0] == "cancel" and set(probes_a) & set(e[1:]))
        b_first = min(i for i, e in enumerate(events) if e[0] == "ran" and scope_b in e[1])
        assert cancel_at < b_first, events
        probes_b = [k for k in inner.log.probe_keys() if probe_scope(k) == scope_b]
        assert probes_b and all(k.startswith(f"svc-{scope_b}-probe-") for k in probes_b)
        b_plan = inner.log.plan_keys()
        assert b_plan and all(scope_b in k and scope_a not in k for k in b_plan), b_plan
        assert set(inner.log.plan_nonces()) == {scope_b}
        assert sorted(fake.released_keys()) == sorted(fake.minted_under(scope_a) + fake.minted_under(scope_b))
    finally:
        gate.set()
        run_bounded(runner.close, BOUND_S)


def test_a_failed_leaf_submit_cancels_the_run_s_queued_leaves() -> None:
    gate = threading.Event()
    fault = OSError("submit refused (injected)")
    backend = RecordingBackend(
        thread_backend(1), gate=gate, fail_key=lambda k: k.endswith("-leaf-2"), fault=fault
    )
    runner = runner_over(backend)
    try:
        with pytest.raises(OSError) as excinfo:
            run_bounded(lambda: runner.run(plain_plan(4, "m68a-leaf-fault")), BOUND_S)
        assert excinfo.value is fault
        queued = [k for k in backend.log.plan_keys() if k.endswith(("-leaf-0", "-leaf-1"))]
        assert len(queued) == 2 and set(queued) <= set(backend.log.cancelled()), backend.log.cancelled()
        gate.set()
        backend.drain()
        assert not set(queued) & set(backend.log.ran_keys()), backend.log.ran_keys()
    finally:
        gate.set()
        run_bounded(runner.close, BOUND_S)


def _enter_two_sets_together(
    services: Any, spec: ServiceSpec, backend: Any, children: Any, trial: int
) -> None:
    start = threading.Barrier(2)
    opened = threading.Barrier(3)
    release = threading.Event()
    got: dict[int, tuple[str, int]] = {}
    errors: list[BaseException] = []

    def enter(i: int) -> None:
        try:
            start.wait(30)
            with services.ServiceSet([spec], backend) as eps:
                endpoint = eps["web"]
                got[i] = (endpoint, children.add(int(http_get(to_http(endpoint) + "/"))))
                opened.wait(60)
                release.wait(60)
        except BaseException as exc:
            errors.append(exc)
            opened.abort()

    threads = [threading.Thread(target=enter, args=(i,), daemon=True) for i in range(2)]
    for t in threads:
        t.start()
    try:
        with contextlib.suppress(threading.BrokenBarrierError):
            opened.wait(90)
        assert not errors, errors
        (ep0, pid0), (ep1, pid1) = got[0], got[1]
        assert endpoint_port(ep0) != endpoint_port(ep1), f"trial {trial}: one port for two sets"
        assert pid0 != pid1 and not pid_gone(pid0) and not pid_gone(pid1), f"trial {trial}: a child is not up"
    finally:
        release.set()
        for t in threads:
            t.join(60)
    assert not any(t.is_alive() for t in threads)
    assert pid_gone(pid0) and pid_gone(pid1), f"trial {trial}: a set's child outlived its with"


def test_two_sets_entered_together_get_distinct_ports(children: Any) -> None:
    services = services_api()
    spec = child_spec("web", children.report(), ports=free_range(6))
    backend = thread_backend(2)
    try:
        for trial in range(10):
            _enter_two_sets_together(services, spec, backend, children, trial)
    finally:
        run_bounded(backend.close, BOUND_S)


def test_a_warm_user_held_set_serves_two_plans_with_one_child(
    children: Any, caplog: pytest.LogCaptureFixture
) -> None:
    services = services_api()
    spec = child_spec("web", children.report(), ports=free_range())
    plan = spy_plan(SpyProcess("warm"), 2, [spec])
    with caplog.at_level(logging.INFO), closing_bounded(runner_over(thread_backend())) as runner:
        with bounded_set(services.ServiceSet([spec], runner.backend)) as eps:
            runner.services = eps
            first = run_bounded(lambda: runner.run(plan), BOUND_S)
            second = run_bounded(lambda: runner.run(plan), BOUND_S)
            (pid,) = body_pids(first.value) | body_pids(second.value)
            children.add(pid)
            assert not pid_gone(pid)
        assert pid_gone(pid), "the warm child outlived its set's with"
        runner.services = None
        legs = [s.leg for s in statuses_named(caplog.records, "web")]
        assert legs == ["managed", "user", "user"], legs
        after = run_bounded(lambda: runner.run(plain_plan(2, "m68a-warm-after")), BOUND_S)
        assert after.n_partitions == 2  # the runner is still open


# ---- bind, probe, resolve ----------------------------------------------------------------------------


def test_bind_comes_after_the_probe_and_before_the_first_plan_task(children: Any) -> None:
    backend = RecordingBackend(thread_backend())
    plan = spy_plan(SpyProcess("bind-order"), 2, [child_spec("web", children.report(), ports=free_range())])
    with closing_bounded(runner_over(backend)) as runner:
        run_bounded(lambda: runner.run(plan), BOUND_S)
    events = trace_snapshot()
    probe_runs = [i for i, e in enumerate(events) if e[0] == "ran" and is_probe_key(e[1])]
    binds = [i for i, e in enumerate(events) if e[0] == "bind" and e[1] == "bind-order"]
    plan_submits = [i for i, e in enumerate(events) if e[0] == "submit" and is_plan_key(e[1])]
    assert probe_runs and len(binds) == 1 and plan_submits, events
    assert max(probe_runs) < binds[0] < min(plan_submits), events
    assert backend.log.answers, "no probe answered before the bind"


def test_resolve_services_runs_once_on_the_run_value_while_the_child_is_up(children: Any) -> None:
    plan = spy_plan(SpyProcess("resolve"), 2, [child_spec("web", children.report(), ports=free_range())])
    with closing_bounded(runner_over(thread_backend())) as runner:
        result = run_bounded(lambda: runner.run(plan), BOUND_S)
    (resolve,) = spy_events("resolve", "resolve")
    _, value, alive, returned = resolve
    assert alive is True, "resolve_services ran after the service was released"
    assert result.value is returned
    (pid,) = {int(b) for b in value}
    assert len(value) == 2 and pid_gone(children.add(pid))


def test_resolve_reaches_a_collated_part(children: Any) -> None:
    """Needs graphed's resolve walk (``_Collated`` forwarding ``resolve_services``, plan §3.2)."""
    spec = child_spec("web", children.report(), ports=free_range())
    plan = collate(
        {"a": spy_plan(SpyProcess("col-a"), 2, [spec]), "b": spy_plan(SpyProcess("col-b"), 2, [spec])}
    )
    with closing_bounded(runner_over(thread_backend())) as runner:
        result = run_bounded(lambda: runner.run(plan), BOUND_S)
    resolved = {}
    for tag in ("col-a", "col-b"):
        calls = spy_events(tag, "resolve")
        assert len(calls) == 1, f"{tag}: resolve_services called {len(calls)} times"
        _, value, alive, returned = calls[0]
        assert alive is True and len(value) == 2, calls
        resolved[tag] = returned
    assert result.value == {"a": resolved["col-a"], "b": resolved["col-b"]}


def test_resolve_reaches_an_aggregate_plan_reduce(children: Any) -> None:
    """Needs graphed's resolve walk (``_PartitionReduce`` forwarding ``resolve_services`` to its
    ``reduce``, plan §3.2)."""
    spec = child_spec("web", children.report(), ports=free_range())
    plan = spy_aggregate_plan(SpyReduce("agg"), spec, concat, empty_tuple)
    assert plan.services == (spec,)
    with closing_bounded(runner_over(thread_backend())) as runner:
        result = run_bounded(lambda: runner.run(plan), BOUND_S)
    calls = spy_events("agg", "resolve")
    assert len(calls) == 1, f"resolve_services called {len(calls)} times"
    _, value, alive, returned = calls[0]
    assert alive is True and len(value) == 2
    assert result.value is returned


def test_an_unbound_plan_is_refused_before_any_process_call() -> None:
    plan = spy_plan(SpyProcess("unbound"), 2, [bare_spec("web")])
    executor = local_api().ThreadExecutor(2)
    try:
        with pytest.raises(UnboundService) as excinfo:
            run_bounded(lambda: executor.run(plan), BOUND_S)
    finally:
        run_bounded(executor.close, BOUND_S)
    assert excinfo.value.name == "web"
    assert spy_events("unbound", "call") == []


def test_on_close_runs_before_teardown(children: Any) -> None:
    ON_CLOSE_MODES["on-close"] = "record"
    plan = spy_plan(SpyProcess("on-close"), 2, [child_spec("web", children.report(), ports=free_range())])
    with closing_bounded(runner_over(thread_backend())) as runner:
        result = run_bounded(lambda: runner.run(plan), BOUND_S)
    assert spy_events("on-close", "no-on_close") == []
    assert spy_events("on-close", "on_close") == [("on_close", True)], spy_events("on-close")
    (pid,) = body_pids(result.value)
    assert pid_gone(children.add(pid))


def test_a_raising_on_close_is_logged_and_the_value_stands(
    children: Any, caplog: pytest.LogCaptureFixture
) -> None:
    ON_CLOSE_MODES["on-close-raise"] = "raise"
    plan = spy_plan(
        SpyProcess("on-close-raise"), 2, [child_spec("web", children.report(), ports=free_range())]
    )
    with caplog.at_level(logging.INFO), closing_bounded(runner_over(thread_backend())) as runner:
        result = run_bounded(lambda: runner.run(plan), BOUND_S)
        (pid,) = body_pids(result.value)
        assert pid_gone(children.add(pid)), "a raising on_close skipped the child's release"
    assert isinstance(result.value, Resolved) and result.value.tag == "on-close-raise"
    assert spy_events("on-close-raise", "on_close") == [("on_close", True)]
    text = [r.getMessage() + str(r.exc_info[1] if r.exc_info else "") for r in caplog.records]
    assert any("on_close fault injected" in t for t in text), "the raising on_close was not logged"


def test_statuses_are_logged_with_leg_and_ready_at(children: Any, caplog: pytest.LogCaptureFixture) -> None:
    plan = spy_plan(SpyProcess("status"), 2, [child_spec("web", children.report(), ports=free_range())])
    with caplog.at_level(logging.INFO), closing_bounded(runner_over(thread_backend())) as runner:
        run_bounded(lambda: runner.run(plan), BOUND_S)
    records = [r for r in caplog.records if hasattr(r, "status")]
    assert records and all(
        r.name == "graphed_executors.services" and r.levelno == logging.INFO for r in records
    )
    (status,) = status_records(caplog.records)
    assert (status.name, status.leg, status.host) == ("web", "managed", "driver")
    assert status.endpoint.startswith("http://127.0.0.1:")
    assert status.started_at is not None and status.ready_at is not None
    assert status.started_at <= status.ready_at


# ---- the probe and host identity -----------------------------------------------------------------------


def test_on_thread_backend_every_probe_answer_carries_the_driver_identity(
    children: Any, caplog: pytest.LogCaptureFixture
) -> None:
    services = services_api()
    backend = RecordingBackend(thread_backend())
    plan = spy_plan(SpyProcess("same-host"), 2, [child_spec("web", children.report(), ports=free_range())])
    with caplog.at_level(logging.INFO), closing_bounded(runner_over(backend)) as runner:
        result = run_bounded(lambda: runner.run(plan), BOUND_S)
    driver = run_bounded(services.host_identity, BOUND_S)
    assert backend.log.answers, "no probe answered"
    assert all(answer[0] == driver for _key, answer in backend.log.answers), backend.log.answers
    (status,) = statuses_named(caplog.records, "web")
    assert status.identity == driver
    assert isinstance(result.value, Resolved)


@pytest.mark.parametrize(
    ("worker_hosts", "n_workers", "passes"),
    [
        ((SERVICE_HOST,), 3, False),
        ((SERVICE_HOST,), 1, False),
        ((DRIVER_HOST,), 3, True),
        ((THIRD_HOST,), 3, True),
        ((SERVICE_HOST, THIRD_HOST), 3, True),
    ],
    ids=[
        "only-the-service-host",
        "only-the-service-host-one-worker",
        "the-driver-host",
        "a-third-host",
        "service-host-then-a-third",
    ],
)
def test_the_two_host_probe_passes_only_off_the_service_host(
    worker_hosts: tuple[str, ...],
    n_workers: int,
    passes: bool,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    services = services_api()
    fake = HostFake(tmp_path / "ads", worker_hosts=worker_hosts, n_workers=n_workers)
    plan = spy_plan(
        SpyProcess(f"two-host-{len(worker_hosts)}-{passes}", service="web"), 2, [hosted_spec("web")]
    )
    with caplog.at_level(logging.INFO), closing_bounded(runner_over(fake)) as runner:
        if passes:
            run_bounded(lambda: runner.run(plan), BOUND_S)
        else:
            with pytest.raises(services.ServiceUnreachable) as excinfo:
                run_bounded(lambda: runner.run(plan), BOUND_S)
            assert "only same-host workers answered" in excinfo.value.reason
    probes = fake.inner.log.probe_keys()
    assert len(set(probes)) == len(probes), "a probe was resubmitted under the same key"
    assert all(h == {"resources": None, "workers": None} for h in fake.probe_hints), fake.probe_hints
    if passes:
        (status,) = statuses_named(caplog.records, "web")
        assert (status.leg, status.host, status.identity) == ("managed", "cluster", SERVICE_HOST)
        assert len(probes) == (2 if worker_hosts[0] == SERVICE_HOST else 1), probes
    else:
        assert len(probes) == max(2, n_workers), probes  # the bound, at least two with one worker
    assert fake.released_keys() == [k for _s, k in fake.minted]


@pytest.mark.parametrize("worker_host", [DRIVER_HOST, SERVICE_HOST])
@pytest.mark.parametrize("leg", ["user", "site"])
def test_a_user_or_site_service_passes_on_any_answer(
    leg: str, worker_host: str, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    with CountingHTTPServer() as srv:
        site = {"http": srv.endpoint()} if leg == "site" else {}
        given = {"web": srv.endpoint()} if leg == "user" else None
        fake = HostFake(tmp_path / "ads", worker_hosts=(worker_host,), site_services=site, hosting=False)
        plan = spy_plan(SpyProcess(f"any-{leg}-{worker_host}"), 2, [bare_spec("web")])
        with caplog.at_level(logging.INFO), closing_bounded(runner_over(fake, services=given)) as runner:
            run_bounded(lambda: runner.run(plan), BOUND_S)
        (status,) = statuses_named(caplog.records, "web")
        assert (status.leg, status.identity) == (leg, None)
        assert len(fake.inner.log.probe_keys()) == 1


def test_the_probe_task_returns_host_identity(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    services = services_api()
    with CountingHTTPServer() as srv:
        monkeypatch.setenv(
            "_CONDOR_MACHINE_AD", str(write_machine_ad(tmp_path / "m.ad", "wn042.m68a.example"))
        )
        assert run_bounded(services.host_identity, BOUND_S) == "wn042.m68a.example"
        identity, reasons = run_bounded(
            lambda: services._probe_services([(srv.endpoint(), "http:/")]), BOUND_S
        )
        assert identity == "wn042.m68a.example"
        assert list(reasons.values() if hasattr(reasons, "values") else reasons) == [None]
        assert srv.gets == 1
        monkeypatch.delenv("_CONDOR_MACHINE_AD")
        assert run_bounded(services.host_identity, BOUND_S) == socket.getfqdn()
        identity, _ = run_bounded(lambda: services._probe_services([(srv.endpoint(), "http:/")]), BOUND_S)
        assert identity == socket.getfqdn()
