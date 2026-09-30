"""m68b B1 live legs: cluster-hosted services on a real pool (plan-services.md §3.3, the
``test_cluster_services_live.py`` row).

Needs the ``htcondor2`` bindings and a pool (the ``test-htcondor`` job's personal HTCondor), gated as
``tests/frozen/m68a/test_services_live.py`` gates: skipped where the bindings are not installed, and,
where they are, a missing schedd is a failure naming what is missing.

The profile is a copy of ``generic`` with ``service_ports=None``, so ``service_hosts == ("cluster",)`` and
the generic ``recipes.http_server`` (``inputs=()``, serving the empty ``service/``) is hosted on the
cluster. (a) its announce carries the pool host's ``Machine``, every probe answer carries it, a pilot task
GETs it, and each run's end removes its own service cluster, which history then orders against a task's
clock; (b) a GPU request no slot matches times out and a child that exits at once fails before
``timeout_s``, each job gone from the queue afterwards and its announce key forgotten.
"""

from __future__ import annotations

import dataclasses
import logging
import re
import threading
import time
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import Any

import pytest
from graphed.services import Launch, ServiceSpec
from m68b_harness import (
    HARNESS_FILE,
    GatedGet,
    TimedGet,
    backend_api,
    cluster_api,
    htcondor_api,
    keys_of,
    recipes_api,
    run_bounded,
    server_api,
    service_plan,
    spy_method,
    wait_for,
)

from graphed_executors.local._transport import LookupFreeHTTPServer

POOL_PROBE_S = 30.0
LIVE_S = 600.0
GONE_S = 30.0 + 30.0  # job_max_vacate_time, plus the schedd's own pace
SERVICES_LOGGER = "graphed_executors.services"


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
    return htcondor2


def cluster_only() -> Any:
    return dataclasses.replace(htcondor_api().SITES["generic"], service_ports=None)


def live_runner(tmp_path: Path, n_pilots: int) -> Any:
    runner = run_bounded(
        lambda: backend_api().htcondor_runner(
            n_pilots=n_pilots,
            site=cluster_only(),
            log_dir=tmp_path / "logs",
            user_modules=[HARNESS_FILE],
            min_pilots=n_pilots,
        ),
        LIVE_S,
    )
    assert tuple(runner.backend.service_hosts) == ("cluster",)
    return runner


def record_submits(backend: Any) -> tuple[list[str], list[tuple[str, Any]]]:
    """Patch ``backend.submit`` (on the instance) to keep every key and each probe task's answer."""
    real = backend.submit
    keys: list[str] = []
    answers: list[tuple[str, Any]] = []
    lock = threading.Lock()

    def submit(fn: Any, /, *args: Any, key: str, **kwargs: Any) -> Any:
        with lock:
            keys.append(key)
        fut = real(fn, *args, key=key, **kwargs)
        if key.startswith("svc-") and "-probe-" in key:
            fut.add_done_callback(
                lambda f: (
                    None if f.cancelled() or f.exception() is not None else answers.append((key, f.result()))
                )
            )
        return fut

    backend.submit = submit
    return keys, answers


def scopes(keys: list[str]) -> list[str]:
    """The run scopes (``run_nonce``) named by probe keys ``svc-<scope>-probe-<i>``, in first-seen order."""
    out: list[str] = []
    for key in keys:
        if key.startswith("svc-") and "-probe-" in key:
            scope = key[len("svc-") : key.rindex("-probe-")]
            if scope not in out:
                out.append(scope)
    return out


def batch(scope: str) -> str:
    return f'regexp("^graphed-service-{scope}-", JobBatchName)'


def queued(schedd: Any, constraint: str) -> list[Any]:
    return list(schedd.query(constraint=constraint, projection=["ClusterId", "JobStatus"]))


def history(schedd: Any, constraint: str) -> list[Any]:
    attrs = ["ClusterId", "JobStatus", "JobBatchName", "JobCurrentStartDate", "EnteredCurrentStatus"]
    return list(schedd.history(constraint, attrs, match=10))


def statuses(records: list[logging.LogRecord]) -> list[Any]:
    return [r.status for r in records if r.name == SERVICES_LOGGER and hasattr(r, "status")]


class _Gate(LookupFreeHTTPServer):
    daemon_threads = True
    state = "closed"
    polls = 0


class _GateHandler(BaseHTTPRequestHandler):
    server: _Gate

    def do_GET(self) -> None:
        self.server.polls += 1
        body = self.server.state.encode()
        self.send_response(200)
        self.send_header("content-type", "text/plain")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        return None


def http_server_spec() -> ServiceSpec:
    spec = recipes_api().http_server("web")
    assert spec.launch is not None and spec.launch.inputs == ()
    return dataclasses.replace(spec, timeout_s=300.0)


def test_a_cluster_hosted_http_server_serves_its_run_and_leaves_with_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    htc = require_pool()
    schedd = htc.Schedd()
    events: list[tuple[Any, ...]] = []
    spy_method(monkeypatch, server_api().TaskServer, "forget_announce", events)
    spy_method(monkeypatch, cluster_api().ServiceJob, "stop", events)
    spec = http_server_spec()
    runner = live_runner(tmp_path, 2)
    keys, answers = record_submits(runner.backend)
    gate = _Gate(("", 0), _GateHandler)
    threading.Thread(target=gate.serve_forever, daemon=True).start()
    try:
        with caplog.at_level(logging.INFO, logger=SERVICES_LOGGER):
            first = run_bounded(lambda: runner.run(service_plan(TimedGet(), 1, "live-1", (spec,))), LIVE_S)
        identity = run_bounded(runner.backend.host_identity, POOL_PROBE_S)
        (status,) = statuses(caplog.records)
        assert (status.leg, status.host, status.identity) == ("managed", "cluster", identity)
        assert status.endpoint.startswith(f"http://{identity}:"), status.endpoint
        assert answers and all(answer[0] == identity for _key, answer in answers), (identity, answers)
        ((_t1, listing, own_file),) = first.value
        assert own_file == 404, "the job's own service.json is served: the child does not run in service/"
        assert "service.json" not in listing and "graphed-secret" not in listing, listing
        (scope1,) = scopes(keys)
        assert wait_for(lambda: not queued(schedd, batch(scope1)), GONE_S), "run 1's service outlived its run"
        assert [e[0] for e in events] == ["forget_announce", "stop"], events
        assert keys_of(events, "forget_announce")[0].startswith(f"{scope1}-")

        second = run_bounded(lambda: runner.run(service_plan(TimedGet(), 1, "live-2", (spec,))), LIVE_S)
        ((t2, _listing, _own),) = second.value
        scope2 = scopes(keys)[1]
        assert scope2 != scope1
        assert wait_for(lambda: not queued(schedd, batch(scope2)), GONE_S), "run 2's service outlived its run"

        gate_url = f"http://{runner.backend.advertise_host}:{gate.server_address[1]}/"
        held = runner.submit(service_plan(GatedGet(gate_url), 1, "live-held", (spec,)))
        assert wait_for(lambda: gate.polls > 0, LIVE_S), "the submitted run's task never started"
        scope_held = scopes(keys)[2]
        direct = run_bounded(lambda: runner.run(service_plan(TimedGet(), 1, "live-direct", (spec,))), LIVE_S)
        assert len(direct.value) == 1
        scope_direct = scopes(keys)[3]
        assert wait_for(lambda: not queued(schedd, batch(scope_direct)), GONE_S)
        assert queued(schedd, batch(scope_held)), "the direct run's end removed the submitted run's service"
        gate.state = "open"
        assert len(held.result(LIVE_S).value) == 1
        assert wait_for(lambda: not queued(schedd, batch(scope_held)), GONE_S)
    finally:
        try:
            run_bounded(runner.close, LIVE_S)
        finally:
            gate.shutdown()
            gate.server_close()
    pilots = f"ClusterId == {runner.backend.launcher.cluster[1]}"
    assert wait_for(lambda: not queued(schedd, pilots), GONE_S), "close() left the pilots queued"
    ran = history(schedd, pilots)
    assert ran, "no pilot cluster in history"
    (h1,) = history(schedd, batch(scope1))
    (h2,) = history(schedd, batch(scope2))
    assert len({h1["ClusterId"], h2["ClusterId"], ran[0]["ClusterId"]}) == 3
    assert h1["JobStatus"] == 3 and h1["EnteredCurrentStatus"] < t2, (dict(h1), t2)
    assert h2["JobStatus"] == 3, dict(h2)
    assert h2["JobCurrentStartDate"] <= t2 and int(t2) <= h2["EnteredCurrentStatus"], (dict(h2), t2)
    assert str(h2["JobBatchName"]).startswith(f"graphed-service-{scope2}-")


def test_a_late_or_dead_service_job_is_removed_and_its_key_forgotten(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    htc = require_pool()
    schedd = htc.Schedd()
    events: list[tuple[Any, ...]] = []
    spy_method(monkeypatch, server_api().TaskServer, "forget_announce", events)
    runner = live_runner(tmp_path, 1)
    backend = runner.backend
    try:
        base = http_server_spec()
        assert base.launch is not None
        gpus = dataclasses.replace(
            base, launch=dataclasses.replace(base.launch, resources={"gpus": 2}), timeout_s=20.0
        )
        with pytest.raises(TimeoutError) as late:
            run_bounded(lambda: backend.host_service(gpus, "late"), LIVE_S)
        text = str(late.value)
        assert re.search(r"(?<![\w.])20(\.0+)?(?![\w.])", text) and re.search(r"JobStatus\W*1\b", text), text
        assert wait_for(lambda: not queued(schedd, batch("late")), GONE_S), (
            "the timed-out job is still queued"
        )
        assert any(k.startswith("late-") for k in keys_of(events, "forget_announce")), events

        dead = ServiceSpec(
            "dead",
            "http",
            check="http:/",
            launch=Launch(argv=("{python}", "-c", "import sys; sys.exit(7)")),
            timeout_s=240.0,
        )
        started = time.monotonic()
        with pytest.raises(RuntimeError) as died:
            run_bounded(lambda: backend.host_service(dead, "dead"), LIVE_S)
        assert time.monotonic() - started < 240.0, "a dead job was reported only at the timeout"
        assert re.search(r"JobStatus\W*4\b", str(died.value)), str(died.value)
        assert wait_for(lambda: not queued(schedd, batch("dead")), GONE_S), "the dead job is still queued"
        assert any(k.startswith("dead-") for k in keys_of(events, "forget_announce")), events
    finally:
        run_bounded(runner.close, LIVE_S)
