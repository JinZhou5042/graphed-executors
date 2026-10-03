"""m69b: a need of a worker moves the runner's pilots only while no plan of its backend starts its services
(plan-services.md §5.2 "Ordering"), over the frozen ``OrderSchedd`` recorder: a need while a later plan's
services start, beside one or between two, releases its held pilots only after the set's last announce,
a driver job's pilots are submitted only after its SERVICE node announced, and a driver job whose pilots
never start exits 1 with or without services."""

from __future__ import annotations

import importlib
import json
import pickle
import sys
import threading
import time
from pathlib import Path
from typing import Any

import pytest
from graphed.services import ServiceSpec

from graphed_executors.htcondor_backend import HTCondorBackend, driver, launch
from graphed_executors.htcondor_backend import server as server_mod
from graphed_executors.htcondor_backend.announce import SECRET_FILE, URL_FILE
from graphed_executors.submit.services import ServiceSet

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "frozen" / "m69b"))
order = importlib.import_module("m69b_order")
harness = importlib.import_module("services_harness")

SERVICE_PORT = 10007  # named in the announce; nothing dials it
ANNOUNCE_AFTER_S = 1.0


def later_plan_backend(tmp_path: Path, schedd: Any) -> HTCondorBackend:
    """A backend whose first plan ran: its pilots submitted (they start, and answer a set's probe)."""
    backend = HTCondorBackend(launch.CondorPilots("generic", log_dir=tmp_path), 2, host="127.0.0.1")
    backend.min_pilots = 0
    backend.wait_for_pilots(0)
    return backend


def closed(backend: HTCondorBackend, schedd: Any) -> None:
    schedd.finish_pilots()
    backend.close()
    schedd.stop_pilots()


def test_a_need_beside_a_starting_service_releases_the_held_pilots_only_after_its_announce(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(server_mod, "POLL_S", 0.5)
    with harness.CountingHTTPServer() as service:
        schedd = order.OrderSchedd(service.port, pilots=True, announce_after_s=None)
        order.order_bindings(monkeypatch, schedd)
        backend = later_plan_backend(tmp_path, schedd)
        started: list[object] = []

        def plan_b() -> None:
            with ServiceSet([harness.hosted_spec("web")], backend, scope="planB") as endpoints:
                started.append(endpoints)

        thread = threading.Thread(target=plan_b, daemon=True)
        try:
            thread.start()
            deadline = time.monotonic() + 30.0
            while not schedd.service_dirs() and time.monotonic() < deadline:
                time.sleep(0.05)
            (job_dir,) = schedd.service_dirs()
            schedd.mark("plan A needs a worker")
            backend.submit(time.sleep, 0, key="graphed-planA-leaf-0")
            schedd.mark("plan B's server announces")
            schedd.announce(job_dir)
            thread.join(60.0)
        finally:
            backend.stop_waiting()
            closed(backend, schedd)
            thread.join(30.0)
    assert len(started) == 1, schedd.events
    waiting = order.between(schedd.events, "plan A needs a worker", "plan B's server announces")
    assert order.acts(waiting, "Release") == [], waiting
    after = order.between(schedd.events, "plan B's server announces")
    assert len(order.acts(after, "Release")) == 1, after
    assert len(order.acts(schedd.events, "Hold")) == 1, schedd.events


def test_a_need_between_a_set_s_services_leaves_the_pilots_held_to_its_last_announce(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(server_mod, "POLL_S", 0.5)
    with harness.CountingHTTPServer() as service:
        schedd = order.OrderSchedd(service.port, pilots=True, announce_after_s=0.5)
        order.order_bindings(monkeypatch, schedd)
        backend = later_plan_backend(tmp_path, schedd)
        host = backend.host_service

        def host_then_need(spec: ServiceSpec, scope: str) -> tuple[str, str, str]:
            got = host(spec, scope)
            schedd.mark(f"{spec.name} announced")
            backend.n_workers()
            return got

        monkeypatch.setattr(backend, "host_service", host_then_need)
        specs = [harness.hosted_spec("web"), harness.hosted_spec("web2")]
        try:
            schedd.mark("the set starts")
            with ServiceSet(specs, backend, scope="planB"):
                schedd.mark("the set started")
        finally:
            closed(backend, schedd)
    inside = order.between(schedd.events, "the set starts", "the set started")
    holds, releases = order.acts(inside, "Hold"), order.acts(inside, "Release")
    assert len(holds) == 1 and len(releases) == 1, inside
    assert order.acts(order.between(schedd.events, "web2 announced"), "Release")[:1] == releases, inside


def driver_job(tmp_path: Path, services: tuple[str, ...]) -> tuple[Path, Path]:
    """A DAG driver job's dir over pilots="condor", each of ``services`` a SERVICE node; and its DAG dir."""
    job, dag = tmp_path / "job", tmp_path / "dag"
    job.mkdir()
    dag.mkdir()
    run = {
        "pilots": "condor",
        "n_pilots": 1,
        "site": "generic",
        "image": None,
        "log_dir": str(tmp_path / "pilots"),
        "request_memory_mb": 1024,
        "min_pilots": 1,
        "retries": 0,
        "max_in_flight": 1,
        "schedd_locate": [harness.FAKE_POOL, harness.FAKE_SCHEDD],
        "user_modules": [],
        "endpoints": None,
        "announce_only": {name: f"svc{i}" for i, name in enumerate(services)},
        "dag_dir": str(dag),
        "extra_submit": None,
    }
    (job / "run.json").write_text(json.dumps(run))
    (job / "plan.pkl").write_bytes(pickle.dumps(order.order_plan("m69b-gate-dag", *services)))
    return job, dag


def service_node(schedd: Any, dag: Path, node: Path) -> threading.Thread:
    """SERVICE node ``node``: idle ``ANNOUNCE_AFTER_S`` after the driver publishes its url, then announce."""

    def announce() -> None:
        deadline = time.monotonic() + 60.0
        while not (dag / URL_FILE).exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        time.sleep(ANNOUNCE_AFTER_S)
        node.mkdir()
        url = (dag / URL_FILE).read_text().strip()
        (node / "service.json").write_text(json.dumps({"key": node.name, "url": url}))
        (node / "graphed-secret").write_text((dag / SECRET_FILE).read_text().strip())
        schedd.announce(node)

    thread = threading.Thread(target=announce, daemon=True)
    thread.start()
    return thread


def test_a_driver_job_submits_its_pilots_after_its_service_node_announced(tmp_path: Path) -> None:
    job, dag = driver_job(tmp_path, ("web",))
    with harness.CountingHTTPServer() as service, pytest.MonkeyPatch.context() as mp:
        schedd = order.OrderSchedd(service.port, pilots=True)
        order.order_bindings(mp, schedd)
        node = service_node(schedd, dag, tmp_path / "svc0")
        try:
            code = driver.main([str(job)])
        finally:
            schedd.finish_pilots()
            schedd.stop_pilots()
            node.join(60.0)
    assert code == driver.EXIT_DONE, (job / "driver.log").read_text()
    kinds = [e.kind for e in schedd.events]
    assert "announce" in kinds and "submit-pilots" in kinds, kinds
    assert kinds.index("announce") < kinds.index("submit-pilots"), kinds


@pytest.mark.parametrize("services", [(), ("web",)], ids=["no-services", "services"])
def test_a_driver_job_whose_pilots_never_start_exits_1(
    services: tuple[str, ...], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    job, dag = driver_job(tmp_path, services)
    wait = HTCondorBackend.wait_for_pilots
    monkeypatch.setattr(HTCondorBackend, "wait_for_pilots", lambda self, n, timeout=0.0: wait(self, n, 0.5))
    schedd = order.OrderSchedd(SERVICE_PORT)  # its pilots never start
    order.order_bindings(monkeypatch, schedd)
    nodes = [service_node(schedd, dag, tmp_path / "svc0")] if services else []
    assert driver.main([str(job)]) == driver.EXIT_FAILED
    for node in nodes:
        node.join(60.0)
    assert "0 of 1 pilots connected after 0.5s" in (job / "driver.log").read_text()
