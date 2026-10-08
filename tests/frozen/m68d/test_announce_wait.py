"""m68d A (plan §2 "A — the driver fails fast on an ended, held or unschedulable SERVICE node"), rows
A1-A4 of §4, every OS. The in-job backend is built by ``driver._runner`` from a DAG ``run.json`` with no
pilots, a machine ad and (unless a case drops it) a job ad naming ``DAG_ID``; the bindings are recorded
(``FakeHTCondor`` over ``NodeSchedd``) and svc0's queue ad follows each case's phases."""

from __future__ import annotations

import json
import re
import sys
import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest
from m68d_harness import (
    FAKE_POOL,
    FAKE_SCHEDD,
    HOLD_TEXT,
    LOCATE_TEXT,
    NODE,
    QUERY_TEXT,
    SERVICE_ID,
    FakeHTCondor,
    NodeSchedd,
    Web,
    announce_later,
    cut,
    driver_api,
    driver_log,
    fake_classad2,
    free_range,
    gated_plan,
    htcondor_api,
    logged,
    node_ad,
    node_spec,
    raised_by,
    record_bindings,
    run_bounded,
    run_json,
    server_api,
    site_copy,
    write_job_ad,
    write_machine_ad,
)

LOCATE = [FAKE_POOL, FAKE_SCHEDD]
IDLE, RUNNING, HELD = 1, 2, 5


@contextmanager
def in_job_backend(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, locate: bool = True, job_ad: bool = True
) -> Iterator[tuple[Any, Path]]:
    """``driver._runner``'s backend for a DAG driver node (no pilots), with ``driver.log`` open as
    ``driver.main`` holds it; yields ``(backend, job)`` and closes the runner on exit."""
    job, dag = tmp_path / "job", tmp_path / "dag"
    dag.mkdir(parents=True)
    monkeypatch.setenv("_CONDOR_MACHINE_AD", str(write_machine_ad(tmp_path / "machine.ad", "127.0.0.1")))
    if job_ad:
        monkeypatch.setenv("_CONDOR_JOB_AD", str(write_job_ad(tmp_path / "job.ad")))
    else:
        monkeypatch.delenv("_CONDOR_JOB_AD", raising=False)
    site_copy(monkeypatch, "m-dee-node", "generic", worker_ports=free_range(3))
    run = run_json("m-dee-node", dag_dir=dag, n_pilots=0, schedd_locate=LOCATE if locate else None)
    with driver_log(job) as log:
        runner = run_bounded(lambda: driver_api()._runner(run, job, log), 60.0)
        try:
            yield runner.backend, job
        finally:
            run_bounded(runner.close, 60.0)


def text_of(err: BaseException, *paths: Path) -> str:
    """An exception's text and its ``legs`` (a ``ServiceUnavailable``'s), with every tmp path cut."""
    legs = getattr(err, "legs", None) or {}
    return cut(" ".join([str(err), *map(str, legs.values())]), *paths)


def fast_polls(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(server_api(), "POLL_S", 0.2)


def node_answers(schedd: NodeSchedd, status: int) -> list[tuple[Any, ...]]:
    return [e for e in schedd.node_queries() if e[3] == status]


# ---- A1: a held or departed node fails the wait at once ----------------------------------------------------


@pytest.mark.parametrize("case", ["held", "absent"])
def test_a_held_or_departed_service_node_fails_the_wait_at_once(
    case: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fast_polls(monkeypatch)
    ad = node_ad(HELD, HoldReasonCode=1, HoldReason=HOLD_TEXT) if case == "held" else None
    schedd = NodeSchedd([(0.0, ad)])
    fake = record_bindings(monkeypatch, FakeHTCondor(schedd))
    with in_job_backend(tmp_path, monkeypatch) as (backend, _job):
        begun = time.monotonic()
        err = raised_by(lambda: backend.host_service(node_spec(600.0), "m-dee-scope"), 20.0)
        took = time.monotonic() - begun
    text = text_of(err, tmp_path)
    assert isinstance(err, RuntimeError), repr(err)
    assert NODE in text, text
    assert (HOLD_TEXT if case == "held" else "ended before it announced") in text, text
    assert took < 2.0, f"raised after {took:.1f}s"
    assert ("locate", FAKE_POOL, "Schedd", FAKE_SCHEDD) in fake.log, fake.log
    projected = [set(e[2]) for e in schedd.node_queries()]
    assert any({"JobStatus", "HoldReasonCode", "HoldReason"} <= p for p in projected), schedd.log


# ---- A2: a node no slot could ever run fails the wait; a matchable one waits for its slot ------------------


A2_CASES = {
    "above-every-slot": [(0.0, node_ad(IDLE, memory_mb=23000))],
    "fits-only-without-the-driver-claim": [(0.0, node_ad(IDLE, memory_mb=12000))],
    "idle-and-matchable": [(0.0, node_ad(IDLE))],
    "running-after-idle": [(1.0, node_ad(IDLE)), (0.0, node_ad(RUNNING))],
}


@pytest.mark.parametrize("case", sorted(A2_CASES))
def test_an_unschedulable_service_node_fails_the_wait_naming_its_request(
    case: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fast_polls(monkeypatch)
    monkeypatch.setitem(sys.modules, "classad2", fake_classad2())
    schedd = NodeSchedd(A2_CASES[case])
    fake = record_bindings(monkeypatch, FakeHTCondor(schedd))
    with Web() as web, in_job_backend(tmp_path, monkeypatch) as (backend, _job):
        begun = time.monotonic()
        if case == "idle-and-matchable":
            announce_later(tmp_path / "dag", web.port, 1.5)
            got = run_bounded(lambda: backend.host_service(node_spec(0.5), "m-dee-scope"), 20.0)
            took = time.monotonic() - begun
            assert tuple(got) == (web.endpoint, SERVICE_ID, NODE), got
            assert took >= 1.4 and len(node_answers(schedd, IDLE)) >= 3, (took, schedd.log)
        elif case == "running-after-idle":
            err = raised_by(lambda: backend.host_service(node_spec(0.5), "m-dee-scope"), 20.0)
            ended = time.monotonic()
            assert isinstance(err, TimeoutError) and "timeout_s" in str(err), repr(err)
            first_running = node_answers(schedd, RUNNING)
            assert first_running, f"no running answer was served before the raise: {schedd.log}"
            assert ended - first_running[0][4] >= 0.45, f"raised {ended - first_running[0][4]:.2f}s after it ran"
            assert node_answers(schedd, IDLE), schedd.log
        else:
            err = raised_by(lambda: backend.host_service(node_spec(600.0), "m-dee-scope"), 20.0)
            took = time.monotonic() - begun
            asked = "23000" if case == "above-every-slot" else "12000"
            text = text_of(err, tmp_path)
            assert not isinstance(err, TimeoutError), repr(err)
            assert re.search(rf"RequestMemory\D{{0,16}}{asked}\b", text), text
            assert took < 10.0, f"refused after {took:.1f}s"
            assert logged(fake.log, "act") == [], f"the node was acted on, though DAGMan owns it: {fake.log}"
            assert any(e[0] == "collector-query" for e in fake.log), fake.log


# ---- A3: without schedd access the wait is the plain one ----------------------------------------------------


def waited(backend: Any, dag: Path, delay_s: float = 0.6) -> None:
    """``host_service`` resolves on an announce posted ``delay_s`` into its wait."""
    with Web() as web:
        announce_later(dag, web.port, delay_s)
        got = run_bounded(lambda: backend.host_service(node_spec(600.0), "m-dee-scope"), 30.0)
        assert tuple(got) == (web.endpoint, SERVICE_ID, NODE), got


def node_lines(job: Path, pattern: str) -> list[str]:
    return [line for line in (job / "driver.log").read_text().splitlines() if re.search(pattern, line)]


A3_CASES = ["no-schedd-locate", "no-job-ad", "no-bindings", "a-raising-locate", "a-raising-query"]


@pytest.mark.parametrize("case", A3_CASES)
def test_without_schedd_access_the_wait_is_the_plain_one(
    case: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fast_polls(monkeypatch)
    running = [(0.0, node_ad(RUNNING))]
    schedd = NodeSchedd(running, refuse=QUERY_TEXT if case == "a-raising-query" else None)
    if case == "no-bindings":
        monkeypatch.setitem(sys.modules, "htcondor2", None)
    else:
        record_bindings(monkeypatch, FakeHTCondor(schedd, locate_refuses=case == "a-raising-locate"))
    shape = {"locate": case != "no-schedd-locate", "job_ad": case != "no-job-ad"}
    with in_job_backend(tmp_path / "case", monkeypatch, **shape) as (backend, job):
        waited(backend, tmp_path / "case" / "dag")
    if case in ("no-schedd-locate", "no-job-ad"):
        assert logged(schedd.log, "query") == [] and logged(schedd.log, "locate") == [], schedd.log
        # control: with both present the same wait reads svc0's ad
        control = NodeSchedd(running)
        record_bindings(monkeypatch, FakeHTCondor(control))
        with in_job_backend(tmp_path / "control", monkeypatch) as (backend, _job):
            waited(backend, tmp_path / "control" / "dag")
        assert control.node_queries(), control.log
        return
    token = {
        "no-bindings": r"ImportError|ModuleNotFoundError|htcondor bindings",
        "a-raising-locate": re.escape(LOCATE_TEXT),
        "a-raising-query": re.escape(QUERY_TEXT),
    }[case]
    assert len(node_lines(job, token)) == 1, (job / "driver.log").read_text()


# ---- A4: a DAG run.json names its schedd where jobs can submit ------------------------------------------------


def submitted_run(monkeypatch: pytest.MonkeyPatch, plan: Any, **kwargs: Any) -> dict[str, Any]:
    record_bindings(monkeypatch, FakeHTCondor(NodeSchedd()))
    kwargs.setdefault("request_memory_mb", 1024)
    handle = run_bounded(lambda: htcondor_api().submit_driverless(plan, **kwargs), 60.0)
    run: dict[str, Any] = json.loads((Path(handle.log_dir) / "run.json").read_text())
    return run


def fake_venv(root: Path) -> Path:
    root.mkdir(parents=True)
    (root / "pyvenv.cfg").write_text("home = /usr/bin\ninclude-system-site-packages = false\n")
    return root


def test_a_dag_run_json_names_its_schedd_where_jobs_can_submit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dag_plan = gated_plan(tmp_path / "mark", node_spec(30.0))
    generic = submitted_run(monkeypatch, dag_plan, site="generic", pilots="local", log_dir=tmp_path / "g")
    site_copy(monkeypatch, "lxplus", "lxplus", job_root=str(tmp_path))
    image = "/cvmfs/unpacked.cern.ch/registry.hub.docker.com/coffeateam/coffea-almalinux-noml:latest"
    lxplus = submitted_run(
        monkeypatch,
        dag_plan,
        site="lxplus",
        pilots="local",
        image=image,
        env=fake_venv(tmp_path / "env"),
        log_dir=tmp_path / "x",
    )
    plain = submitted_run(monkeypatch, gated_plan(tmp_path / "plain", None), site="generic", pilots="local", log_dir=tmp_path / "p")
    rows: Sequence[dict[str, Any]] = (generic, lxplus)
    assert all(run["announce_only"] == {"web": NODE} for run in rows), rows
    assert [run["schedd_locate"] for run in rows] == [LOCATE, LOCATE], rows
    assert plain["schedd_locate"] is None and not plain["announce_only"], plain
