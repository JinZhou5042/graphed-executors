"""m68a §3.1 "Driverless endpoints" and D6: a driverless job resolves its plan's services in the driver job,
by the three legs, and classifies a service-phase failure as environment (exit 1). Every OS, no bindings:
``submit_driverless`` runs over the recorded bindings and the job runs as ``python -m ...driver <dir>``
in a scratch dir laid out from the recorded ``transfer_input_files`` (the m67 idiom).

The plan's process (``services_harness.GetProcess``) GETs its bound endpoint from each pilot task, and its
``resolve_services`` GETs it again in the driver, while the service is up, and wraps the value; the
managed service is ``recipes.http_server``'s requirement with ``service_child.py`` as its argv (it answers
every GET with its pid, so the server's pid is observable). Reading of E3: this file needs no histserv
(N11 moved the in-job histserv leg to m69b), so it does not ``importorskip`` it.

Discriminates: a driverless bare endpoint reaching the job, a service-phase exception exiting 3, a
refusal that is not retried or a plan error that is, a value resolved after its service is gone (or
shipped unresolved), and a ``result.pkl`` the submitter cannot load.
"""

from __future__ import annotations

import dataclasses
import importlib
import json
import logging
import os
import pickle
import secrets
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from services_harness import (
    DRIVER_MODULE,
    FAKE_CLUSTER,
    FAKE_SCHEDD,
    HARNESS_DIR,
    HARNESS_FILE,
    RUN_TIMEOUT_S,
    CountingHTTPServer,
    DriverResolved,
    RecordingBackend,
    RecordingSchedd,
    bounded_set,
    child_launch,
    closed_port,
    driver_api,
    endpoint_port,
    free_range,
    get_plan,
    hold_port,
    htcondor_api,
    job_dir,
    pid_gone,
    plain_plan,
    port_free,
    read_report,
    recipes_api,
    record_bindings,
    run_bounded,
    server_api,
    services_api,
    status_records,
    submit_api,
    submits,
    task_stage_error,
    write_machine_ad,
)


def web_spec(report: Path, ports: tuple[int, int] | None = None) -> Any:
    """``recipes.http_server("web")`` (kind ``http``, check ``http:/``) whose argv runs the pid-answering
    child; ``ports`` a free range; a job-sized ``timeout_s``."""
    spec = recipes_api().http_server("web")
    return dataclasses.replace(
        spec, ports=ports or free_range(), timeout_s=60.0, launch=child_launch("serve", report)
    )


def prepared_job(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, plan: Any, **kwargs: Any) -> Path:
    """A job scratch dir from a recorded ``submit_driverless`` (generic site, one local pilot)."""
    log_dir = tmp_path / "submit"
    log_dir.mkdir()
    fake = record_bindings(monkeypatch, RecordingSchedd())
    run_bounded(
        lambda: htcondor_api().submit_driverless(
            plan,
            site="generic",
            n_pilots=1,
            log_dir=log_dir,
            request_memory_mb=1024,
            user_modules=[HARNESS_FILE],
            **kwargs,
        ),
        RUN_TIMEOUT_S,
    )
    (entry,) = submits(fake.log)
    job = job_dir(entry[1], log_dir, tmp_path / "job")
    write_machine_ad(job / ".machine.ad")
    return job


def run_driver(job: Path) -> tuple[int, int, str]:
    """(exit code, driver pid, stderr) of ``python -m ...driver <job>`` run in ``job``."""
    env = dict(os.environ, _CONDOR_MACHINE_AD=str(job / ".machine.ad"))
    proc = subprocess.Popen(
        [sys.executable, "-m", DRIVER_MODULE, str(job)],
        cwd=job,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        _, err = proc.communicate(timeout=RUN_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.communicate()
        raise
    return proc.returncode, proc.pid, err


def read_result(job: Path) -> tuple[bool, Any]:
    ok, payload = pickle.loads((job / "result.pkl").read_bytes())
    return ok, payload


def driver_log(job: Path) -> str:
    path = job / "driver.log"
    return path.read_text() if path.is_file() else "<no driver.log>"


# ---- the in-job service leg -----------------------------------------------------------------------------


def test_the_driver_job_hosts_the_service_and_resolves_the_value(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, children: Any
) -> None:
    report = children.report()
    spec = web_spec(report)
    job = prepared_job(tmp_path, monkeypatch, get_plan(2, "m68a-dl-main", [spec]))
    code, driver_pid, err = run_driver(job)
    assert code == 0, (err, driver_log(job))
    ok, result = read_result(job)  # loaded after the job exited
    assert ok is True, result
    value = result.value
    assert isinstance(value, DriverResolved), f"the value was shipped unresolved: {value!r}"
    server_pid = children.add(read_report(report)["pid"])
    assert value.driver_pid == driver_pid
    assert server_pid not in (driver_pid, os.getpid())
    assert value.body == str(server_pid)
    pilots = {pid for pid, _body in value.leaves}
    assert pilots and driver_pid not in pilots and server_pid not in pilots, value.leaves
    assert {body for _pid, body in value.leaves} == {str(server_pid)}
    assert pid_gone(server_pid), "the driver job's service outlived the job"
    assert port_free(endpoint_port(value.endpoint))


def test_given_endpoints_short_circuit_the_launch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, children: Any
) -> None:
    report = children.report()
    with CountingHTTPServer() as srv:
        job = prepared_job(
            tmp_path,
            monkeypatch,
            get_plan(2, "m68a-dl-given", [web_spec(report)]),
            services={"web": srv.endpoint()},
        )
        assert json.loads((job / "run.json").read_text())["endpoints"] == {"web": srv.endpoint()}
        code, _driver_pid, err = run_driver(job)
        assert code == 0, (err, driver_log(job))
        # the plan's count: the driver's own set (one leg-1 check, one probe answer), the run's set (the
        # same two), one GET per task (two partitions), one by resolve_services in the driver
        assert srv.gets == 7, srv.gets
    _ok, result = read_result(job)
    assert result.value.body == str(os.getpid()), "the value was not resolved against the given endpoint"
    assert result.value.endpoint == srv.endpoint()
    assert not report.exists(), "the driver job started a service it was given an endpoint for"


@pytest.mark.parametrize("bare", ["127.0.0.1:8080", "web.m68a.example:80", "triton://h:1"])
def test_a_bare_endpoint_is_refused_before_any_bindings_call(
    bare: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, children: Any
) -> None:
    fake = record_bindings(monkeypatch, RecordingSchedd())
    plan = get_plan(2, "m68a-dl-bare", [web_spec(children.report())])
    with pytest.raises(ValueError) as excinfo:
        run_bounded(
            lambda: htcondor_api().submit_driverless(
                plan,
                log_dir=tmp_path,
                request_memory_mb=1024,
                user_modules=[HARNESS_FILE],
                services={"web": bare},
            ),
            RUN_TIMEOUT_S,
        )
    text = str(excinfo.value)
    assert bare in text and all(s in text for s in ("tcp", "http", "https", "grpc", "grpcs")), text
    assert fake.log == [], fake.log
    assert not (tmp_path / "run.json").exists()


def test_a_plan_without_services_starts_nothing(caplog: pytest.LogCaptureFixture) -> None:
    """r24: the driver enters a ``ServiceSet`` over ``plan.services`` for every plan; with none it
    submits no probe, starts nothing and logs no status (and the engine's own phase does the same)."""
    services, submit = services_api(), submit_api()
    backend = RecordingBackend(submit.ThreadBackend(1))
    try:
        with caplog.at_level(logging.INFO):
            with bounded_set(services.ServiceSet((), backend, endpoints={})) as eps:
                assert dict(eps) == {}
            runner = submit.SubmitRunner(backend)
            try:
                result = run_bounded(lambda: runner.run(plain_plan(2, "m68a-dl-none")), RUN_TIMEOUT_S)
            finally:
                run_bounded(runner.close, RUN_TIMEOUT_S)
        assert result.n_partitions == 2
        assert backend.log.probe_keys() == [], backend.log.keys()
        assert status_records(caplog.records) == []
    finally:
        run_bounded(backend.close, RUN_TIMEOUT_S)


# ---- service-phase failures exit 1 ------------------------------------------------------------------------


def test_a_dead_given_endpoint_exits_1_with_service_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, children: Any
) -> None:
    dead = f"http://127.0.0.1:{closed_port()}"
    plan = get_plan(2, "m68a-dl-dead", [web_spec(children.report())])
    job = prepared_job(tmp_path, monkeypatch, plan, services={"web": dead})
    code, _pid, err = run_driver(job)
    assert code == 1, (code, err, driver_log(job))
    ok, exc = read_result(job)
    assert ok is False and isinstance(exc, services_api().ServiceUnavailable), exc
    assert exc.name == "web" and "user" in exc.legs


def test_a_held_port_range_exits_1_with_os_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, children: Any
) -> None:
    port = closed_port()
    report = children.report()
    job = prepared_job(tmp_path, monkeypatch, get_plan(2, "m68a-dl-held", [web_spec(report, (port, port))]))
    with hold_port(port):
        code, _pid, err = run_driver(job)
    assert code == 1, (code, err, driver_log(job))
    ok, exc = read_result(job)
    assert ok is False and isinstance(exc, OSError), exc
    assert not report.exists()


def test_a_refusal_of_the_in_run_recheck_exits_1(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, children: Any
) -> None:
    """The given endpoint answers the driver's leg-1 check and the one probe answer, then refuses: the
    re-check inside ``runner.run`` raises ``ServiceUnavailable``, which ``_exit_code`` maps to 1."""
    with CountingHTTPServer(ok_first=2) as srv:
        plan = get_plan(2, "m68a-dl-recheck", [web_spec(children.report())])
        job = prepared_job(tmp_path, monkeypatch, plan, services={"web": srv.endpoint()})
        code, _pid, err = run_driver(job)
        assert srv.gets >= 3, "the run's own set never re-checked the given endpoint"
    assert code == 1, (code, err, driver_log(job))
    ok, exc = read_result(job)
    assert ok is False and isinstance(exc, services_api().ServiceUnavailable), exc


def _service_unavailable() -> BaseException:
    exc: BaseException = services_api().ServiceUnavailable(
        "web", {"user": "refused", "site": "none", "managed": "no"}
    )
    return exc


def _service_unreachable() -> BaseException:
    exc: BaseException = services_api().ServiceUnreachable("web", "grpc://h:1", "wn1", "refused")
    return exc


def _worker_lost() -> BaseException:
    exc: BaseException = server_api().WorkerLost("svc-abc-probe-0", "pilot-1")
    return exc


EXIT_CODES = {
    "ServiceUnavailable": (_service_unavailable, 1),
    "ServiceUnreachable": (_service_unreachable, 1),
    "raw WorkerLost": (_worker_lost, 1),
    "ValueError": (lambda: ValueError("negative pt"), 3),
    "StageError from a task's ValueError": (task_stage_error, 3),
}


@pytest.mark.parametrize("case", sorted(EXIT_CODES))
def test_exit_code_classifies_the_in_run_types(case: str) -> None:
    make, expected = EXIT_CODES[case]
    assert run_bounded(lambda: driver_api()._exit_code(make()), RUN_TIMEOUT_S) == expected


@pytest.mark.parametrize(("fail", "exc_type"), [("ValueError", ValueError), ("OSError", OSError)])
def test_a_plan_error_with_a_live_service_exits_3_intact(
    fail: str, exc_type: type[BaseException], tmp_path: Path, monkeypatch: pytest.MonkeyPatch, children: Any
) -> None:
    with CountingHTTPServer() as srv:
        plan = get_plan(2, f"m68a-dl-{fail}", [web_spec(children.report())], fail=fail)
        job = prepared_job(tmp_path, monkeypatch, plan, services={"web": srv.endpoint()})
        code, _pid, err = run_driver(job)
    assert code == 3, (code, err, driver_log(job))
    ok, exc = read_result(job)
    assert ok is False and type(exc) is exc_type, repr(exc)
    assert "m68a-dl-" in str(exc), str(exc)


# ---- the hop from job to submitter ------------------------------------------------------------------------


def test_a_service_unreachable_round_trips_through_the_result_blob() -> None:
    services = services_api()
    unreachable = services.ServiceUnreachable("t", "grpc://h:1", "w", "refused")
    ok, exc = pickle.loads(run_bounded(lambda: driver_api()._result_blob(False, unreachable), RUN_TIMEOUT_S))
    assert ok is False and type(exc) is services.ServiceUnreachable, repr(exc)
    assert (exc.name, exc.endpoint, exc.worker, exc.reason) == ("t", "grpc://h:1", "w", "refused")
    assert "refused" in str(exc) and "t" in str(exc)


def test_a_service_unavailable_round_trips_through_the_result_blob() -> None:
    services = services_api()
    legs = {"user": "none given", "site": "no site row", "managed": "no launch"}
    unavailable = services.ServiceUnavailable("t", legs)
    ok, exc = pickle.loads(run_bounded(lambda: driver_api()._result_blob(False, unavailable), RUN_TIMEOUT_S))
    assert ok is False and type(exc) is services.ServiceUnavailable, repr(exc)
    assert exc.name == "t" and dict(exc.legs) == legs
    assert all(reason in str(exc) for reason in legs.values()), str(exc)


def test_an_exception_that_does_not_reload_arrives_as_text(tmp_path: Path) -> None:
    """Run in a fresh interpreter: stdlib exception pickling is process-wide state (``distributed``
    replaces it on import)."""
    out = tmp_path / "narrow.json"
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join([HARNESS_DIR, env.get("PYTHONPATH", "")]).rstrip(os.pathsep)
    code = f"import services_harness as h; h.narrow_blob_main({str(out)!r})"
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
    assert got["control"] == "TypeError", "control: a dumps-only blob of NarrowError loads here"
    assert got["ok"] is False and got["type"] == "RuntimeError", got
    assert "NarrowError" in got["text"], got["text"]


def test_a_result_the_submitter_cannot_load_names_the_error_and_the_log(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    schedd = RecordingSchedd(queue=[[{"JobStatus": 4, "ExitCode": 1, "HoldReasonCode": 0}]])
    fake = record_bindings(monkeypatch, schedd)
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    modname = f"m68a_vanishing_{secrets.token_hex(4)}"
    mod_dir = tmp_path / "mods"
    mod_dir.mkdir()
    (mod_dir / f"{modname}.py").write_text("class Gone(Exception):\n    pass\n")
    sys.path.insert(0, str(mod_dir))
    try:
        module = importlib.import_module(modname)
        blob = pickle.dumps((False, module.Gone("from a module the submitter lacks")))
    finally:
        sys.path.remove(str(mod_dir))
        sys.modules.pop(modname, None)
        shutil.rmtree(mod_dir)
        importlib.invalidate_caches()
    (log_dir / "result.pkl").write_bytes(blob)
    (log_dir / "driver.log").write_text("driver pid=1\nexit 3\n")
    with pytest.raises(ModuleNotFoundError):
        pickle.loads(blob)  # control: a bare load
    handle = htcondor_api().RunHandle(
        site="generic", schedd=FAKE_SCHEDD, cluster=FAKE_CLUSTER, log_dir=log_dir, submitted_at=0.0
    )
    with pytest.raises(RuntimeError) as excinfo:
        run_bounded(handle.result, RUN_TIMEOUT_S)
    assert "ModuleNotFoundError" in str(excinfo.value) and "driver.log" in str(excinfo.value), str(
        excinfo.value
    )
    assert ("_htcondor",) in fake.log and not submits(fake.log)
