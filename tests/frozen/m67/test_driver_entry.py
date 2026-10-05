"""m67 D6: the driver entry, ``python -m graphed_executors.htcondor_backend.driver <dir>``, run the way a job
runs it: from a scratch dir laid out by ``transfer_input_files`` of a recorded ``submit_driverless``.

``pilots="local"`` runs the plan on two pilot subprocesses (pids other than the driver's) whose task
server listens on a ``worker_ports`` port, and writes ``result.pkl = (True, ExecResult)`` equal to the
sequential result, plus ``driver.log``. A plan error exits 3 with the intact ``StageError`` in
``result.pkl``; pilots that cannot start and a missing ``run.json`` exit 1 with ``(False, <exception>)``
in ``result.pkl`` and a ``driver.log``, the two files every exit must leave for the output transfer.
``pilots="condor"`` (run in-process over recorded bindings whose schedd starts the pilots locally) reaches
its schedd only through ``Collector(pool).locate(Schedd, name)`` from ``run.json.schedd_locate``, never
``Schedd()``, which has no local daemon to find inside a job.

Discriminates a driver that computes on its own pid, a lost or swallowed exception, a plan error that
exits 1 (and is retried), a pilot failure that exits 3, a hang, a task server on the login-side
``driver_ports``, and a ``Schedd()`` call in the job path."""

from __future__ import annotations

import os
import pickle
import subprocess
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import pytest
from driverless_harness import (
    DRIVER_MODULE,
    FAKE_POOL,
    FAKE_SCHEDD,
    HARNESS_FILE,
    RUN_TIMEOUT_S,
    PilotSchedd,
    RecordingSchedd,
    assert_intact_stage_error,
    concat_plan,
    driver_api,
    expected_concat,
    fake_venv,
    htcondor_api,
    job_dir,
    job_plan,
    record_bindings,
    run_bounded,
    schedd_locations,
    stage_error_plan,
    submits,
    write_machine_ad,
)
from graphed.core.execution import ExecResult, SequentialRunner

IMAGE = "/cvmfs/unpacked.cern.ch/registry.hub.docker.com/coffeateam/coffea-almalinux9-noml:2026.9.0-py3.12"

# the pilots' python is the driver's sys.executable; this entry points it at a path that does not exist
BOGUS_PYTHON_ENTRY = (
    "import runpy, sys; sys.executable = sys.argv[1] + '/no-such-python'; "
    f"runpy.run_module({DRIVER_MODULE!r}, run_name='__main__', alter_sys=True)"
)


def prepared_job(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, plan: Any, **kwargs: Any) -> Path:
    """A job scratch dir from a recorded ``submit_driverless``, with ``.machine.ad``."""
    log_dir = tmp_path / "submit"
    log_dir.mkdir()
    fake = record_bindings(monkeypatch, RecordingSchedd())
    htcondor_api().submit_driverless(
        plan, log_dir=log_dir, request_memory_mb=2048, user_modules=[HARNESS_FILE], **kwargs
    )
    (entry,) = submits(fake.log)
    job = job_dir(entry[1], log_dir, tmp_path / "job")
    write_machine_ad(job / ".machine.ad")
    return job


def run_driver(job: Path, argv: list[str] | None = None) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ, _CONDOR_MACHINE_AD=str(job / ".machine.ad"))
    cmd = argv if argv is not None else [sys.executable, "-m", DRIVER_MODULE, str(job)]
    return subprocess.run(
        cmd, cwd=job, env=env, capture_output=True, text=True, timeout=RUN_TIMEOUT_S, check=False
    )


def run_driver_popen(job: Path) -> tuple[int, int, str]:
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


def assert_reported_failure(job: Path) -> BaseException:
    """Exit 1 is the driver's own verdict, not an interpreter traceback: both transferred outputs exist
    and ``result.pkl`` holds ``(False, <the exception>)``."""
    assert (job / "driver.log").is_file(), sorted(p.name for p in job.iterdir())
    ok, err = read_result(job)
    assert ok is False and isinstance(err, BaseException), (ok, err)
    assert not isinstance(err, NotImplementedError), repr(err)
    return err


def test_local_pilots_run_the_plan_inside_the_slot(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    job = prepared_job(
        tmp_path,
        monkeypatch,
        job_plan(8, "m67entry"),
        site="lxplus",
        image=IMAGE,
        env=fake_venv(tmp_path / "env"),
        n_pilots=2,
        pilots="local",
        min_pilots=2,
    )
    code, driver_pid, err = run_driver_popen(job)
    assert code == 0, err
    ok, result = read_result(job)
    assert ok is True and isinstance(result, ExecResult), (ok, result)
    expected = SequentialRunner().run(concat_plan(8, "m67entry")).value
    assert pickle.dumps(result.value.text) == pickle.dumps(expected)
    assert result.value.text == expected_concat(8, "m67entry")
    pids = {pid for pid, _cluster, _url in result.value.sites}
    assert len(pids) >= 2, pids
    assert driver_pid not in pids and os.getpid() not in pids, (driver_pid, pids)
    ports = {urlsplit(url).port for _pid, _cluster, url in result.value.sites}
    low, high = htcondor_api().SITES["lxplus"].worker_ports
    assert ports and all(p is not None and low <= p <= high for p in ports), (ports, (low, high))
    assert (job / "driver.log").is_file()


def test_a_plan_error_exits_3_with_the_intact_stage_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = prepared_job(tmp_path, monkeypatch, stage_error_plan(4, "m67poison"), site="generic", n_pilots=1)
    proc = run_driver(job)
    assert proc.returncode == 3, (proc.returncode, proc.stderr)
    ok, err = read_result(job)
    assert ok is False
    assert_intact_stage_error(err)


def test_pilots_that_cannot_start_exit_1(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    job = prepared_job(tmp_path, monkeypatch, concat_plan(2, "m67bogus"), site="generic", n_pilots=2)
    proc = run_driver(job, [sys.executable, "-c", BOGUS_PYTHON_ENTRY, str(job)])
    assert proc.returncode == 1, (proc.returncode, proc.stderr)
    assert_reported_failure(job)


def test_a_missing_run_json_exits_1(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    job = prepared_job(tmp_path, monkeypatch, concat_plan(2, "m67norun"), site="generic", n_pilots=1)
    (job / "run.json").unlink()
    proc = run_driver(job)
    assert proc.returncode == 1, (proc.returncode, proc.stderr)
    assert "run.json" in str(assert_reported_failure(job))


def driver_exit(job: Path) -> int:
    try:
        code = driver_api().main([str(job)])
    except SystemExit as exc:
        code = exc.code
    return 0 if code is None else int(code)


def test_condor_pilots_reach_the_schedd_by_location_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = prepared_job(
        tmp_path,
        monkeypatch,
        job_plan(6, "m67condor"),
        site="generic",
        n_pilots=2,
        pilots="condor",
        min_pilots=2,
    )
    inner = PilotSchedd()
    fake = record_bindings(monkeypatch, inner)
    monkeypatch.chdir(job)
    monkeypatch.setenv("_CONDOR_MACHINE_AD", str(job / ".machine.ad"))
    code = run_bounded(lambda: driver_exit(job))
    assert code == 0
    locations = schedd_locations(fake.log)
    assert locations and None not in locations, f"Schedd() in the job path: {fake.log}"
    assert set(locations) == {FAKE_SCHEDD}, locations
    assert ("locate", FAKE_POOL, "Schedd", FAKE_SCHEDD) in fake.log, fake.log
    (entry,) = submits(fake.log)
    url = str(entry[1]["arguments"]).split()[0]
    assert urlsplit(url).hostname == "127.0.0.1", f"{url} is not on the machine ad host"
    ok, result = read_result(job)
    assert ok is True, result
    assert result.value.text == expected_concat(6, "m67condor")
    pids = {pid for pid, _cluster, _url in result.value.sites}
    assert len(pids) >= 2 and os.getpid() not in pids, pids
