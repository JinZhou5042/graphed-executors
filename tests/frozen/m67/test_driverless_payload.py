"""m67 D5/D6: what ``submit_driverless`` ships, on data only (bindings recorded, every OS).

With ``launch._htcondor`` recorded, one call writes ``plan.pkl`` and ``run.json`` and submits ONE job
whose description carries the driverless keys. The payload is witnessed where it runs: the job's
scratch dir is built from ``transfer_input_files`` alone, and a fresh interpreter there (no
``PYTHONPATH``) unpickles ``plan.pkl`` with stdlib pickle and reproduces the sequential result
bit-for-bit. The refusals (a lambda or ``__main__`` process, ``pilots="condor"`` on a profile without
``worker_ports``, an lpc ``log_dir`` outside the sandbox, a non-AFS ``log_dir`` for lxplus
self-submission) raise before any bindings call. The site rows carry the measured D4 port ranges.

Discriminates an unshipped ``run.json`` or user module, a pickle only this process can load, a retried
plan error (``retry_until`` missing), a fat slot sized wrong, outputs that never come back, a refusal
that is late, absent or over-broad, and a ``schedd_locate`` the job cannot use."""

from __future__ import annotations

import dataclasses
import json
import os
import pickle
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from driverless_harness import (
    FAKE_CLUSTER,
    FAKE_POOL,
    FAKE_SCHEDD,
    HARNESS_FILE,
    RecordingSchedd,
    concat_plan,
    expected_concat,
    fake_venv,
    htcondor_api,
    input_entries,
    job_dir,
    record_bindings,
    submits,
)
from graphed.core.execution import Partition, Plan, SequentialRunner

IMAGE = "/cvmfs/unpacked.cern.ch/registry.hub.docker.com/coffeateam/coffea-almalinux9-noml:2026.9.0-py3.12"

PROBE = """
import pickle, sys
from graphed.core.execution import SequentialRunner
with open("plan.pkl", "rb") as f:
    plan = pickle.load(f)
sys.stdout.buffer.write(pickle.dumps((SequentialRunner().run(plan).value, sys.modules[plan.process.__module__].__file__)))
"""


def submit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, plan: Any, **kwargs: Any
) -> tuple[Any, Any, Path]:
    """``submit_driverless`` under the recorder: (handle, the fake bindings, log_dir)."""
    log_dir = tmp_path / "submit"
    log_dir.mkdir(parents=True)
    fake = record_bindings(monkeypatch, RecordingSchedd())
    kwargs.setdefault("request_memory_mb", 3000)
    handle = htcondor_api().submit_driverless(plan, log_dir=log_dir, **kwargs)
    return handle, fake, log_dir


def only_description(fake: Any) -> dict[str, Any]:
    entries = submits(fake.log)
    assert len(entries) == 1, f"{len(entries)} submits"
    _, desc, count, _spool = entries[0]
    assert count in (0, 1), f"the driver is ONE job, submitted with count={count}"
    return dict(desc)


def test_local_payload_runs_in_a_fresh_interpreter_from_the_job_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan = concat_plan(5, "m67payload")
    handle, fake, log_dir = submit(
        tmp_path, monkeypatch, plan, site="generic", n_pilots=2, pilots="local", user_modules=[HARNESS_FILE]
    )
    desc = only_description(fake)
    job = job_dir(desc, log_dir, tmp_path / "job")
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    proc = subprocess.run(
        [sys.executable, "-c", PROBE], cwd=job, env=env, capture_output=True, timeout=120, check=False
    )
    assert proc.returncode == 0, proc.stderr.decode()
    value, module_file = pickle.loads(proc.stdout)
    assert pickle.dumps(value) == pickle.dumps(SequentialRunner().run(concat_plan(5, "m67payload")).value)
    assert value == expected_concat(5, "m67payload")
    assert Path(module_file).resolve().parent == job.resolve(), module_file
    run = json.loads((job / "run.json").read_text())
    assert (run["pilots"], run["n_pilots"], run["site"]) == ("local", 2, "generic"), run
    assert run["schedd_locate"] is None, run
    assert (handle.site, handle.schedd, handle.cluster) == ("generic", FAKE_SCHEDD, FAKE_CLUSTER)


def test_local_description_carries_the_driverless_keys(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, fake, log_dir = submit(
        tmp_path, monkeypatch, concat_plan(3, "m67keys"), site="generic", n_pilots=3, pilots="local"
    )
    desc = only_description(fake)
    executable = Path(desc.get("initialdir") or log_dir, desc["executable"])
    assert executable.name == "driver.sh" and executable.is_file(), desc["executable"]
    names = {Path(e).name for e in input_entries(desc)}
    assert {"plan.pkl", "run.json"} <= names, names
    assert "env.tgz" not in names, names
    assert desc["transfer_output_files"].replace(" ", "") == "result.pkl,driver.log"
    assert str(desc["request_cpus"]) == "3", "the fat slot holds every local pilot"
    assert str(desc["request_memory"]) == "3000"
    assert str(desc["max_retries"]) == "2"
    assert str(desc["retry_until"]) == "3", "exit 3 is a plan error and must cease retries"
    assert str(desc["JobBatchName"]).startswith("graphed-driverless-"), desc["JobBatchName"]


def test_lxplus_description_ships_the_env_and_takes_extra_submit_last(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env = fake_venv(tmp_path / "env")
    _, fake, _ = submit(
        tmp_path,
        monkeypatch,
        concat_plan(2, "m67lx"),
        site="lxplus",
        image=IMAGE,
        env=env,
        n_pilots=2,
        pilots="local",
        extra_submit={"+JobFlavour": '"workday"'},
    )
    desc = only_description(fake)
    assert "env.tgz" in {Path(e).name for e in input_entries(desc)}
    assert desc["MY.SingularityImage"] == f'"{IMAGE}"'
    assert desc["MY.SendCredential"] == "True"
    assert desc["+JobFlavour"] == '"workday"'
    (_, _, _, spool) = submits(fake.log)[0]
    assert spool is True and ("spool",) in fake.log, "lxplus spools its sandbox"


def test_condor_pilots_record_where_the_job_finds_its_schedd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, fake, log_dir = submit(
        tmp_path, monkeypatch, concat_plan(2, "m67locate"), site="generic", n_pilots=4, pilots="condor"
    )
    desc = only_description(fake)
    assert str(desc["request_cpus"]) == "1", "self-submitted pilots need no fat slot"
    run = json.loads(Path(desc.get("initialdir") or log_dir, "run.json").read_text())
    assert (run["pilots"], run["n_pilots"]) == ("condor", 4), run
    assert list(run["schedd_locate"]) == [FAKE_POOL, FAKE_SCHEDD], run
    assert os.path.isabs(run["log_dir"]), run


# ---- refusals ------------------------------------------------------------------------------------


def refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, plan: Any, match: str, **kwargs: Any) -> None:
    log_dir = kwargs.pop("log_dir", tmp_path / "submit")
    fake = record_bindings(monkeypatch, RecordingSchedd())
    with pytest.raises(ValueError, match=match):
        htcondor_api().submit_driverless(plan, log_dir=log_dir, request_memory_mb=2048, **kwargs)
    assert fake.log == [], f"refused after touching the bindings: {fake.log}"


def lambda_plan() -> Plan[str]:
    base = concat_plan(2, "m67lambda")
    return dataclasses.replace(base, process=lambda partition, resources: str(partition.uri))


def main_plan(monkeypatch: pytest.MonkeyPatch) -> Plan[str]:
    def m67_main_process(partition: Partition, resources: object) -> str:
        return str(partition.uri)

    m67_main_process.__module__ = "__main__"
    m67_main_process.__qualname__ = "m67_main_process"
    monkeypatch.setattr(sys.modules["__main__"], "m67_main_process", m67_main_process, raising=False)
    return dataclasses.replace(concat_plan(2, "m67main"), process=m67_main_process)


def test_a_lambda_process_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    refused(tmp_path, monkeypatch, lambda_plan(), "user_modules", site="generic", n_pilots=1)


def test_a_main_module_process_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    refused(tmp_path, monkeypatch, main_plan(monkeypatch), "user_modules", site="generic", n_pilots=1)


def test_condor_pilots_are_refused_on_a_profile_without_worker_ports(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    api = htcondor_api()
    profile = api.SiteProfile(
        name="m67-noports", submit={}, spool=False, ship_env=False, sandbox_root=None, schedd_query=None
    )
    assert profile.worker_ports is None
    monkeypatch.setitem(api.SITES, "m67-noports", profile)
    plan = concat_plan(2, "m67noports")
    refused(tmp_path, monkeypatch, plan, "worker_ports", site="m67-noports", n_pilots=2, pilots="condor")
    # the same profile runs a fat slot: the refusal is about self-submission only
    _, fake, _ = submit(
        tmp_path / "control", monkeypatch, plan, site="m67-noports", n_pilots=2, pilots="local"
    )
    assert len(submits(fake.log)) == 1


def test_an_lpc_log_dir_outside_the_sandbox_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    refused(
        tmp_path,
        monkeypatch,
        concat_plan(2, "m67lpc"),
        "3DayLifetime",
        site="lpc",
        image=IMAGE,
        env=fake_venv(tmp_path / "env"),
        n_pilots=2,
        pilots="local",
    )


def test_lxplus_self_submission_needs_an_afs_log_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    refused(
        tmp_path,
        monkeypatch,
        concat_plan(2, "m67afs"),
        "/afs",
        site="lxplus",
        image=IMAGE,
        env=fake_venv(tmp_path / "env"),
        n_pilots=2,
        pilots="condor",
    )


# ---- site data (D4) ------------------------------------------------------------------------------


def test_site_rows_carry_the_measured_port_ranges() -> None:
    api = htcondor_api()
    lpc, lxplus, generic = api.SITES["lpc"], api.SITES["lxplus"], api.SITES["generic"]
    assert (lpc.worker_ports, lpc.service_ports) == ((10000, 10100), (10001, 10100))
    assert (lxplus.worker_ports, lxplus.service_ports) == ((10000, 10100), None)
    assert (generic.worker_ports, generic.service_ports) == ((10000, 10100), (10000, 10100))
    assert tuple(lpc.service_hosts) == ("driver", "cluster")
    assert tuple(lxplus.service_hosts) == ("cluster",)
    assert tuple(generic.service_hosts) == ("driver", "cluster")
    bare = api.SiteProfile(
        name="m67-bare", submit={}, spool=False, ship_env=False, sandbox_root=None, schedd_query=None
    )
    assert (bare.worker_ports, bare.service_ports, tuple(bare.service_hosts)) == (None, None, ())
