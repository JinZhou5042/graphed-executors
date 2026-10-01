"""m68b part B2 extras: the in-job backend's SERVICE-node paths and a driver-node state the frozen suite
does not reach."""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import time
from dataclasses import replace
from http import HTTPStatus
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from graphed.core.execution import Partition, Plan
from graphed.services import Launch, ServiceSpec

from graphed_executors.htcondor_backend import (
    SITES,
    HTCondorBackend,
    LocalPilots,
    RunHandle,
    driver,
    launch,
    submit_driverless,
)
from graphed_executors.htcondor_backend.driverless import DAG_OPTIONS
from graphed_executors.htcondor_backend.server import sign


def test_a_node_that_never_announces_times_out_and_a_release_drops_its_pending_announce() -> None:
    backend = HTCondorBackend(
        LocalPilots(),
        0,
        host="127.0.0.1",
        port_range=(0, 0),
        in_job=SITES["generic"],
        announced={"web": "svc0"},
    )
    try:
        launch = Launch(argv=("{python}",), resources={"gpus": 1})
        spec = ServiceSpec("web", "http", check="http:/", launch=launch, timeout_s=0.2)
        with pytest.raises(TimeoutError, match=r"SERVICE node svc0 .*timeout_s=0\.2"):
            backend.host_service(spec, "scope")
        secret = backend._server.announce_secret(["svc0"])
        body = b"svc0 127.0.0.1:4242 node.m68b.example"
        assert backend._server.announce(body, sign(secret, body).encode()) == HTTPStatus.OK
        got = backend.host_service(spec, "scope")
        assert got == ("http://127.0.0.1:4242", "node.m68b.example", "svc0"), "control: an announce resolves"
        assert backend._server.announce(body, sign(secret, body).encode()) == HTTPStatus.OK
        backend.release_service("svc0")
        with pytest.raises(TimeoutError, match="svc0"):
            backend.host_service(spec, "scope")
    finally:
        backend.close()


class _Schedd:
    """A running DAGMan job whose driver node's queue ad is ``node``."""

    def __init__(self, node: dict[str, Any]) -> None:
        self.node = node

    def query(self, constraint: str, projection: list[str]) -> list[dict[str, Any]]:
        return [dict(self.node)] if "DAGNodeName" in constraint else [{"JobStatus": 2}]


def test_a_driver_node_held_only_while_its_input_spools_is_queued(tmp_path: Path) -> None:
    handle = RunHandle(site="generic", schedd="s", cluster=7, log_dir=tmp_path, submitted_at=0.0, dag=True)
    assert handle._poll(_Schedd({"JobStatus": 5, "HoldReasonCode": 16}))[0] == "queued"
    assert handle._poll(_Schedd({"JobStatus": 5, "HoldReasonCode": 12}))[0] == "held", "control: a real hold"


def _leaf(partition: Partition, resources: object) -> tuple[str, ...]:
    return (partition.uri,)


def _concat(a: tuple[str, ...], b: tuple[str, ...]) -> tuple[str, ...]:
    return a + b


def _empty() -> tuple[str, ...]:
    return ()


REFUSED_AFTER_MKDTEMP = ["log_dir", "service-input", "user_modules"]


@pytest.mark.parametrize("log_dir", [None, ""])
@pytest.mark.parametrize("case", REFUSED_AFTER_MKDTEMP)
def test_a_refused_run_leaves_no_temporary_log_dir(
    case: str, log_dir: str | None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    temp = tmp_path / "tmp"
    temp.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(temp))
    (tmp_path / "mod.py").write_text("X = 1\n")
    gpu = Launch(argv=("{python}",), resources={"gpus": 1})
    inputs = (str(tmp_path / "missing"),) if case == "service-input" else ()
    spec = ServiceSpec("web", "http", check="http:/", launch=replace(gpu, inputs=inputs))
    root = {"log_dir": tmp_path / "root", "service-input": "/", "user_modules": temp}[case]
    monkeypatch.setitem(SITES, "m68b-x", replace(SITES["generic"], name="m68b-x", job_root=str(root)))
    services = () if case == "log_dir" else (spec,)
    plan = Plan(process=_leaf, combine=_concat, empty=_empty, tasks=(), services=services)
    kwargs: dict[str, Any] = {
        "site": "m68b-x",
        "request_memory_mb": 1024,
        "user_modules": [tmp_path / "mod.py"],
        "log_dir": log_dir,
    }
    if case == "log_dir":
        kwargs["pilots"] = "condor"
    with pytest.raises(ValueError, match=case.replace("service-input", "does not exist")):
        submit_driverless(plan, **kwargs)
    assert list(temp.iterdir()) == []

    def no_bindings() -> None:
        raise RuntimeError("no bindings here")

    monkeypatch.setitem(SITES, "m68b-x", replace(SITES["generic"], name="m68b-x"))
    monkeypatch.setattr(launch, "_htcondor", no_bindings)
    with pytest.raises(RuntimeError, match="no bindings here"):
        submit_driverless(
            Plan(process=_leaf, combine=_concat, empty=_empty, tasks=()),
            site="m68b-x",
            request_memory_mb=1024,
        )
    assert len(list(temp.iterdir())) == 1, "control: a run past its refusals keeps its temporary log_dir"


class _Submits:
    """A schedd that records each submit description."""

    def __init__(self, submitted: list[dict[str, str]]) -> None:
        self.submitted = submitted

    def submit(self, desc: dict[str, str], count: int = 0, spool: bool = False) -> Any:
        self.submitted.append(desc)
        return SimpleNamespace(cluster=lambda: 7)

    def query(self, constraint: str = "", projection: Any = None) -> list[Any]:
        return []


def test_driverless_pilots_take_the_run_s_extra_submit_last(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    submitted: list[dict[str, str]] = []
    fake = SimpleNamespace(
        param={"SCHEDD_HOST": "s1", "COLLECTOR_HOST": "pool"},
        Submit=dict,
        Schedd=lambda ad=None: _Submits(submitted),
        Collector=lambda pool=None: SimpleNamespace(locate=lambda kind, name: {"Name": name}),
        DaemonType=SimpleNamespace(Schedd="schedd"),
    )
    monkeypatch.setattr(launch, "_htcondor", lambda: fake)
    monkeypatch.setattr(driver, "host_identity", lambda: "127.0.0.1")
    site = replace(SITES["generic"], name="m68b-x", submit={"+DesiredOS": '"EL9"'})
    monkeypatch.setitem(SITES, "m68b-x", site)
    extra = {"+DesiredOS": '"EL8"', "request_disk": "20G"}
    plan = Plan(process=_leaf, combine=_concat, empty=_empty, tasks=())
    job = tmp_path / "run"
    submit_driverless(
        plan, site="m68b-x", pilots="condor", request_memory_mb=1024, log_dir=job, extra_submit=extra
    )
    run = json.loads((job / "run.json").read_text())
    with open(tmp_path / "driver.log", "a") as log:
        driver._runner(run, job, log).close()
        run.pop("extra_submit", None)  # a run.json written before the field: the site's keys alone
        driver._runner(run, job, log).close()
    driver_job, pilots, plain = submitted
    assert {key: driver_job[key] for key in extra} == extra, driver_job
    assert {key: pilots[key] for key in extra} == extra, pilots
    assert plain["+DesiredOS"] == '"EL9"' and "request_disk" not in plain, "control: " + str(plain)


def _gpu_plan() -> Plan[tuple[str, ...]]:
    gpu = Launch(argv=("{python}",), resources={"gpus": 1})
    web = ServiceSpec("web", "http", check="http:/", launch=gpu)
    return Plan(process=_leaf, combine=_concat, empty=_empty, tasks=(), services=(web,))


class _SubmitType:
    """``htcondor2.Submit``: a call's ``issue_credentials`` and each ``from_dag``'s options are recorded."""

    def __init__(self, bindings: _DagBindings) -> None:
        self.bindings = bindings

    def __call__(self, desc: dict[str, str]) -> Any:
        return SimpleNamespace(issue_credentials=lambda: self.bindings.log.append("issue"))

    def from_dag(self, dag: str, options: dict[str, Any]) -> dict[str, str]:
        self.bindings.options.append(options)
        return {"dag_file": dag}


class _DagBindings:
    """Bindings whose schedd's ``BIN`` is ``/schedd/bin`` and whose credd holds ``stored``; ``log`` records
    the ``RemoteParam`` lookups, credential calls and submits in order."""

    param = {"SCHEDD_HOST": "s1", "COLLECTOR_HOST": "pool"}  # noqa: RUF012  (read only)
    DaemonType = SimpleNamespace(Schedd="schedd")
    CredType = SimpleNamespace(Kerberos="krb")

    def __init__(self, stored: str | None = "1790802437") -> None:
        self.stored = stored
        self.log: list[str] = []
        self.options: list[dict[str, Any]] = []
        self.Submit = _SubmitType(self)

    def Collector(self, pool: str | None = None) -> Any:
        return SimpleNamespace(locate=lambda kind, name: {"Name": name})

    def RemoteParam(self, ad: dict[str, str]) -> dict[str, str]:
        self.log.append(f"RemoteParam {ad['Name']}")
        return {"BIN": "/schedd/bin"}

    def Credd(self) -> Any:
        return SimpleNamespace(query_user_cred=self._query)

    def _query(self, kind: str) -> str | None:
        self.log.append(f"query {kind}")
        return self.stored

    def Schedd(self, ad: Any = None) -> Any:
        return SimpleNamespace(submit=self._submit)

    def _submit(self, desc: dict[str, str], count: int = 0, spool: bool = False) -> Any:
        self.log.append(f"submit {sorted(desc)}")
        return SimpleNamespace(cluster=lambda: 7)


def test_the_dag_names_the_schedd_s_condor_dagman(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _DagBindings()
    monkeypatch.setattr(launch, "_htcondor", lambda: fake)
    handle = submit_driverless(_gpu_plan(), site="generic", request_memory_mb=1024, log_dir=tmp_path / "logs")
    assert handle.dag
    assert fake.options == [{**DAG_OPTIONS, "dagman": "/schedd/bin/condor_dagman"}]
    assert fake.log == ["RemoteParam s1", "submit ['dag_file']"]


@pytest.mark.parametrize(("stored", "issued"), [(None, ["issue"]), ("1790802437", [])])
def test_a_dag_whose_nodes_send_a_credential_stores_one_first(
    stored: str | None, issued: list[str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = _DagBindings(stored)
    monkeypatch.setattr(launch, "_htcondor", lambda: fake)
    site = replace(SITES["generic"], name="m68b-x", submit={"MY.SendCredential": "True"})
    monkeypatch.setitem(SITES, "m68b-x", site)
    submit_driverless(_gpu_plan(), site="m68b-x", request_memory_mb=1024, log_dir=tmp_path / "logs")
    assert fake.log == ["RemoteParam s1", "query krb", *issued, "submit ['dag_file']"]


def test_the_dagman_job_runs_the_schedd_s_condor_dagman_not_the_one_on_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    htcondor2: Any = pytest.importorskip("htcondor2", reason="the live pool leg runs in test-htcondor")
    decoy = tmp_path / "decoy" / "condor_dagman"
    decoy.parent.mkdir()
    decoy.write_text("#!/bin/sh\nexit 1\n")
    decoy.chmod(0o755)
    monkeypatch.setenv("PATH", f"{decoy.parent}{os.pathsep}{os.environ['PATH']}")
    assert shutil.which("condor_dagman") == str(decoy), "control: from_dag alone would name the decoy"
    handle = submit_driverless(_gpu_plan(), site="generic", request_memory_mb=1024, log_dir=tmp_path / "logs")
    schedd = htcondor2.Schedd()
    run = f"ClusterId == {handle.cluster} || DAGManJobId == {handle.cluster}"
    try:
        (ad,) = schedd.query(f"ClusterId == {handle.cluster}", ["Cmd"])
        located = htcondor2.Collector().locate(htcondor2.DaemonType.Schedd, handle.schedd)
        assert ad["Cmd"] == f"{htcondor2.RemoteParam(located)['BIN']}/condor_dagman"
    finally:
        handle.remove()
        deadline = time.monotonic() + 120
        while schedd.query(run, ["ClusterId"]) and time.monotonic() < deadline:
            time.sleep(1)
    assert schedd.query(run, ["ClusterId"]) == [], "the run's jobs outlived its removal"
