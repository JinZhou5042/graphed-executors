"""m67 driver-entry and launcher paths the frozen suite does not reach: the host fallback, an outcome
that does not pickle, the default schedd's name, ``service_hosts`` with worker ports alone, the exit
code of a run whose workers were lost, a failing close, the entry run under ``-W error``, and the
pilot cluster in ``driver.log``."""

from __future__ import annotations

import io
import json
import pickle
import socket
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from graphed.core.execution import ExecResult, Partition, Plan, Task

from graphed_executors.htcondor_backend import (
    CondorPilots,
    HTCondorBackend,
    SiteProfile,
    backend,
    driver,
    launch,
    server,
)
from graphed_executors.htcondor_backend.driverless import DRIVER_MODULE
from graphed_executors.htcondor_backend.sites import SITES
from graphed_executors.submit import services


def test_the_driver_host_falls_back_to_this_host(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(socket, "getfqdn", lambda: "fallback.example")
    monkeypatch.delenv("_CONDOR_MACHINE_AD", raising=False)
    assert services.host_identity() == "fallback.example"
    ad = tmp_path / ".machine.ad"
    ad.write_text('Name = "slot1@x"\n')
    monkeypatch.setenv("_CONDOR_MACHINE_AD", str(ad))
    assert services.host_identity() == "fallback.example"
    ad.write_text('Name = "slot1@x"\nMachine = "node7.example"\n')
    assert services.host_identity() == "node7.example"


class Unpicklable(Exception):
    def __reduce__(self) -> Any:
        raise TypeError("no pickling")


def test_an_unpicklable_outcome_comes_back_as_text() -> None:
    ok, err = pickle.loads(driver._result_blob(False, Unpicklable("lost handle")))
    assert ok is False and type(err) is RuntimeError
    assert "Unpicklable did not round-trip through pickle (TypeError: no pickling): lost handle" in str(err)


def test_the_default_schedd_is_named_by_its_own_ad() -> None:
    located: list[Any] = []

    class Collector:
        def __init__(self, pool: str | None = None) -> None:
            self.pool = pool

        def locate(self, daemon: str, name: str | None = None) -> dict[str, str]:
            located.append((self.pool, daemon, name))
            return {"Name": "local-schedd"}

    kinds = SimpleNamespace(Schedd="schedd")
    fake = SimpleNamespace(param={}, Collector=Collector, DaemonType=kinds, Schedd=lambda: "default")
    assert CondorPilots(SITES["generic"])._choose(fake) == ("local-schedd", "default")
    assert located == [(None, "schedd", None)]


def test_service_hosts_follow_the_port_rows() -> None:
    only_workers = SiteProfile(
        name="w",
        submit={},
        spool=False,
        ship_env=False,
        sandbox_root=None,
        schedd_query=None,
        worker_ports=(1, 2),
    )
    assert only_workers.service_hosts == ("cluster",)


PLAN_MODULE = """
import os


def die(partition, resources):
    os._exit(9)


def fail(partition, resources):
    raise ValueError("negative pt")


def ok(partition, resources):
    return "x"


def concat(a, b):
    return a + b


def empty():
    return ""
"""


def write_job(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, process: str) -> None:
    monkeypatch.setattr(server, "LEASE_S", 2.0)
    (tmp_path / "m67_exit_plan.py").write_text(PLAN_MODULE)
    monkeypatch.syspath_prepend(str(tmp_path))
    module = __import__("m67_exit_plan")
    parts = [Partition(f"mem://m67exit/{i}", "", i, i + 1) for i in range(2)]
    plan = Plan(
        process=getattr(module, process),
        combine=module.concat,
        empty=module.empty,
        tasks=tuple(Task(i, p) for i, p in enumerate(parts)),
    )
    (tmp_path / "plan.pkl").write_bytes(pickle.dumps(plan))
    run = {"pilots": "local", "site": "generic", "n_pilots": 2, "min_pilots": 1, "retries": 0}
    (tmp_path / "run.json").write_text(json.dumps({**run, "max_in_flight": 2}))


@pytest.mark.parametrize(
    ("process", "code", "cause"), [("die", 1, "KilledWorker"), ("fail", 3, "ValueError")]
)
def test_lost_workers_exit_1_and_a_plan_error_exits_3(
    process: str, code: int, cause: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write_job(tmp_path, monkeypatch, process)
    assert driver.main([str(tmp_path)]) == code
    ok, err = pickle.loads((tmp_path / "result.pkl").read_bytes())
    assert ok is False and getattr(err, "cause_type", type(err).__name__) == cause, err


def test_pilots_lost_after_the_driver_waits_exit_1(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    write_job(tmp_path, monkeypatch, "ok")
    wait = HTCondorBackend.wait_for_pilots
    waits: list[int] = []

    def wait_then_lose_all(self: HTCondorBackend, n: int, timeout: float = backend.N_WORKERS_WAIT_S) -> int:
        waits.append(n)
        if len(waits) > 1:  # a second wait sees no pilot; cut its timeout so it fails fast
            return wait(self, n, timeout=3.0)
        live = wait(self, n, timeout)
        assert isinstance(self.launcher, launch.LocalPilots)
        for proc in self.launcher._procs:
            proc.kill()
            proc.wait()
        while self._server.live_pilots():
            time.sleep(0.05)
        return live

    monkeypatch.setattr(HTCondorBackend, "wait_for_pilots", wait_then_lose_all)
    assert driver.main([str(tmp_path)]) == 1
    ok, err = pickle.loads((tmp_path / "result.pkl").read_bytes())
    assert ok is False and getattr(err, "cause_type", None) == "KilledWorker", err


def _unused_leaf(partition: Partition, resources: object) -> int:
    return 0


class ClosingRunner:
    backend = None  # the plan declares no services, so the driver's service set never reaches it
    services = None

    def __init__(self, outcome: object) -> None:
        self.outcome = outcome

    def wait_for_pilots(self) -> int:
        return 1

    def run(self, plan: object) -> object:
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return self.outcome

    def close(self) -> None:
        raise OSError("schedd went away")


@pytest.mark.parametrize(
    ("outcome", "code"),
    [(ExecResult(value="text", n_partitions=1, n_combines=0), 0), (ValueError("negative pt"), 3)],
)
def test_a_failing_close_keeps_the_run_outcome(
    outcome: object, code: int, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "run.json").write_text(json.dumps({"min_pilots": 1}))
    (tmp_path / "plan.pkl").write_bytes(pickle.dumps(Plan(process=_unused_leaf, combine=max, empty=int)))
    monkeypatch.setattr(driver, "_runner", lambda run, job, log: ClosingRunner(outcome))
    assert driver.main([str(tmp_path)]) == code
    ok, payload = pickle.loads((tmp_path / "result.pkl").read_bytes())
    assert (ok, type(payload)) == (code == 0, type(outcome))
    assert "OSError: schedd went away" in (tmp_path / "driver.log").read_text()


def test_the_entry_runs_under_warnings_as_errors(tmp_path: Path) -> None:
    proc = subprocess.run(
        [sys.executable, "-W", "error", "-m", DRIVER_MODULE, str(tmp_path)],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert (proc.returncode, proc.stderr) == (1, "")
    assert "run.json" in (tmp_path / "driver.log").read_text()


class PilotSchedd:
    def submit(self, desc: Any, count: int = 0, spool: bool = False) -> Any:
        return SimpleNamespace(cluster=lambda: 5151)

    def query(self, constraint: str = "", projection: Any = None) -> list[Any]:
        return []


def test_the_driver_log_names_the_pilot_cluster(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fake = SimpleNamespace(
        param={},
        Submit=dict,
        Schedd=lambda ad=None: PilotSchedd(),
        Collector=lambda pool=None: SimpleNamespace(locate=lambda kind, name: {"Name": name}),
        DaemonType=SimpleNamespace(Schedd="schedd"),
    )
    monkeypatch.setattr(launch, "_htcondor", lambda: fake)
    monkeypatch.setattr(driver, "host_identity", lambda: "127.0.0.1")
    run = {
        "pilots": "condor",
        "site": "generic",
        "n_pilots": 1,
        "image": None,
        "request_memory_mb": 1,
        "log_dir": str(tmp_path / "pilots"),
        "user_modules": [],
        "schedd_locate": ["pool", "s1"],
        "min_pilots": 1,
        "retries": 0,
        "max_in_flight": 1,
    }
    log = io.StringIO()
    driver._runner(run, tmp_path, log).close()
    assert "cluster=('s1', 5151)" in log.getvalue(), log.getvalue()
