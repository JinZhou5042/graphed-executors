"""m68a HTCondor paths the frozen suite does not reach: a driver-job backend over self-submitted pilots
(``pilots="condor"``) takes its service ports from its row's ``worker_ports``; ``CondorPilots.stop``
still removes its cluster when counting the live pilots fails, and fetches a finished spooled job's
logs first; a driverless submit whose spool fails removes the job it queued; a failed release is
logged, not raised. No bindings: every schedd here is a recorder."""

from __future__ import annotations

import copy
import dataclasses
import logging
import pickle
from pathlib import Path
from types import MappingProxyType, SimpleNamespace
from typing import Any

import pytest
from graphed.core.execution import Partition, Plan

from graphed_executors.htcondor_backend import (
    SITES,
    CondorPilots,
    HTCondorBackend,
    RunHandle,
    SiteProfile,
    driver,
    launch,
    submit_driverless,
)
from graphed_executors.submit.services import ServiceStatus, ServiceUnavailable, release_quietly


class Schedd:
    """A recording schedd: ``query`` answers ``queue``; ``spool_raises`` makes ``spool`` raise it."""

    def __init__(self, queue: list[dict[str, Any]], spool_raises: BaseException | None = None) -> None:
        self.queue = queue
        self.spool_raises = spool_raises
        self.log: list[tuple[Any, ...]] = []

    def submit(self, desc: Any, count: int = 0, spool: bool = False) -> Any:
        self.log.append(("submit", count, spool))
        return SimpleNamespace(cluster=lambda: 77)

    def spool(self, result: Any) -> None:
        self.log.append(("spool",))
        if self.spool_raises is not None:
            raise self.spool_raises

    def query(self, constraint: str = "", projection: Any = None) -> list[dict[str, Any]]:
        self.log.append(("query", constraint))
        return list(self.queue)

    def retrieve(self, constraint: str) -> None:
        self.log.append(("retrieve", constraint))

    def act(self, action: Any, constraint: str, reason: str = "") -> None:
        self.log.append(("act", action, constraint))


def bindings(schedd: Schedd) -> Any:
    return SimpleNamespace(
        param={"SCHEDD_HOST": "s1", "COLLECTOR_HOST": "cm"},
        Submit=dict,
        Schedd=lambda ad=None: schedd,
        Collector=lambda pool=None: SimpleNamespace(locate=lambda kind, name=None: {"Name": name or "s1"}),
        DaemonType=SimpleNamespace(Schedd="schedd"),
        JobAction=SimpleNamespace(Remove="Remove"),
    )


def unused_leaf(partition: Partition, resources: object) -> int:
    return 0


def test_a_driver_job_over_its_own_pilots_serves_on_the_worker_ports(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    schedd = Schedd([])
    monkeypatch.setattr(launch, "_htcondor", lambda: bindings(schedd))
    row = dataclasses.replace(SITES["lxplus"], spool=False, ship_env=False)
    pilots = CondorPilots(row, image="img", log_dir=tmp_path, schedd_locate=("cm", "s1"))
    backend = HTCondorBackend(pilots, 1, host="127.0.0.1", port_range=(0, 0), in_job=row)
    try:
        assert backend.service_ports == row.worker_ports
        assert backend.service_hosts == ("driver",) and backend.advertise_host == "127.0.0.1"
        assert dict(backend.site_services) == dict(row.services)
    finally:
        backend.close()


def test_stop_removes_the_cluster_when_counting_pilots_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    schedd = Schedd([{"JobStatus": 4}])
    monkeypatch.setattr(launch, "_htcondor", lambda: bindings(schedd))
    pilots = CondorPilots(dataclasses.replace(SITES["generic"], spool=True), log_dir=tmp_path)
    pilots.start("http://h:1", b"s", 1)

    def alive() -> int:
        raise OSError("schedd went away (extra)")

    monkeypatch.setattr(pilots, "alive", alive)
    with caplog.at_level(logging.WARNING):
        pilots.stop()
    assert ("retrieve", "ClusterId == 77 && JobStatus == 4") in schedd.log
    assert ("act", "Remove", "ClusterId == 77") in schedd.log
    assert any("schedd went away (extra)" in str(r.exc_info[1]) for r in caplog.records if r.exc_info)


def test_a_failed_release_is_logged_not_raised(caplog: pytest.LogCaptureFixture) -> None:
    def fails() -> None:
        raise RuntimeError("release fault (extra)")

    with caplog.at_level(logging.WARNING):
        release_quietly("a thing", fails)
    (record,) = [r for r in caplog.records if r.exc_info]
    assert "releasing a thing failed" in record.getMessage()


def test_a_driverless_submit_whose_spool_fails_leaves_no_job(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    schedd = Schedd([{"JobStatus": 1}], spool_raises=RuntimeError("spool reset (extra)"))
    monkeypatch.setattr(launch, "_htcondor", lambda: bindings(schedd))
    monkeypatch.setitem(
        SITES, "m68a-spooled", dataclasses.replace(SITES["generic"], name="m68a-spooled", spool=True)
    )
    plan = Plan(process=unused_leaf, combine=max, empty=int)
    with pytest.raises(RuntimeError, match="spool reset"):
        submit_driverless(plan, site="m68a-spooled", log_dir=tmp_path, request_memory_mb=1)
    assert ("act", "Remove", "ClusterId == 77") in schedd.log


def test_a_missing_result_is_not_a_load_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    schedd = Schedd([{"JobStatus": 4, "ExitCode": 0}])
    monkeypatch.setattr(launch, "_htcondor", lambda: bindings(schedd))
    handle = RunHandle(site="generic", schedd="s1", cluster=77, log_dir=tmp_path, submitted_at=0.0)
    with pytest.raises(FileNotFoundError):
        handle.result()


def test_the_driver_restores_the_package_logger(tmp_path: Path) -> None:
    package = logging.getLogger("graphed_executors")
    before = (package.level, list(package.handlers))
    assert driver.main([str(tmp_path)]) == 1  # no run.json: a setup failure
    assert (package.level, list(package.handlers)) == before
    assert "exit 1" in (tmp_path / "driver.log").read_text()


@pytest.mark.parametrize("name", [*sorted(SITES), "synthetic"])
def test_site_rows_pickle_copy_and_replace(name: str) -> None:
    """``services`` is a mappingproxy (the frozen-mapping defaults rule); the row still pickles,
    deep-copies and replaces, as graphed's ``Launch`` does, and comes back read-only."""
    row = SITES.get(name) or SiteProfile(
        name="m68a-synthetic",
        submit={"k": "v"},
        spool=False,
        ship_env=False,
        sandbox_root=None,
        schedd_query=None,
        service_ports=(20001, 20010),
        worker_ports=(20000, 20010),
        services={"triton": "grpcs://triton.m68a.example:443", "http": "http://web.m68a.example:80"},
    )
    for copied in (pickle.loads(pickle.dumps(row)), copy.deepcopy(row), dataclasses.replace(row)):
        assert copied == row and dict(copied.services) == dict(row.services)
        assert isinstance(copied.services, MappingProxyType)
    moved = dataclasses.replace(row, services={"http": "http://other.m68a.example:81"})
    assert dict(pickle.loads(pickle.dumps(moved)).services) == {"http": "http://other.m68a.example:81"}


def test_a_refusal_with_read_only_legs_still_reaches_the_result_blob() -> None:
    legs = MappingProxyType({"user": "no endpoint given", "managed": "no launch recipe"})
    ok, exc = pickle.loads(driver._result_blob(False, ServiceUnavailable("web", legs)))
    assert ok is False and type(exc) is ServiceUnavailable and exc.legs == dict(legs)
    status = ServiceStatus("web", "user", None, "http://h:1", None, None, 1.0)
    assert pickle.loads(pickle.dumps(status)) == status and copy.deepcopy(status) == status
