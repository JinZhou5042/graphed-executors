"""m67 ``RunHandle`` and ``submit_driverless`` paths the frozen suite does not reach: collector failover,
a job the schedd has forgotten, other job states, an unbounded wait, the logs a run returns."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from graphed_executors.htcondor_backend import SITES, RunHandle, SiteProfile, launch, submit_driverless


class Schedd:
    def __init__(self, location: Any = None, queue: list[Any] | None = None) -> None:
        self.location = location
        self.queue = queue or []
        self.history_calls: list[tuple[str, list[str], int]] = []

    def query(self, constraint: str, projection: list[str]) -> list[Any]:
        return self.queue

    def history(self, constraint: str, projection: list[str], match: int = -1) -> list[Any]:
        self.history_calls.append((constraint, projection, match))
        return []


def fake_htcondor(pool: str, queue: list[Any], located: list[Any]) -> Any:
    class Collector:
        def __init__(self, node: str | None = None) -> None:
            self.node = node

        def locate(self, daemon: str, name: str | None = None) -> dict[str, Any]:
            located.append((self.node, name))
            if self.node == "down":
                raise OSError("collector down")
            return {"Name": name, "via": self.node}

    kinds = SimpleNamespace(Schedd="schedd")
    schedds: list[Schedd] = []

    def make_schedd(ad: Any = None) -> Schedd:
        schedds.append(Schedd(ad, queue))
        return schedds[-1]

    return SimpleNamespace(
        param={"FERMIHTC_REMOTE_POOL": pool},
        Collector=Collector,
        DaemonType=kinds,
        Schedd=make_schedd,
        schedds=schedds,
    )


def handle(tmp_path: Path, site: str = "lpc") -> RunHandle:
    return RunHandle(site=site, schedd="s1", cluster=7, log_dir=tmp_path, submitted_at=0.0)


def test_a_handle_locates_its_schedd_through_the_next_collector(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    located: list[Any] = []
    fake = fake_htcondor("down, up", [{"JobStatus": 2}], located)
    monkeypatch.setattr(launch, "_htcondor", lambda: fake)
    assert handle(tmp_path).status() == "running"
    assert located == [("down", "s1"), ("up", "s1")]


def test_no_collector_knowing_the_schedd_is_an_error_naming_each(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = fake_htcondor("down", [], [])
    monkeypatch.setattr(launch, "_htcondor", lambda: fake)
    with pytest.raises(RuntimeError, match="schedd s1 not found: down: collector down"):
        handle(tmp_path).status()


def test_a_job_neither_queued_nor_in_history_is_an_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = fake_htcondor("up", [], [])
    monkeypatch.setattr(launch, "_htcondor", lambda: fake)
    with pytest.raises(RuntimeError, match="cluster 7 is neither in the queue of s1 nor its history"):
        handle(tmp_path).status()
    assert [call[2] for call in fake.schedds[-1].history_calls] == [1]


@pytest.mark.parametrize(("ad", "status"), [({"JobStatus": 6}, "running"), ({"JobStatus": 4}, "failed")])
def test_other_job_states(
    ad: dict[str, int], status: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = fake_htcondor("up", [ad], [])
    monkeypatch.setattr(launch, "_htcondor", lambda: fake)
    assert handle(tmp_path, site="generic").status() == status


def test_wait_without_a_timeout_returns_the_terminal_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = fake_htcondor("up", [{"JobStatus": 3}], [])
    monkeypatch.setattr(launch, "_htcondor", lambda: fake)
    assert handle(tmp_path, site="generic").wait(poll_s=0.0) == "removed"


def test_logs_are_the_driver_files_that_came_back(tmp_path: Path) -> None:
    (tmp_path / "driver.log").write_text("exit 0\n")
    (tmp_path / "driver.err").write_text("")
    (tmp_path / "plan.pkl").write_bytes(b"")
    assert handle(tmp_path).logs() == {"driver.log": "exit 0\n", "driver.err": ""}


def test_an_unknown_pilots_mode_is_refused(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="pilots='slurm'"):
        submit_driverless(cast(Any, None), pilots="slurm", request_memory_mb=1, log_dir=tmp_path)


def test_condor_pilots_are_refused_where_jobs_cannot_submit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(launch, "_htcondor", lambda: pytest.fail("a bindings call"))
    with pytest.raises(ValueError, match="site 'lpc' does not let a job submit jobs"):
        submit_driverless(cast(Any, None), site="lpc", pilots="condor", request_memory_mb=1, log_dir=tmp_path)
    assert [name for name, row in SITES.items() if not row.jobs_can_submit] == ["lpc"]
    six = SiteProfile(name="s", submit={}, spool=False, ship_env=False, sandbox_root=None, schedd_query=None)
    assert six.jobs_can_submit is True
