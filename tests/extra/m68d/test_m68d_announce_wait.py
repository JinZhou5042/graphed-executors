"""m68d A paths the frozen suite does not reach: a DAG driver over condor pilots counts the runner's running
pilots against a SERVICE node's room and holds its queued pilots while the node waits (over the recorded
bindings, every OS, and once on a pool, where a rerun holds and releases a queued pilot); an idle node on a
pool whose collector lists no slot waits for its announce unmatched."""

from __future__ import annotations

import logging
import os
import re
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest
from graphed.core.execution import SequentialRunner

from graphed_executors.htcondor_backend import CondorPilots, HTCondorBackend, submit_driverless
from graphed_executors.htcondor_backend import server as server_mod
from graphed_executors.htcondor_backend.launch import CondorReason

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "frozen" / "m68d"))
from m68d_harness import (
    CHILD_FILE,
    DAG_ID,
    FAKE_POOL,
    FAKE_SCHEDD,
    HARNESS_FILE,
    NODE,
    SERVICE_ID,
    FakeAd,
    FakeHTCondor,
    GatedGet,
    NodeSchedd,
    Web,
    announce_later,
    child_starts,
    fake_classad2,
    free_range,
    gated_plan,
    matches,
    names_the_node,
    node_ad,
    node_spec,
    raised_by,
    record_bindings,
    run_bounded,
    site_copy,
    wait_for,
    write_job_ad,
    write_machine_ad,
)

LOCATE = (FAKE_POOL, FAKE_SCHEDD)
IDLE = 1


class PilotSchedd:
    """A :class:`NodeSchedd` over ``phases`` that also holds the runner's pilot jobs (``pilots``, cleared
    before a close)."""

    def __init__(self, phases: list[tuple[float, Any]]) -> None:
        self.node = NodeSchedd(phases)
        self.log = self.node.log
        self.pilots: list[Any] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self.node, name)

    def query(self, constraint: Any = "true", projection: Any = None, **kwargs: Any) -> list[Any]:
        text, proj = (
            str(kwargs.get("constraint", constraint)),
            tuple(kwargs.get("projection", projection) or ()),
        )
        rows = [FakeAd({k: job[k] for k in proj if k in job}) for job in self.pilots if matches(job, text)]
        return [*self.node.query(constraint, projection, **kwargs), *rows]


def running_pilot() -> FakeAd:
    """A pilot of the runner's cluster (the recorded submit answers ``DAG_ID``) holding 4000 MiB of the slot."""
    return FakeAd(
        ClusterId=DAG_ID, ProcId=0, JobStatus=2, RemoteHost="slot1_2@m-node.example", MemoryProvisioned=4000
    )


@contextmanager
def condor_dag_backend(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, schedd: PilotSchedd
) -> Iterator[tuple[HTCondorBackend, FakeHTCondor]]:
    """A DAG driver node's backend over condor pilots (none submitted yet) on the recorded bindings."""
    monkeypatch.setattr(server_mod, "POLL_S", 0.2)
    monkeypatch.setitem(sys.modules, "classad2", fake_classad2())
    monkeypatch.setenv("_CONDOR_MACHINE_AD", str(write_machine_ad(tmp_path / "machine.ad", "127.0.0.1")))
    monkeypatch.setenv("_CONDOR_JOB_AD", str(write_job_ad(tmp_path / "job.ad")))
    fake = record_bindings(monkeypatch, FakeHTCondor(schedd))
    profile = site_copy(monkeypatch, "m-dee-node", "generic", worker_ports=free_range(3))
    pilots = CondorPilots(profile, schedd_locate=LOCATE, log_dir=tmp_path / "pilots")
    backend = HTCondorBackend(
        pilots,
        1,
        host="127.0.0.1",
        port_range=(0, 0),
        in_job=profile,
        announced={"web": NODE},
        schedd_locate=LOCATE,
        dag_dir=tmp_path / "dag",
    )
    try:
        yield backend, fake
    finally:
        schedd.pilots.clear()  # nothing left alive for the pilots' stop to wait on
        run_bounded(backend.close, 60.0)


def acts(fake: FakeHTCondor) -> list[tuple[str, str]]:
    return [(e[1], e[2]) for e in fake.log if e[0] == "act"]


def test_a_rerun_s_node_match_counts_the_running_pilots_and_holds_the_queued_ones(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    schedd = PilotSchedd([(0.0, node_ad(IDLE, memory_mb=6000))])
    with condor_dag_backend(tmp_path, monkeypatch, schedd) as (backend, fake):
        backend._need()  # the first set's run submitted the pilots
        schedd.pilots.append(running_pilot())
        with backend.starting_services():
            err = raised_by(lambda: backend.host_service(node_spec(600.0), "m-dee-scope"), 20.0)
            held = acts(fake)
        released = acts(fake)[len(held) :]
    text = " ".join([str(err), *getattr(err, "legs", {}).values()])
    assert re.search(r"RequestMemory\D{0,16}6000\b", text) and "the runner's running pilots" in text, repr(
        err
    )
    assert held == [("Hold", f"ClusterId == {DAG_ID} && JobStatus == 1")], fake.log
    assert [action for action, _ in released] == ["Release"], fake.log


def test_before_the_pilots_are_submitted_the_same_node_fits_and_nothing_is_held(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    schedd = PilotSchedd([(0.0, node_ad(IDLE, memory_mb=6000))])
    with Web() as web, condor_dag_backend(tmp_path, monkeypatch, schedd) as (backend, fake):
        publish_pair(backend, tmp_path / "dag")
        announce_later(tmp_path / "dag", web.port, 1.0)
        got = run_bounded(lambda: backend.host_service(node_spec(600.0), "m-dee-scope"), 20.0)
    assert tuple(got) == (web.endpoint, SERVICE_ID, NODE), got
    assert acts(fake) == [], fake.log


def test_an_idle_node_on_a_pool_listing_no_slot_waits_for_its_announce(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    schedd = PilotSchedd([(0.0, node_ad(IDLE, memory_mb=10**6))])
    with Web() as web, condor_dag_backend(tmp_path, monkeypatch, schedd) as (backend, fake):
        fake.machines = []
        publish_pair(backend, tmp_path / "dag")
        announce_later(tmp_path / "dag", web.port, 1.0)
        got = run_bounded(lambda: backend.host_service(node_spec(0.2), "m-dee-scope"), 20.0)
    assert tuple(got) == (web.endpoint, SERVICE_ID, NODE), got
    assert [e for e in fake.log if e[0] == "collector-query"], fake.log


def publish_pair(backend: HTCondorBackend, dag: Path) -> None:
    """The driver's url and svc0's announce secret in ``dag``, as ``driver._runner`` publishes them."""
    dag.mkdir(parents=True, exist_ok=True)
    (dag / "graphed-secret").write_text(backend._server.announce_secret([NODE]).hex())
    (dag / "driver.url").write_text(backend._server.url)


class HistorySchedd(PilotSchedd):
    """A :class:`PilotSchedd` whose history answers ``rows``, or raises ``refuse``; asks are logged."""

    def __init__(self, phases: list[tuple[float, Any]], rows: list[Any], refuse: str | None = None) -> None:
        super().__init__(phases)
        self.rows, self.refuse = rows, refuse

    def history(
        self, constraint: Any = None, projection: Any = None, match: int = -1, **kwargs: Any
    ) -> list[Any]:
        self.log.append(("history", str(constraint), tuple(projection or ()), match))
        if self.refuse is not None:
            raise RuntimeError(self.refuse)
        return self.rows


DEPARTED: dict[str, tuple[list[Any], str | None, str]] = {
    "held-then-removed": (
        [FakeAd(JobStatus=3, LastHoldReason="m-held-before-it-announced", RemoveReason="m-periodic-remove")],
        None,
        "JobStatus=3, LastHoldReason='m-held-before-it-announced';",
    ),
    "removed": (
        [FakeAd(JobStatus=3, RemoveReason="m-removed-by-hand")],
        None,
        "JobStatus=3, RemoveReason='m-removed-by-hand';",
    ),
    "no-history-row": ([], None, "no job ad;"),
    "unreadable-history": ([], "m-history-refused", "no job ad;"),
}


@pytest.mark.parametrize("case", sorted(DEPARTED))
def test_a_departed_node_is_named_by_its_history_row(
    case: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    rows, refuse, state = DEPARTED[case]
    schedd = HistorySchedd([(0.0, None)], rows, refuse)
    with (
        caplog.at_level(logging.WARNING, logger="graphed_executors"),
        condor_dag_backend(tmp_path, monkeypatch, schedd) as (backend, _fake),
    ):
        err = raised_by(lambda: backend.host_service(node_spec(600.0), "m-dee-scope"), 20.0)
    assert isinstance(err, RuntimeError) and f"({NODE}) ended before it announced: {state}" in str(err), repr(
        err
    )
    asks = [e for e in schedd.log if e[0] == "history"]
    assert asks and {e[3] for e in asks} == {1} and all(names_the_node(e[1]) for e in asks), asks
    warned = [r.getMessage() for r in caplog.records if NODE in r.getMessage()]
    if rows:
        assert warned == [], warned
    else:
        assert len(warned) == 1 and (refuse or "no history row") in warned[0], warned


HOLD_REASON = "m68d test: svc0 held before it announces"


def test_on_a_pool_a_node_held_and_removed_before_the_driver_looks_is_named_by_its_hold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    htcondor2: Any = pytest.importorskip("htcondor2", reason="the live pool leg runs in test-htcondor")
    argv = ("{python}", Path(CHILD_FILE).name, "serve", "{port}", str(tmp_path / "child"))
    monkeypatch.chdir(tmp_path)
    (tmp_path / "logs").mkdir()
    handle = submit_driverless(
        gated_plan(tmp_path / "mark", node_spec(600.0, argv, inputs=(CHILD_FILE,))),
        site="generic",
        n_pilots=1,
        pilots="local",
        request_memory_mb=1024,
        log_dir=tmp_path / "logs",
        user_modules=[HARNESS_FILE],
    )
    schedd = htcondor2.Schedd()
    run = f"ClusterId == {handle.cluster} || DAGManJobId == {handle.cluster}"
    node = f'DAGManJobId == {handle.cluster} && DAGNodeName == "{NODE}"'
    driver = f'DAGManJobId == {handle.cluster} && DAGNodeName == "driver"'
    both = f"({node}) || ({driver})"
    try:
        assert wait_for(lambda: len(schedd.query(both, ["ClusterId"])) == 2, 120.0, 0.1), "no nodes"
        schedd.act(htcondor2.JobAction.Hold, both, reason=CondorReason(HOLD_REASON))
        started = [ad for ad in schedd.query(both, ["DAGNodeName", "NumJobStarts"]) if ad.get("NumJobStarts")]
        assert started == [], f"a node started before the hold: {started}"
        assert wait_for(lambda: not schedd.query(node, ["ClusterId"]), 180.0, 2.0), "svc0 stayed queued"
        schedd.act(htcondor2.JobAction.Release, driver)
        status = run_bounded(lambda: handle.wait(timeout=600.0, poll_s=2), 660.0)
        log = str(handle.logs().get("driver.log", ""))
        assert status == "failed", log[-4000:]
        named = (
            rf"\({NODE}\) ended before it announced: JobStatus=3, LastHoldReason='{re.escape(HOLD_REASON)}"
        )
        assert re.search(named, log), log[-4000:]
    finally:
        if schedd.query(run, ["ClusterId"]):
            schedd.act(htcondor2.JobAction.Remove, run)
        assert wait_for(lambda: not schedd.query(run, ["ClusterId"]), 180.0, 2.0), (
            "the run outlived its removal"
        )


def test_on_a_pool_a_rerun_holds_the_queued_condor_pilot_and_releases_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    htcondor2: Any = pytest.importorskip("htcondor2", reason="the live pool leg runs in test-htcondor")
    slots = htcondor2.Collector().query(htcondor2.AdType.Startd, projection=["TotalSlotMemory", "Memory"])
    room = max(int(ad.get("TotalSlotMemory") or ad.get("Memory") or 0) for ad in slots)
    each = int(0.4 * room)  # the driver and one pilot fit beside svc0; the second pilot stays queued
    mark, report = tmp_path / "mark", tmp_path / "child"
    argv = ("{python}", Path(CHILD_FILE).name, "serve", "{port}", str(report))
    spec = node_spec(180.0, argv, memory_mb=256, inputs=(CHILD_FILE,))
    monkeypatch.chdir(tmp_path)
    (tmp_path / "logs").mkdir()
    handle = submit_driverless(
        gated_plan(mark, spec),
        site="generic",
        n_pilots=2,
        pilots="condor",
        request_memory_mb=each,
        log_dir=tmp_path / "logs",
        user_modules=[HARNESS_FILE],
    )
    schedd = htcondor2.Schedd()
    run = f"ClusterId == {handle.cluster} || DAGManJobId == {handle.cluster}"
    try:
        assert wait_for((mark / "t0.done").exists, 600.0, 1.0), handle.logs().get("driver.log")
        os.kill(child_starts(report)[-1][0], 9)  # SIGKILL, which win32's signal stubs lack
        assert wait_for(lambda: len(child_starts(report)) == 2, 60.0), child_starts(report)
        (mark / "killed").touch()
        status = run_bounded(lambda: handle.wait(timeout=600.0, poll_s=2), 660.0)
        log = str(handle.logs().get("driver.log", ""))
        assert status == "done", log[-4000:]
        assert len(re.findall(r"^rerun:", log, re.MULTILINE)) == 1, log[-4000:]
        events = (Path(handle.log_dir) / "pilots" / "pilots.log").read_text()
        assert re.search(r"^012 .*\n.*graphed: a service of this run waits", events, re.MULTILINE), events
        assert re.search(r"^013 ", events, re.MULTILINE), events
        twin = mark.with_name("twin")
        twin.mkdir()
        (twin / "killed").touch()
        with Web() as web:
            expected = SequentialRunner().run(gated_plan(twin, None, process=GatedGet(web.endpoint))).value
        assert run_bounded(handle.result).value == expected
    finally:
        if schedd.query(run, ["ClusterId"]):
            schedd.act(htcondor2.JobAction.Remove, run)
        assert wait_for(lambda: not schedd.query(run, ["ClusterId"]), 180.0, 2.0), (
            "the run outlived its removal"
        )


@pytest.mark.parametrize("missing", ["schedd_locate", "job_ad"])
def test_a_wait_without_a_locator_or_a_job_ad_reads_no_queue_and_logs_nothing(
    missing: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    schedd = PilotSchedd([(0.0, node_ad(IDLE, memory_mb=6000))])
    with Web() as web, condor_dag_backend(tmp_path, monkeypatch, schedd) as (backend, fake):
        if missing == "schedd_locate":
            backend._schedd_locate = None
        else:
            monkeypatch.delenv("_CONDOR_JOB_AD")
        publish_pair(backend, tmp_path / "dag")
        announce_later(tmp_path / "dag", web.port, 0.6)
        with caplog.at_level(logging.WARNING, logger="graphed_executors"):
            got = run_bounded(lambda: backend.host_service(node_spec(600.0), "m-dee-scope"), 20.0)
    assert tuple(got) == (web.endpoint, SERVICE_ID, NODE), got
    assert schedd.node_queries() == [], fake.log
    assert caplog.records == [], [r.getMessage() for r in caplog.records]


def test_an_idle_node_is_matched_once_across_its_idle_polls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    schedd = PilotSchedd([(0.0, node_ad(IDLE, memory_mb=6000))])
    with Web() as web, condor_dag_backend(tmp_path, monkeypatch, schedd) as (backend, fake):
        publish_pair(backend, tmp_path / "dag")
        announce_later(tmp_path / "dag", web.port, 1.0)
        got = run_bounded(lambda: backend.host_service(node_spec(600.0), "m-dee-scope"), 20.0)
    assert tuple(got) == (web.endpoint, SERVICE_ID, NODE), got
    idle_answers = [e for e in schedd.node_queries() if e[3] == IDLE]
    machine_queries = [e for e in fake.log if e[0] == "collector-query" and "Machine" in e[3]]
    assert len(idle_answers) >= 3 and len(machine_queries) == 1, (len(idle_answers), machine_queries)
