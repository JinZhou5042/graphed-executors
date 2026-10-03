"""m69b schedulability paths the frozen suite does not reach: the physical-memory read on the other OS's branch,
the driver check's fall-through to a cluster host and its refusal beside the earlier legs, the per-start sum,
a size-less spec; a driver job's slot ``Memory``; the condor announce wait's idle log cadence and a deadline
that outlives an eviction; the collector query, a partitionable slot's totals, and a job that left the queue
before its match; a pool whose collector lists no slot (submit and wait); a node's disk counted whole; the
backend's close removing a waiting job in its own thread, a serialized stop() done once, no submit after a
stop or once the waits are stopped; no pilot submitted for a need beside a starting service once the run
ends, while a held one is still released, a wait for submitted pilots ended by close() alone, and a
pilot submit in flight that close() waits for and removes; a wait timed from the pilots' submit or from
its call, whichever is later, and a wait below min_pilots that is not the first need's; a held pilot
counted alive, a running pilot's claim taken out of its slot, a later plan's failed service start releasing
the pilots it held, a driver job's too-big service refused naming the sizes, and graphed's release leaving a
user's hold (that one on a real schedd). The rest run over stand-ins."""

from __future__ import annotations

import ctypes
import logging
import os
import re
import socket
import sys
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from graphed.services import Launch, ServiceSpec

from graphed_executors.htcondor_backend import SITES, CondorPilots, HTCondorBackend, launch
from graphed_executors.htcondor_backend import backend as backend_mod
from graphed_executors.htcondor_backend import server as server_mod
from graphed_executors.htcondor_backend.services import ServiceJob, _as_whole, _less_claim, machine_ads
from graphed_executors.submit import ThreadBackend
from graphed_executors.submit.services import (
    ServiceSet,
    ServiceUnavailable,
    host_identity,
    machine_ad,
    physical_memory_mb,
)

MIB = 1 << 20
LISTEN = (
    "import socket, sys, time; s = socket.create_server((sys.argv[1], int(sys.argv[2]))); time.sleep(120)"
)


# ---- the host's physical memory ------------------------------------------------------------------------


def test_the_windows_branch_reads_total_physical_memory(monkeypatch: pytest.MonkeyPatch) -> None:
    lengths: list[int] = []

    def status(ref: Any) -> int:
        lengths.append(ref._obj.dwLength)
        ref._obj.ullTotalPhys = 3 * 1024 * MIB + 5
        return 1

    monkeypatch.setattr(sys, "platform", "win32")
    kernel32 = SimpleNamespace(GlobalMemoryStatusEx=status)
    monkeypatch.setattr(ctypes, "windll", SimpleNamespace(kernel32=kernel32), raising=False)
    assert physical_memory_mb() == 3072
    assert lengths == [64]  # MEMORYSTATUSEX's size, which the call requires in dwLength


def test_a_failed_windows_call_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "platform", "win32")
    kernel32 = SimpleNamespace(GlobalMemoryStatusEx=lambda ref: 0)
    monkeypatch.setattr(ctypes, "windll", SimpleNamespace(kernel32=kernel32), raising=False)
    monkeypatch.setattr(ctypes, "WinError", lambda: OSError("GlobalMemoryStatusEx failed"), raising=False)
    with pytest.raises(OSError, match="GlobalMemoryStatusEx failed"):
        physical_memory_mb()


def test_the_posix_branch_multiplies_pages(monkeypatch: pytest.MonkeyPatch) -> None:
    pages = {"SC_PAGE_SIZE": 16384, "SC_PHYS_PAGES": 3 * 65536 + 7}
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(os, "sysconf", pages.__getitem__, raising=False)
    assert physical_memory_mb() == 3072


# ---- the engine's driver check ---------------------------------------------------------------------------


class HostingBackend(ThreadBackend):
    """``ThreadBackend(1)`` holding ``driver_memory_mb`` beside the driver and hosting the rest "on the
    cluster" as listeners here, or refusing them with ``refuse``."""

    service_hosts = ("driver", "cluster")

    def __init__(self, driver_memory_mb: int, refuse: str | None = None) -> None:
        super().__init__(1)
        self.driver_memory_mb = driver_memory_mb
        self.refuse = refuse
        self.hosted: dict[str, socket.socket] = {}

    def host_service(self, spec: ServiceSpec, scope: str) -> tuple[str, str, str]:
        if self.refuse is not None:
            raise ServiceUnavailable(spec.name, {"managed": self.refuse})
        sock = socket.create_server(("127.0.0.1", 0))
        key = f"{scope}-{len(self.hosted)}"
        self.hosted[key] = sock
        return f"tcp://127.0.0.1:{sock.getsockname()[1]}", host_identity(), key

    def release_service(self, key: str) -> None:
        self.hosted.pop(key).close()


def spec(name: str, memory_mb: int | None) -> ServiceSpec:
    resources = {} if memory_mb is None else {"memory_mb": memory_mb}
    argv = ("{python}", "-c", LISTEN, "{host}", "{port}")
    return ServiceSpec(
        name, kind="m69b", check="tcp", launch=Launch(argv, resources=resources), timeout_s=30.0
    )


@pytest.fixture
def backends() -> Iterator[list[HostingBackend]]:
    made: list[HostingBackend] = []
    yield made
    for backend in made:
        backend.close()


def hosting(made: list[HostingBackend], driver_memory_mb: int, refuse: str | None = None) -> HostingBackend:
    made.append(HostingBackend(driver_memory_mb, refuse))
    return made[-1]


def test_a_server_the_driver_cannot_hold_runs_on_the_cluster_and_says_why(
    backends: list[HostingBackend],
) -> None:
    backend = hosting(backends, 137)
    services = ServiceSet([spec("big", 311)], backend)
    with services:
        ((key, _sock),) = backend.hosted.items()
        (status,) = services.statuses()
    assert (status.leg, status.host) == ("managed", "cluster")
    assert "311 MiB" in status.detail and "137 MiB" in status.detail, status.detail
    assert backend.hosted == {}, "the cluster host outlived its set"
    assert key.startswith(services.scope)


def test_a_cluster_refusal_keeps_why_the_earlier_legs_failed(backends: list[HostingBackend]) -> None:
    backend = hosting(backends, 137, refuse="no slot holds it")
    with pytest.raises(ServiceUnavailable) as err:
        ServiceSet([spec("big", 311)], backend).start()
    assert list(err.value.legs) == ["user", "site", "driver", "managed"]
    assert err.value.legs["managed"] == "no slot holds it"
    assert "311 MiB" in err.value.legs["driver"] and "137 MiB" in err.value.legs["driver"]


def test_the_driver_sum_starts_afresh_with_each_start(backends: list[HostingBackend]) -> None:
    backend = hosting(backends, 300)
    services = ServiceSet([spec("again", 300)], backend)
    for _ in range(2):
        with services:
            pass
        assert [(s.leg, s.host) for s in services.statuses()] == [("managed", "driver")]
    assert backend.hosted == {}


def test_a_service_without_a_size_fits_any_driver(backends: list[HostingBackend]) -> None:
    backend = hosting(backends, 0)
    services = ServiceSet([spec("sizeless", None)], backend)
    with services:
        pass
    assert [(s.leg, s.host) for s in services.statuses()] == [("managed", "driver")]


# ---- machine ads ----------------------------------------------------------------------------------------


def test_machine_ad_reads_one_attribute(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("_CONDOR_MACHINE_AD", raising=False)
    assert machine_ad("Memory") is None
    ad = tmp_path / ".machine.ad"
    ad.write_text('Machine = "node7.example"\nMemory = 3072\n')
    monkeypatch.setenv("_CONDOR_MACHINE_AD", str(ad))
    assert (machine_ad("Machine"), machine_ad("Memory"), machine_ad("Cpus")) == (
        "node7.example",
        "3072",
        None,
    )


class NoPilots:
    def start(self, url: str, secret: bytes, n: int) -> None:
        pass

    def alive(self) -> int:
        return 0

    def stop(self) -> None:
        pass


@pytest.mark.parametrize(("lines", "expected"), [("Memory = 3072\n", 3072), ("Cpus = 4\n", None)])
def test_a_driver_job_holds_its_services_to_its_slot_memory(
    lines: str, expected: int | None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ad = tmp_path / ".machine.ad"
    ad.write_text(f'Machine = "127.0.0.1"\n{lines}')
    monkeypatch.setenv("_CONDOR_MACHINE_AD", str(ad))
    attached = HTCondorBackend(NoPilots(), 1, host="127.0.0.1")
    in_job = HTCondorBackend(NoPilots(), 1, host="127.0.0.1", in_job=SITES["generic"])
    try:
        assert (attached.driver_memory_mb, in_job.driver_memory_mb) == (None, expected)
    finally:
        attached.close()
        in_job.close()


def test_a_driver_job_refuses_a_service_too_big_for_its_slot_naming_the_sizes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ad = tmp_path / ".machine.ad"
    ad.write_text('Machine = "127.0.0.1"\nMemory = 100\n')
    monkeypatch.setenv("_CONDOR_MACHINE_AD", str(ad))
    backend = HTCondorBackend(
        NoPilots(), 1, host="127.0.0.1", in_job=SITES["generic"], announced={"gpu": "svc0"}
    )  # its host_service answers the DAG's SERVICE nodes only
    try:
        with pytest.raises(ServiceUnavailable) as refused:
            ServiceSet([spec("small", 200)], backend, scope="p").start()
        managed = refused.value.legs["managed"]
        assert "200 MiB" in managed and "driver_memory_mb of 100 MiB" in managed, refused.value.legs
    finally:
        backend.close()


# ---- the announce wait ------------------------------------------------------------------------------------


class Answers:
    """A ``ServiceJob`` stand-in whose ``ad()`` gives ``states`` in turn, the last repeating."""

    def __init__(self, states: list[dict[str, Any]]) -> None:
        self.key = "scope-0123456789abcdef"
        self.dir = Path("service-scope")
        self.states = states

    def ad(self) -> dict[str, Any]:
        return self.states.pop(0) if len(self.states) > 1 else self.states[0]


@pytest.fixture
def condor(monkeypatch: pytest.MonkeyPatch) -> Iterator[HTCondorBackend]:
    monkeypatch.setattr(server_mod, "POLL_S", 0.02)
    backend = HTCondorBackend(NoPilots(), 1, host="127.0.0.1")
    try:
        yield backend
    finally:
        backend.close()


def announcing_after(backend: HTCondorBackend, monkeypatch: pytest.MonkeyPatch, polls: int) -> None:
    """``wait_announce`` answers on its ``polls``-th call."""
    calls = iter(range(polls))

    def wait(key: str, timeout: float) -> tuple[str, str] | None:
        return None if next(calls, None) is not None else ("127.0.0.1:10007", "node7")

    monkeypatch.setattr(backend._server, "wait_announce", wait)


@pytest.mark.parametrize(("every_s", "lines"), [(1000.0, 1), (0.0, 3)])
def test_a_waiting_job_is_logged_once_per_interval_and_never_timed_out(
    every_s: float,
    lines: int,
    condor: HTCondorBackend,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(backend_mod, "IDLE_LOG_S", every_s)
    announcing_after(condor, monkeypatch, 3)
    job: Any = Answers([{"JobStatus": 1}, {"JobStatus": 1}, {"JobStatus": 5, "HoldReasonCode": 16}])
    waits = ServiceSpec("waits", kind="m69b", check="tcp", timeout_s=0.01)
    with caplog.at_level(logging.INFO, logger="graphed_executors"):
        got = condor._await_announce(job, waits)
    assert got == ("127.0.0.1:10007", "node7")
    logged = [r.getMessage() for r in caplog.records if job.key in r.getMessage()]
    assert len(logged) == lines and "JobStatus=1" in logged[0], logged


def test_the_deadline_outlives_an_eviction(condor: HTCondorBackend, monkeypatch: pytest.MonkeyPatch) -> None:
    announcing_after(condor, monkeypatch, 10**6)
    job: Any = Answers([{"JobStatus": 2}, {"JobStatus": 1}])
    quick = ServiceSpec("evicted", kind="m69b", check="tcp", timeout_s=0.2)
    with pytest.raises(TimeoutError, match=r"timeout_s=0\.2 of its start: JobStatus=1"):
        condor._await_announce(job, quick)


# ---- the pool's slots -------------------------------------------------------------------------------------


@pytest.mark.parametrize("locate", [None, ("cm.example:9618", "schedd.example")])
def test_machine_ads_ask_the_schedd_s_pool_and_drop_dynamic_slots(
    locate: tuple[str, str] | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    asked: list[tuple[Any, ...]] = []
    ads = [{"Name": "slot1", "SlotType": "Partitionable"}, {"Name": "slot1_1", "SlotType": "Dynamic"}]

    def collector(*pool: str) -> Any:
        def query(constraint: str) -> list[dict[str, str]]:
            asked.append((*pool, constraint))
            return ads

        return SimpleNamespace(query=query)

    monkeypatch.setattr(launch, "_htcondor", lambda: SimpleNamespace(Collector=collector))
    got = machine_ads(CondorPilots("generic", schedd_locate=locate))
    assert [ad["Name"] for ad in got] == ["slot1"]
    pool = () if locate is None else (locate[0],)
    assert asked == [(*pool, 'MyType == "Machine"')]


def test_a_partitionable_slot_is_matched_at_its_totals() -> None:
    busy = {
        "PartitionableSlot": True,
        "Memory": 997,
        "TotalSlotMemory": 15973,
        "Cpus": 1,
        "TotalSlotCpus": 10,
        "Disk": 524288,
        "TotalSlotDisk": 2097152,
    }
    assert _as_whole(dict(busy)) == {**busy, "Memory": 15973, "Cpus": 10, "Disk": 2097152}
    static = {"Memory": 2048, "TotalSlotMemory": 4096}
    assert _as_whole(dict(static)) == static


def test_a_job_that_left_the_queue_is_not_refused_by_the_match(tmp_path: Path) -> None:
    schedd = SimpleNamespace(query=lambda constraint: [])
    pilots = CondorPilots("generic", log_dir=tmp_path)
    pilots._schedd = schedd
    job = ServiceJob(spec("gone", None), pilots, key="scope-gone", url="http://127.0.0.1:1", secret=b"s")
    job.cluster = 4242
    assert job.match_refusal([{"SlotType": "Partitionable"}]) is None


def test_a_job_waiting_only_on_a_busy_node_s_disk_matches() -> None:
    classad2 = pytest.importorskip("classad2")  # ships with the htcondor bindings (Linux)
    job = classad2.ClassAd("[ RequestDisk = 1048576; Requirements = TARGET.Disk >= RequestDisk ]")
    busy = "[ PartitionableSlot = true; Disk = 524288; TotalSlotDisk = 2097152; Requirements = true ]"
    assert not job.symmetricMatch(classad2.ClassAd(busy))  # the collector's busy node, as it reports it
    assert job.symmetricMatch(_as_whole(classad2.ClassAd(busy)))


# ---- a whole backend over a stand-in pool -------------------------------------------------------------------


class Pool:
    """``htcondor2`` over a schedd whose every job answers ``status`` (``None``: gone) and a collector that
    lists no slot; ``log`` records submits (by batch name) and actions."""

    def __init__(self, status: int | None) -> None:
        self.status = status
        self.log: list[tuple[str, str]] = []
        self.clusters = iter(range(70, 100))
        self.JobAction = SimpleNamespace(Remove="Remove", Hold="Hold", Release="Release")
        self.param = {"SCHEDD_HOST": "s1"}
        self.Submit = dict

    def Schedd(self, ad: Any = None) -> Any:
        return self

    def Collector(self, *pool: str) -> Any:
        return SimpleNamespace(query=lambda constraint: [])

    def submit(self, desc: dict[str, str], count: int = 0, spool: bool = False) -> Any:
        cluster = next(self.clusters)
        self.log.append(("submit", f"{desc['JobBatchName']} {cluster}"))
        return SimpleNamespace(cluster=lambda: cluster)

    def query(self, constraint: str = "", projection: Any = None) -> list[dict[str, Any]]:
        return [] if self.status is None else [{"JobStatus": self.status}]

    def history(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        return []

    def act(self, action: str, constraint: str, reason: str = "") -> None:
        self.log.append(("act", f"{action} {constraint}"))

    def service_cluster(self) -> str:
        (submit,) = [entry for kind, entry in self.log if kind == "submit" and "graphed-service-" in entry]
        return submit.rsplit(" ", 1)[1]


def pooled(
    status: int | None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[HTCondorBackend, Pool]:
    pool = Pool(status)
    monkeypatch.setattr(launch, "_htcondor", lambda: pool)
    monkeypatch.setattr(launch, "CLOSE_WAIT_S", 0.0)  # the stand-in's pilots never exit by themselves
    pilots = CondorPilots("generic", log_dir=tmp_path)
    return HTCondorBackend(pilots, 1, host="127.0.0.1", service_hosts=("cluster",)), pool


def test_a_pool_whose_collector_lists_no_slot_submits_and_waits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # were the empty ad list matched, this classad2 would reach max() over no slot
    monkeypatch.setitem(sys.modules, "classad2", SimpleNamespace(ClassAd=dict))
    monkeypatch.setattr(server_mod, "POLL_S", 0.02)
    backend, pool = pooled(2, tmp_path, monkeypatch)
    try:
        announcing_after(backend, monkeypatch, 2)
        endpoint, identity, key = backend.host_service(spec("unlisted", 64), "scope")
        assert (endpoint, identity) == ("tcp://127.0.0.1:10007", "node7")
        backend.release_service(key)
        assert ("act", f"Remove ClusterId == {pool.service_cluster()}") in pool.log
    finally:
        pool.status = None
        backend.close()


def key_of(pool: Pool) -> str:
    (submit,) = [entry for kind, entry in pool.log if kind == "submit" and "graphed-service-" in entry]
    return submit.split(" ", 1)[0].removeprefix("graphed-service-")


def test_backend_close_removes_a_waiting_service_job_before_it_returns(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(server_mod, "POLL_S", 3.0)  # the waiter's own poll comes long after close() returns
    backend, pool = pooled(1, tmp_path, monkeypatch)  # the job stays idle
    raised: list[BaseException] = []

    def wait() -> None:
        try:
            backend.host_service(spec("idle", 64), "scope")
        except Exception as exc:
            raised.append(exc)

    waiter = threading.Thread(target=wait, daemon=True)
    waiter.start()
    deadline = time.monotonic() + 10.0
    while not any(kind == "submit" for kind, _ in pool.log) and time.monotonic() < deadline:
        time.sleep(0.01)
    backend.close()
    removed = ("act", f"Remove ClusterId == {pool.service_cluster()}") in pool.log
    pool.status = None  # the removed job leaves the queue, as on a schedd
    waiter.join(30.0)
    assert removed, pool.log
    (error,) = raised
    assert key_of(pool) in str(error) and "when the runner closed" in str(error), error


class BlockingPool(Pool):
    """A ``Pool`` whose every ``act`` waits for ``gate``, having set ``acting``."""

    def __init__(self) -> None:
        super().__init__(1)
        self.acting = threading.Event()
        self.gate = threading.Event()

    def act(self, action: str, constraint: str, reason: str = "") -> None:
        self.acting.set()
        self.gate.wait(30.0)
        super().act(action, constraint, reason)


def submitted_job(
    pool: Pool, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, submit: bool = True
) -> ServiceJob:
    monkeypatch.setattr(launch, "_htcondor", lambda: pool)
    pilots = CondorPilots("generic", log_dir=tmp_path)
    pilots.prepare("http://127.0.0.1:1", b"s")
    job = ServiceJob(spec("web", 64), pilots, key="scope-web", url="http://127.0.0.1:1", secret=b"s")
    if submit:
        job.submit()
    return job


def test_a_second_stop_returns_only_after_the_first_removal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pool = BlockingPool()
    job = submitted_job(pool, tmp_path, monkeypatch)
    returned: list[str] = []
    raised: list[BaseException] = []

    def stop(name: str) -> None:
        try:
            job.stop()
        except BaseException as exc:
            raised.append(exc)
        returned.append(name)

    first = threading.Thread(target=stop, args=("first",), daemon=True)
    second = threading.Thread(target=stop, args=("second",), daemon=True)
    try:
        first.start()
        assert pool.acting.wait(10.0), "the first stop() never reached its removal"
        second.start()
        second.join(1.0)
        assert second.is_alive() and returned == [], "a second stop() returned before the removal ran"
    finally:
        pool.gate.set()
        first.join(10.0)
        second.join(10.0)
    assert sorted(returned) == ["first", "second"] and raised == []
    assert job.dir is not None
    secret = job.dir / launch.SECRET_FILE
    assert not secret.exists()
    secret.write_text("")
    job.stop()  # a later stop() repeats none of the work
    assert secret.exists()
    assert [entry for entry in pool.log if entry[0] == "act"] == [
        ("act", f"Remove ClusterId == {job.cluster}")
    ]


def test_a_job_stopped_before_its_submit_is_never_submitted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pool = Pool(1)
    job = submitted_job(pool, tmp_path, monkeypatch, submit=False)
    job.stop()
    with pytest.raises(RuntimeError, match="stopped before it was submitted"):
        job.submit()
    assert [kind for kind, _ in pool.log] == []


def test_no_service_job_is_submitted_once_the_waits_are_stopped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    backend, pool = pooled(1, tmp_path, monkeypatch)
    try:
        backend.stop_waiting()
        with pytest.raises(RuntimeError, match="not submitted: the runner is closing"):
            backend.host_service(spec("late", 64), "scope")
        assert pool.log == []
        assert backend._server._announce_secrets == {}
    finally:
        backend.close()


# ---- the runner's own pilots --------------------------------------------------------------------------


def pilot_submits(pool: Pool) -> list[str]:
    return [entry for kind, entry in pool.log if kind == "submit" and "graphed-pilots-" in entry]


@pytest.mark.parametrize("end", ["stop_waiting", "close"])
def test_a_need_beside_a_starting_service_submits_no_pilot_once_the_run_ends(
    end: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(server_mod, "POLL_S", 0.1)
    backend, pool = pooled(1, tmp_path, monkeypatch)  # the service job stays idle
    raised: dict[str, BaseException] = {}

    def call(name: str, fn: Any) -> None:
        try:
            fn()
        except Exception as exc:
            raised[name] = exc

    def starting() -> None:
        ServiceSet([spec("idle", 64)], backend, scope="s").start()

    server = threading.Thread(target=call, args=("server", starting), daemon=True)
    need = threading.Thread(target=call, args=("need", lambda: backend.wait_for_pilots(1)), daemon=True)
    try:
        server.start()
        assert wait_until(lambda: any(kind == "submit" for kind, _ in pool.log), 10.0)
        need.start()
        need.join(0.5)
        assert need.is_alive() and raised == {}, raised
        getattr(backend, end)()
        server.join(10.0)
        need.join(10.0)
        assert pilot_submits(pool) == [], pool.log
        assert "when the runner closed" in str(raised.get("server")), raised
        assert "0 of 1 pilots connected when the runner closed" in str(raised.get("need")), raised
    finally:
        pool.status = None
        backend.close()
        need.join(10.0)


def test_a_need_beside_a_later_plan_s_starting_service_releases_its_pilots_once_the_run_ends(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(server_mod, "POLL_S", 0.1)
    backend, pool = pooled(1, tmp_path, monkeypatch)
    backend.min_pilots = 0
    backend.wait_for_pilots(0)  # a first plan ran
    raised: list[BaseException] = []

    def starting() -> None:
        try:
            ServiceSet([spec("idle", 64)], backend, scope="s").start()
        except RuntimeError as exc:
            raised.append(exc)

    server = threading.Thread(target=starting, daemon=True)
    try:
        server.start()
        assert wait_until(lambda: any("graphed-service-" in entry for _, entry in pool.log), 10.0)
        backend.n_workers()  # a need, recorded
        assert [entry for _, entry in pool.log if entry.startswith("Release")] == [], pool.log
        backend.stop_waiting()  # a queued plan the drain finishes may need the held pilots
        server.join(10.0)
        assert len(raised) == 1 and "when the runner closed" in str(raised[0]), raised
        assert len([entry for _, entry in pool.log if entry.startswith("Release")]) == 1, pool.log
        assert len(pilot_submits(pool)) == 1, pool.log
    finally:
        pool.status = None
        backend.close()


def test_once_the_pilots_are_submitted_only_close_ends_a_wait_for_them(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    backend, _pool = pooled(1, tmp_path, monkeypatch)
    raised: list[BaseException] = []

    def need() -> None:
        try:
            backend.wait_for_pilots(1)
        except RuntimeError as exc:
            raised.append(exc)

    waiter = threading.Thread(target=need, daemon=True)
    try:
        backend.wait_for_pilots(0)  # submits the pilots
        waiter.start()
        backend.stop_waiting()
        waiter.join(0.5)
        assert waiter.is_alive() and raised == [], "the drain's plans wait for submitted pilots"
        backend.close()
        waiter.join(10.0)
        assert [str(exc) for exc in raised] == ["0 of 1 pilots connected when the runner closed"]
    finally:
        backend.close()
        waiter.join(10.0)


class GatedSubmitPool(Pool):
    """A ``Pool`` whose pilots' submit waits for ``gate``, having set ``submitting``."""

    def __init__(self) -> None:
        super().__init__(1)
        self.submitting = threading.Event()
        self.gate = threading.Event()

    def submit(self, desc: dict[str, str], count: int = 0, spool: bool = False) -> Any:
        if "graphed-pilots-" in desc["JobBatchName"]:
            self.submitting.set()
            self.gate.wait(30.0)
        return super().submit(desc, count, spool)


def test_close_waits_for_a_pilot_submit_in_flight_and_removes_its_cluster(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pool = GatedSubmitPool()
    monkeypatch.setattr(launch, "_htcondor", lambda: pool)
    monkeypatch.setattr(launch, "CLOSE_WAIT_S", 0.0)
    pilots = CondorPilots("generic", log_dir=tmp_path)
    backend = HTCondorBackend(pilots, 1, host="127.0.0.1", service_hosts=("cluster",))
    need = threading.Thread(target=backend.wait_for_pilots, args=(0,), daemon=True)
    closer = threading.Thread(target=backend.close, daemon=True)
    try:
        need.start()
        assert pool.submitting.wait(10.0), "the need never reached the pilots' submit"
        closer.start()
        closer.join(0.5)
        assert closer.is_alive(), "close() returned with a pilot submit in flight"
        pool.gate.set()
        closer.join(10.0)
        assert not closer.is_alive()
        (cluster,) = [entry.rsplit(" ", 1)[1] for entry in pilot_submits(pool)]
        assert ("act", f"Remove ClusterId == {cluster}") in pool.log, pool.log
    finally:
        pool.gate.set()
        backend.close()


def test_a_later_plan_s_failed_service_start_releases_the_pilots_it_held(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(server_mod, "POLL_S", 0.05)
    backend, pool = pooled(None, tmp_path, monkeypatch)  # the service job leaves the queue unannounced
    try:
        backend.min_pilots = 0
        backend.wait_for_pilots(0)  # a first plan ran
        with pytest.raises(RuntimeError, match="ended before it announced"):
            ServiceSet([spec("gone", 64)], backend, scope="s").start()
        moves = [entry.split(" ", 1)[0] for kind, entry in pool.log if kind == "act"]
        assert moves == ["Hold", "Release"], pool.log  # no need of a worker was made
    finally:
        backend.close()


class GonePool(Pool):
    """``Pool`` whose removed jobs leave the queue."""

    def __init__(self, status: int | None) -> None:
        super().__init__(status)
        self.gone: set[str] = set()

    def act(self, action: str, constraint: str, reason: str = "") -> None:
        super().act(action, constraint, reason)
        if action == "Remove":
            self.gone.add(constraint)

    def query(self, constraint: str = "", projection: Any = None) -> list[dict[str, Any]]:
        return [] if constraint in self.gone else super().query(constraint, projection)


@pytest.mark.parametrize("end", ["stop_waiting", "close"])
def test_a_set_waiting_its_turn_to_start_ends_when_the_runner_closes(
    end: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(server_mod, "POLL_S", 0.05)
    pool = GonePool(2)  # a started job that never announces: its wait outlives stop_waiting
    monkeypatch.setattr(launch, "_htcondor", lambda: pool)
    monkeypatch.setattr(launch, "CLOSE_WAIT_S", 0.0)
    pilots = CondorPilots("generic", log_dir=tmp_path)
    backend = HTCondorBackend(pilots, 1, host="127.0.0.1", service_hosts=("cluster",))
    ended: dict[str, BaseException] = {}

    def start(name: str) -> None:
        waits = ServiceSpec(name, kind="m69b", check="tcp", launch=Launch(("serve",)), timeout_s=600.0)
        try:
            ServiceSet([waits], backend, scope=name).start()
        except Exception as exc:
            ended[name] = exc

    first = threading.Thread(target=start, args=("a",), daemon=True)
    second = threading.Thread(target=start, args=("b",), daemon=True)
    try:
        first.start()
        assert wait_until(lambda: len(pool.log) > 0), "plan a submitted no service job"
        second.start()
        time.sleep(0.5)
        assert second.is_alive() and [k for k, _ in pool.log] == ["submit"], pool.log
        getattr(backend, end)()
        second.join(5.0)
        assert not second.is_alive(), "plan b still waited its turn after the runner closed"
        # its turn may come as plan a ends: then the closing backend refuses its submit
        assert isinstance(ended["b"], RuntimeError) and re.search("runner (is )?clos", str(ended["b"])), ended
    finally:
        backend.close()
        first.join(10.0)
    assert not first.is_alive() and "a" in ended, ended
    (submit,) = [entry for kind, entry in pool.log if kind == "submit"]
    assert f"ClusterId == {submit.rsplit(' ', 1)[1]}" in pool.gone, pool.log


def test_a_need_deferred_by_a_service_start_times_its_wait_from_the_pilots_submit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    backend, pool = pooled(1, tmp_path, monkeypatch)
    raised: list[tuple[float, BaseException]] = []

    def need() -> None:
        try:
            backend.wait_for_pilots(1, timeout=1.0)
        except RuntimeError as exc:
            raised.append((time.monotonic(), exc))

    waiter = threading.Thread(target=need, daemon=True)
    try:
        with backend.starting_services():
            waiter.start()
            time.sleep(1.5)  # deferred past its timeout
            assert raised == [] and pilot_submits(pool) == [], (raised, pool.log)
        ended = time.monotonic()
        waiter.join(10.0)
        assert len(pilot_submits(pool)) == 1, pool.log
        ((at, exc),) = raised
        assert "0 of 1 pilots connected after 1.0s" in str(exc) and at - ended >= 1.0, (at - ended, exc)
    finally:
        backend.close()


def test_a_wait_called_after_the_pilots_submit_times_from_the_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    backend, _pool = pooled(1, tmp_path, monkeypatch)
    try:
        backend.wait_for_pilots(0)  # submits the pilots
        time.sleep(1.2)  # past the next wait's timeout
        called = time.monotonic()
        with pytest.raises(RuntimeError, match=r"0 of 1 pilots connected after 1\.0s"):
            backend.wait_for_pilots(1, timeout=1.0)
        assert time.monotonic() - called >= 1.0
    finally:
        backend.close()


def test_a_wait_below_min_pilots_leaves_the_first_need_s_wait_to_come(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    backend, _pool = pooled(1, tmp_path, monkeypatch)
    live = [0]
    monkeypatch.setattr(backend._server, "live_pilots", lambda: live[0])  # pilots connect when the row says
    got: list[int] = []
    first = threading.Thread(target=lambda: got.append(backend.n_workers()), daemon=True)
    later = threading.Thread(target=lambda: got.append(backend.n_workers()), daemon=True)
    try:
        assert backend.wait_for_pilots(0) == 0  # min_pilots is 1
        first.start()
        first.join(0.5)
        assert first.is_alive() and got == [], "the first need did not wait for min_pilots"
        live[0] = 1
        first.join(10.0)
        live[0] = 0  # every pilot lost after the first need's wait: a need no longer waits
        later.start()
        later.join(5.0)
        assert got == [1, 0], got
    finally:
        live[0] = 1
        first.join(10.0)
        later.join(10.0)
        backend.close()


def test_alive_counts_a_pilot_graphed_held_and_not_one_the_user_held(tmp_path: Path) -> None:
    held = f"{launch.HOLD_REASON} (by user someone)"
    ads = [
        {"JobStatus": 2},
        {"JobStatus": 5, "HoldReasonCode": 1, "HoldReason": held},
        {"JobStatus": 5, "HoldReasonCode": 1, "HoldReason": "via condor_hold (by user someone)"},
    ]
    pilots = CondorPilots("generic", log_dir=tmp_path)
    pilots._schedd = SimpleNamespace(query=lambda constraint, projection: ads)
    assert pilots.alive() == 2


class Ad(dict[str, Any]):
    """A job ad as ``schedd.query`` returns it: ``eval`` reads an attribute's value."""

    def eval(self, attr: str) -> Any:
        return self[attr]


def test_a_running_job_s_claim_leaves_the_slot_it_runs_in() -> None:
    whole = {"Name": "slot1@wn", "Memory": 16000, "Cpus": 8, "Disk": 1000}
    static = {"Name": "slot2@wn", "Memory": 4000, "Cpus": 1, "Disk": 1000}
    other = {"Name": "slot1@wn2", "Memory": 16000, "Cpus": 8, "Disk": 1000}
    slots = [dict(whole), dict(static), dict(other)]
    dynamic = Ad(RemoteHost="slot1_3@wn", MemoryProvisioned=6144, CpusProvisioned=2, RequestMemory=6000)
    asked_only = Ad(RemoteHost="slot2@wn", RequestMemory=4000, RequestCpus=1, RequestGPUs=1)
    _less_claim(slots, dynamic)
    _less_claim(slots, asked_only)
    assert slots == [
        {**whole, "Memory": 16000 - 6144, "Cpus": 6},
        {**static, "Memory": 0, "Cpus": 0},
        other,
    ]


class SlotPool(Pool):
    """``Pool`` whose collector lists one 1000 MiB partitionable slot; the schedd answers a job's ad (its
    request, as ``classad2``) and, once the job is in ``running``, its claim on that slot."""

    SLOT = (
        '[ Name = "slot1@wn"; SlotType = "Partitionable"; PartitionableSlot = true; Memory = 0; '
        "TotalSlotMemory = 1000; Cpus = 1; TotalSlotCpus = 4; Requirements = true ]"
    )

    def __init__(self, classad2: Any) -> None:
        super().__init__(None)
        self.classad2 = classad2
        self.asked: dict[int, int] = {}
        self.running: set[int] = set()

    def Collector(self, *pool: str) -> Any:
        return SimpleNamespace(query=lambda constraint: [self.classad2.ClassAd(self.SLOT)])

    def submit(self, desc: dict[str, str], count: int = 0, spool: bool = False) -> Any:
        result = super().submit(desc, count, spool)
        self.asked[result.cluster()] = int(desc["request_memory"])
        return result

    def query(self, constraint: str = "", projection: Any = None) -> list[Any]:
        found = re.search(r"ClusterId == (\d+)", constraint)
        assert found is not None, constraint
        cluster = int(found.group(1))
        if "JobStatus == 2" in constraint:
            claim = Ad(RemoteHost="slot1_1@wn", MemoryProvisioned=self.asked[cluster])
            return [claim] if cluster in self.running else []
        request = f"RequestMemory = {self.asked[cluster]}; Requirements = TARGET.Memory >= RequestMemory"
        return [self.classad2.ClassAd(f"[ {request} ]")]


def test_a_first_plan_s_server_is_refused_where_only_its_set_s_earlier_server_holds_room(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    classad2 = pytest.importorskip("classad2")  # ships with the htcondor bindings (Linux)
    pool = SlotPool(classad2)
    monkeypatch.setattr(launch, "_htcondor", lambda: pool)
    monkeypatch.setattr(launch, "CLOSE_WAIT_S", 0.0)
    pilots = CondorPilots("generic", log_dir=tmp_path)
    backend = HTCondorBackend(pilots, 1, host="127.0.0.1", service_hosts=("cluster",))

    def wait(key: str, timeout: float) -> tuple[str, str] | None:
        job = backend._services[key]
        if job.spec.name != "web1" or job.cluster is None:
            return None
        pool.running.add(job.cluster)
        return ("127.0.0.1:10007", "node7")

    monkeypatch.setattr(backend._server, "wait_announce", wait)
    other = ServiceJob(spec("other", 300), pilots, key="t-0", url=backend._server.url, secret=b"s")
    try:
        other.submit()  # another plan's server, running on the same slot
        assert other.cluster is not None
        pool.running.add(other.cluster)
        backend._services[other.key] = other
        with pytest.raises(ServiceUnavailable) as refused:
            ServiceSet([spec("web1", 600), spec("web2", 600)], backend, scope="s").start()
        managed = refused.value.legs["managed"]
        web1, web2 = (int(e.rsplit(" ", 1)[1]) for k, e in pool.log if k == "submit" and "-s-" in e)
        assert f"'web1' (cluster {web1})" in managed and "'other'" not in managed, managed
        assert "beside them is 400 MiB" in managed, managed
        assert pilots.cluster is None and ("act", f"Remove ClusterId == {web2}") in pool.log, pool.log
    finally:
        backend.close()


def test_a_real_schedd_releases_graphed_s_hold_and_not_the_user_s(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    htc = pytest.importorskip("htcondor2", reason="the htcondor bindings (Linux; the test-htcondor job)")
    monkeypatch.setattr(launch, "CLOSE_WAIT_S", 0.0)
    pilots = CondorPilots("generic", log_dir=tmp_path, request_memory_mb=10**7)  # never matched: idle
    pilots.start("http://127.0.0.1:1", b"s", 2)
    try:
        assert pilots.cluster is not None
        cluster = pilots.cluster[1]
        pilots._schedd.act(htc.JobAction.Hold, f"ClusterId == {cluster} && ProcId == 1")  # the user's

        def statuses() -> list[int]:
            ads = pilots._schedd.query(
                constraint=f"ClusterId == {cluster}", projection=["ProcId", "JobStatus"]
            )
            return [int(ad["JobStatus"]) for ad in sorted(ads, key=lambda ad: int(ad["ProcId"]))]

        pilots.hold_queued()
        assert wait_until(lambda: statuses() == [5, 5]), statuses()
        assert pilots.alive() == 1
        pilots.release_held()
        assert wait_until(lambda: statuses() == [1, 5]), statuses()
    finally:
        pilots.stop()


def wait_until(predicate: Any, timeout_s: float = 30.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while not predicate():
        if time.monotonic() > deadline:
            return False
        time.sleep(0.2)
    return True
