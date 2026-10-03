"""m69b: a served histogram plan's histserv servers are ordinary managed services of the executors
(plan-services.md, the m69b ``test_histserv_managed.py`` row). Every OS, no bindings.

The plan is two equal Weight slots (dyadic weights, so every sum is exact) under a context whose one size
fits each slot alone and not both, so it declares two servers. On ``SubmitRunner(ThreadBackend(2))`` the
engine starts both beside the driver, the run's receipts name them, the resolve reads them while they are
up and the run's end stops them; given endpoints are used instead and emptied; a driverless job hosts them
in the driver job and ships back resolved histograms. Servers the driver host cannot hold (a
``driver_memory_mb`` set on the backend, else the host's physical memory; a driver job's slot ``Memory``)
are refused before the one that overflows starts: the servers beside the driver count together, and a given
endpoint is never held to it.
"""

from __future__ import annotations

import logging
import os
import pickle
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from graphed.core import SequentialRunner
from m69b_harness import (
    HARNESS_FILE,
    RUN_TIMEOUT_S,
    free_range,
    histserv_api,
    host_memory_mb,
    port_free,
    require_histserv,
    run_bounded,
    same_values,
    seen,
    served_plan,
    server_count,
    sized_memory_mb,
    spied,
    unique,
    user_servers,
)
from services_harness import (
    DRIVER_MODULE,
    RecordingSchedd,
    endpoint_port,
    htcondor_api,
    job_dir,
    popen_backstop,
    record_bindings,
    status_records,
    submits,
)

from graphed_executors.submit import SubmitRunner, ThreadBackend
from graphed_executors.submit.services import ServiceUnavailable

SERVICES_LOGGER = "graphed_executors.services"
WORKERS = 2
#: two equal slots of 2**17 Weight bins (flow included): 2 MiB each
BINS = {"a": 2**17 - 2, "b": 2**17 - 2}
#: one small slot: its server opens at the context's size whatever that is
SMALL = {"h": 8}
STATUS_LINE = re.compile(r"service '([^']+)': managed leg \(driver\) at tcp://[^\s:]+:(\d+)")


@pytest.fixture(scope="module")
def size_mb() -> int:
    require_histserv()
    return sized_memory_mb(BINS, WORKERS)


@pytest.fixture(scope="module")
def twin() -> dict[Any, Any]:
    value: dict[Any, Any] = SequentialRunner().run(served_plan(BINS)).value
    return value


def two_server_plan(size_mb: int, ports: tuple[int, int] | None = None) -> tuple[Any, Any]:
    kwargs: dict[str, Any] = {} if ports is None else {"ports": ports}
    ctx = histserv_api().Context(memory_mb=size_mb, workers=WORKERS, name=unique("m69b-managed"), **kwargs)
    plan = served_plan(BINS, ctx)
    names = sorted(s.name for s in plan.services)
    assert [(s[0], s[3]) for s in sorted(ctx.servers())] == [(n, 1) for n in names], ctx.servers()
    assert len(names) == 2
    return ctx, plan


def test_two_servers_beside_the_driver_hold_their_slots_and_stop_with_the_run(
    size_mb: int, twin: dict[Any, Any], caplog: pytest.LogCaptureFixture
) -> None:
    ports = free_range(4)
    _ctx, plan = two_server_plan(size_mb, ports)
    tag = unique("managed")
    runner = SubmitRunner(ThreadBackend(WORKERS))
    try:
        with caplog.at_level(logging.INFO, logger=SERVICES_LOGGER):
            result = run_bounded(lambda: runner.run(spied(plan, tag)))
    finally:
        runner.close()
    statuses = status_records(caplog.records)
    assert sorted(s.name for s in statuses) == sorted(s.name for s in plan.services)
    assert {(s.leg, s.host) for s in statuses} == {("managed", "driver")}
    used = [endpoint_port(s.endpoint) for s in statuses]
    assert len(set(used)) == 2 and all(ports[0] <= p <= ports[1] for p in used), used
    held = seen(tag)
    assert set(held) == {s.endpoint for s in statuses}, (held, statuses)
    assert sorted(held.values()) == [(1, 1), (1, 1)], held
    assert all(port_free(p) for p in used), "a server outlived its run"
    assert same_values(result.value, twin)


def test_given_endpoints_start_nothing_and_are_left_empty(
    size_mb: int, twin: dict[Any, Any], caplog: pytest.LogCaptureFixture
) -> None:
    _ctx, plan = two_server_plan(size_mb)
    names = sorted(s.name for s in plan.services)
    tag = unique("given")
    backend = ThreadBackend(WORKERS)
    backend.driver_memory_mb = 64  # below either server: a given endpoint is never held to it
    with user_servers(2) as endpoints:
        given = dict(zip(names, endpoints, strict=True))
        runner = SubmitRunner(backend, services=given)
        try:
            with caplog.at_level(logging.INFO, logger=SERVICES_LOGGER):
                result = run_bounded(lambda: runner.run(spied(plan, tag)))
            statuses = status_records(caplog.records)
            # the same backend still holds a spec on the managed leg to its 64 MiB
            managed, started = refused(runner, 1024)
        finally:
            runner.close()
        assert sorted((s.name, s.leg, s.endpoint) for s in statuses) == [(n, "user", given[n]) for n in names]
        assert seen(tag) == dict.fromkeys(endpoints, (1, 1))
        assert [server_count(ep) for ep in endpoints] == [0, 0], "the resolve left server copies"
    assert same_values(result.value, twin)
    assert named(1024, managed) and named(64, managed), managed
    assert [p.args for p in started if "histserv" in str(p.args)] == []


def driver_job(
    plan: Any, size_mb: int, slot_mb: int, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[subprocess.CompletedProcess[str], Path]:
    """``plan`` submitted driverless (``pilots="local"``) under recorded bindings, then its driver job run
    here (``python -m …driver``) with a machine ad whose slot ``Memory`` is ``slot_mb``."""
    log_dir = tmp_path / "submit"
    log_dir.mkdir()
    fake = record_bindings(monkeypatch, RecordingSchedd())
    run_bounded(
        lambda: htcondor_api().submit_driverless(
            plan,
            site="generic",
            n_pilots=WORKERS,
            pilots="local",
            log_dir=log_dir,
            request_memory_mb=2048 + 2 * size_mb,
            user_modules=[HARNESS_FILE],
        )
    )
    (entry,) = submits(fake.log)
    job = job_dir(entry[1], log_dir, tmp_path / "job")
    ad = job / ".machine.ad"
    ad.write_text(f'Machine = "127.0.0.1"\nName = "slot1@127.0.0.1"\nCpus = 4\nMemory = {slot_mb}\n')
    proc = subprocess.run(
        [sys.executable, "-m", DRIVER_MODULE, str(job)],
        cwd=job,
        env=dict(os.environ, _CONDOR_MACHINE_AD=str(ad)),
        capture_output=True,
        text=True,
        timeout=RUN_TIMEOUT_S,
        check=False,
    )
    return proc, job


def test_a_driverless_job_hosts_the_servers_and_returns_resolved_histograms(
    size_mb: int, twin: dict[Any, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ports = free_range(4)
    _ctx, plan = two_server_plan(size_mb, ports)
    proc, job = driver_job(plan, size_mb, 65536, tmp_path, monkeypatch)
    log = (job / "driver.log").read_text() if (job / "driver.log").is_file() else "<no driver.log>"
    assert proc.returncode == 0, (proc.stderr, log)
    hosted = {name: int(port) for name, port in STATUS_LINE.findall(log)}
    assert sorted(hosted) == sorted(s.name for s in plan.services), log
    assert all(ports[0] <= p <= ports[1] and port_free(p) for p in hosted.values()), (hosted, ports)
    ok, result = pickle.loads((job / "result.pkl").read_bytes())
    assert ok is True, result
    assert same_values(result.value, twin)


def test_a_driver_job_holds_its_servers_to_its_slot_memory(
    size_mb: int, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _ctx, plan = two_server_plan(size_mb)
    slot = size_mb + size_mb // 2  # one server fits the slot, the two do not
    proc, job = driver_job(plan, size_mb, slot, tmp_path, monkeypatch)
    assert (job / "result.pkl").is_file(), (proc.returncode, proc.stderr)
    ok, result = pickle.loads((job / "result.pkl").read_bytes())
    text = f"{result} {getattr(result, 'legs', '')}"
    assert ok is False and named(slot, text, str(tmp_path)), result


def named(number: int, text: str, *cut: str) -> bool:
    """``number`` stands alone in ``text`` once ``cut`` (the run's own paths) is removed from it."""
    for ident in cut:
        text = text.replace(ident, "")
    return re.search(rf"(?<!\d){number}(?!\d)", text) is not None


def refused(runner: Any, size_mb: int) -> tuple[str, list[Any]]:
    """``legs["managed"]`` of the ``ServiceUnavailable`` a one-server plan of ``size_mb``, bound to no
    endpoint, raises on ``runner``, and every process ``subprocess.Popen`` started meanwhile."""
    require_histserv()
    ctx = histserv_api().Context(memory_mb=size_mb, workers=WORKERS, name=unique("m69b-refused"))
    plan = served_plan(SMALL, ctx)
    assert [s[1] for s in ctx.servers()] == [size_mb], ctx.servers()
    with popen_backstop() as started, pytest.raises(ServiceUnavailable) as err:
        run_bounded(lambda: runner.run(plan))
    return str(err.value.legs["managed"]), list(started)


def refused_start(backend: Any, size_mb: int) -> tuple[str, list[Any]]:
    runner = SubmitRunner(backend)
    try:
        return refused(runner, size_mb)
    finally:
        runner.close()


def test_a_server_the_set_driver_memory_cannot_hold_is_refused_before_it_starts() -> None:
    backend = ThreadBackend(WORKERS)
    backend.driver_memory_mb = 64
    managed, started = refused_start(backend, 1024)
    assert named(1024, managed) and named(64, managed), managed
    assert [p.args for p in started if "histserv" in str(p.args)] == []


def test_a_server_above_the_host_memory_is_refused_on_every_os() -> None:
    host = host_memory_mb()
    managed, started = refused_start(ThreadBackend(WORKERS), host + 1)
    assert named(host + 1, managed) and named(host, managed), (host, managed)
    assert [p.args for p in started if "histserv" in str(p.args)] == []


def test_the_driver_check_sums_the_servers_beside_the_driver(size_mb: int) -> None:
    _ctx, plan = two_server_plan(size_mb)
    backend = ThreadBackend(WORKERS)
    limit = size_mb + size_mb // 2  # one server fits, the two do not
    backend.driver_memory_mb = limit
    runner = SubmitRunner(backend)
    try:
        with popen_backstop() as started, pytest.raises(ServiceUnavailable) as err:
            run_bounded(lambda: runner.run(plan))
    finally:
        runner.close()
    managed = str(err.value.legs["managed"])
    assert named(size_mb, managed) and named(2 * size_mb, managed) and named(limit, managed), managed
    assert len([p for p in started if "histserv" in str(p.args)]) == 1, [p.args for p in started]
