"""m68d B (plan §2 "B — the SERVICE node restarts its service, and a running driver rebinds"), rows
B1-B3 of §4. B1, B1b and B2 run ``announce.py`` as a subprocess (POSIX); B3 runs on every OS."""

from __future__ import annotations

import os
import pickle
import re
import signal
import sys
from pathlib import Path
from typing import Any

import pytest
from graphed.core.execution import SequentialRunner
from m68d_harness import (
    CHILD_FILE,
    NODE,
    AnnounceRun,
    Background,
    GatedGet,
    NoLauncher,
    Web,
    announce_api,
    announce_node,
    child_starts,
    driver_api,
    end_child,
    free_range,
    gated_plan,
    log_lines,
    node_spec,
    run_bounded,
    run_json,
    server_api,
    service_json,
    site_copy,
    wait_for,
    write_driver_job,
    write_machine_ad,
)

posix = pytest.mark.skipif(sys.platform == "win32", reason="announce.py's child is signalled (POSIX)")
LOOPBACK = "127.0.0.1"
ANNOUNCED = "-> 200"  # announce.py's line for an announce the driver took


def node_env(tmp_path: Path) -> dict[str, str]:
    """The node's environment: this process's, with a machine ad naming loopback."""
    return {**os.environ, "_CONDOR_MACHINE_AD": str(write_machine_ad(tmp_path / "node.ad", LOOPBACK))}


def watched_server(dag_dir: Path) -> Any:
    """A task server whose url and announce secret for svc0 are published in ``dag_dir``."""
    server = server_api().TaskServer(LOOPBACK, (0, 0), NoLauncher())
    secret = server.announce_secret([NODE])
    dag_dir.mkdir(parents=True, exist_ok=True)
    (dag_dir / "graphed-secret").write_text(secret.hex())
    (dag_dir / "driver.url").write_text(server.url)
    return server


def child_argv(report: Path) -> list[str]:
    return ["{python}", CHILD_FILE, "serve", "{port}", str(report)]


def announced_port(server: Any, timeout_s: float = 60.0) -> int | None:
    got = run_bounded(lambda: server.wait_announce(NODE, timeout_s), timeout_s + 30.0)
    return None if got is None else int(got[0].rpartition(":")[2])


# ---- B1: watch mode restarts a dead child ---------------------------------------------------------------


@posix
def test_watch_mode_restarts_a_dead_child_on_another_port_and_reannounces(tmp_path: Path) -> None:
    restarts = announce_api().RESTARTS
    assert restarts == 3
    server = watched_server(tmp_path / "dag")
    report = tmp_path / "child"
    job = service_json(tmp_path / "job", argv=child_argv(report), ports=list(free_range(8)), watch=str(tmp_path / "dag"))
    ports: list[int] = []
    try:
        with AnnounceRun(job, node_env(tmp_path), report) as node:
            for death in range(restarts + 1):
                port = announced_port(server)
                assert port is not None, f"announce {death + 1} never arrived:\n{node.output()}"
                ports.append(port)
                pid, child_port = child_starts(report)[-1]
                assert child_port == port, (child_starts(report), port)
                end_child(report, pid, 71 + death)
            assert node.wait(60.0) == 71 + restarts, node.output()
    finally:
        server.close()
        server.shutdown()
    assert len(set(ports)) == restarts + 1, f"announced ports {ports}"
    assert len(child_starts(report)) == restarts + 1, child_starts(report)


@posix
def test_an_attached_child_that_dies_still_ends_the_job(tmp_path: Path) -> None:
    server = watched_server(tmp_path / "dag")
    try:
        # control: the same child in watch mode is started again after its death
        report = tmp_path / "watched"
        job = service_json(tmp_path / "watch", argv=child_argv(report), ports=list(free_range(4)), watch=str(tmp_path / "dag"))
        with AnnounceRun(job, node_env(tmp_path), report):
            assert announced_port(server) is not None
            end_child(report, child_starts(report)[-1][0], 71)
            assert wait_for(lambda: len(child_starts(report)) == 2, 30.0), child_starts(report)
        report = tmp_path / "attached"
        secret = server.announce_secret(["k-attached"])
        job = service_json(
            tmp_path / "attach", argv=child_argv(report), ports=list(free_range(4)), key="k-attached", url=server.url
        )
        (job / "graphed-secret").write_text(secret.hex())
        with AnnounceRun(job, node_env(tmp_path), report) as node:
            assert run_bounded(lambda: server.wait_announce("k-attached", 60.0), 90.0) is not None, node.output()
            end_child(report, child_starts(report)[-1][0], 75)
            assert node.wait(60.0) == 75, node.output()
        assert len(child_starts(report)) == 1, child_starts(report)
    finally:
        server.close()
        server.shutdown()


# ---- B2: the driver reruns once the restarted node re-announces ----------------------------------------


def managed_ports(job: Path) -> list[int]:
    return [int(re.findall(r":(\d+)\s*$", line)[0]) for line in log_lines(job, "managed leg (cluster)")]


@posix
def test_the_driver_reruns_once_a_restarted_service_node_reannounces(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env = node_env(tmp_path)
    monkeypatch.setenv("_CONDOR_MACHINE_AD", env["_CONDOR_MACHINE_AD"])
    site_copy(monkeypatch, "m-dee-node", "generic", worker_ports=free_range(3))
    mark, job, dag = tmp_path / "mark", tmp_path / "job", tmp_path / "dag"
    dag.mkdir()
    plan = gated_plan(mark, node_spec(30.0))
    write_driver_job(job, plan, run_json("m-dee-node", dag_dir=dag))
    report = tmp_path / "child"
    node_job = service_json(tmp_path / "node", argv=child_argv(report), ports=list(free_range(4)), watch=str(dag))
    with AnnounceRun(node_job, env, report) as node:
        driver = Background(lambda: driver_api().main([str(job)]))
        assert wait_for((mark / "t0.done").exists, 120.0), f"task 0 never finished:\n{node.output()}"
        os.kill(child_starts(report)[-1][0], signal.SIGKILL)
        # the restart's announce is pending before the first set is released
        assert wait_for(lambda: node.count(ANNOUNCED) >= 2, 60.0), node.output()
        (mark / "killed").touch()
        code = driver.result()
    ok, value = pickle.loads((job / "result.pkl").read_bytes())
    twin_mark = tmp_path / "twin"
    twin_mark.mkdir()
    (twin_mark / "killed").touch()
    with Web() as web:
        twin = SequentialRunner().run(gated_plan(twin_mark, None, process=GatedGet(web.endpoint)))
    assert (code, ok) == (0, True), (code, value, (job / "driver.log").read_text()[-4000:])
    assert pickle.dumps(value.value) == pickle.dumps(twin.value), (value, twin)
    assert len(log_lines(job, "rerun:")) == 1, log_lines(job, "rerun:")
    ports = managed_ports(job)
    assert len(ports) == 2 and ports[0] != ports[1], log_lines(job, "managed leg (cluster)")
    assert len(log_lines(job, " pilots on ")) == 1, log_lines(job, " pilots on ")


# ---- B3: a rerun waits for a fresh announce -------------------------------------------------------------


def test_a_rerun_waits_for_a_fresh_announce(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("_CONDOR_MACHINE_AD", str(write_machine_ad(tmp_path / "driver.ad", LOOPBACK)))
    site_copy(monkeypatch, "m-dee-node", "generic", worker_ports=free_range(3))
    mark, job, dag = tmp_path / "mark", tmp_path / "job", tmp_path / "dag"
    dag.mkdir()
    write_driver_job(job, gated_plan(mark, node_spec(0.5), n=2), run_json("m-dee-node", dag_dir=dag))
    web = Web()
    driver = Background(lambda: driver_api().main([str(job)]))
    assert announce_node(dag, web.port) == 200
    assert wait_for((mark / "t0.done").exists, 120.0), (job / "driver.log").read_text()[-4000:]
    web.close()
    (mark / "killed").touch()
    code = driver.result()
    ok, payload = pickle.loads((job / "result.pkl").read_bytes())
    assert (code, ok) == (1, False), (code, payload)
    assert isinstance(payload, TimeoutError) and "timeout_s=0.5" in str(payload), repr(payload)
    assert len(log_lines(job, "rerun:")) == 1, log_lines(job, "rerun:")
