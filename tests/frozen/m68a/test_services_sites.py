"""m68a D4 and the §3.1 ``htcondor_backend`` bullet: the site table's service data, the recipes, the
driver-job backend's service surface, and what the m66 acquirers leave behind when they fail. Every OS,
no bindings (``launch._htcondor`` is a recorder, and ``sys.modules["htcondor2"] = None`` in the driver-job
legs, so any bindings import there fails).

The measured rows (P6, P7, m66) are pinned; the ``services`` values are site data, so only their form is:
each passes ``split_endpoint``. A driver-job backend is taken from ``driver._runner(...)``, never built
with ``HTCondorBackend(..., in_job=)`` here, so the leg guards that ``driver.py`` passes ``in_job``.

Discriminates: drifted site data, a bare ``host:port`` site endpoint, a driver job that ignores its site
row (lxplus through the launcher fallback's generic row would offer ``("driver", "cluster")``; the
attached lxplus row offers ``("cluster",)``), a bindings-backed identity inside a job, and an acquirer
that leaves a pilot, a cluster or a bound port behind a failure.
"""

from __future__ import annotations

import dataclasses
import logging
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from graphed.services import split_endpoint
from services_harness import (
    FAKE_CLUSTER,
    NoStartLauncher,
    RecordingSchedd,
    backend_api,
    bounded_set,
    child_spec,
    closed_port,
    driver_api,
    free_range,
    htcondor_api,
    http_get,
    launch_api,
    pid_gone,
    popen_backstop,
    port_free,
    read_report,
    recipes_api,
    record_bindings,
    require_module,
    run_bounded,
    services_api,
    statuses_named,
    submit_api,
    to_http,
    with_ports,
    write_machine_ad,
)

BOUND_S = 120.0
MACHINE = "wn7.m68a.example"

ROWS = {
    "lpc": {
        "driver_ports": (10000, 10100),
        "service_ports": (10001, 10100),
        "worker_ports": (10000, 10100),
        "service_hosts": ("driver", "cluster"),
    },
    "lxplus": {
        "driver_ports": (8786, 8786),
        "service_ports": None,
        "worker_ports": (10000, 10100),
        "service_hosts": ("cluster",),
    },
    "generic": {
        "driver_ports": (10000, 10100),
        "service_ports": (10000, 10100),
        "worker_ports": (10000, 10100),
        "service_hosts": ("driver", "cluster"),
    },
}


def synthetic(name: str, **kwargs: Any) -> Any:
    return htcondor_api().SiteProfile(
        name=name, submit={}, spool=False, ship_env=False, sandbox_root=None, schedd_query=None, **kwargs
    )


# ---- the site table -----------------------------------------------------------------------------------


@pytest.mark.parametrize("site", sorted(ROWS))
def test_the_measured_rows(site: str) -> None:
    profile = htcondor_api().SITES[site]
    got = {field: getattr(profile, field) for field in ROWS[site]}
    assert got == ROWS[site]


@pytest.mark.parametrize("site", sorted(ROWS))
def test_site_services_are_endpoints(site: str) -> None:
    services = htcondor_api().SITES[site].services
    assert hasattr(services, "items"), services
    for kind, endpoint in services.items():
        assert isinstance(kind, str) and kind
        split_endpoint(endpoint)


@pytest.mark.parametrize(
    ("service_ports", "worker_ports", "hosts"),
    [
        ((20001, 20010), (20000, 20010), ("driver", "cluster")),
        (None, (20000, 20010), ("cluster",)),
        ((20001, 20010), None, ("driver",)),
        (None, None, ()),
    ],
    ids=["both", "no-service-ports", "no-worker-ports", "neither"],
)
def test_service_hosts_derive_from_the_port_ranges(
    service_ports: tuple[int, int] | None, worker_ports: tuple[int, int] | None, hosts: tuple[str, ...]
) -> None:
    profile = synthetic("m68a-synthetic", service_ports=service_ports, worker_ports=worker_ports, services={})
    assert profile.service_hosts == hosts
    assert dict(profile.services) == {}


@pytest.mark.parametrize(
    ("service_ports", "worker_ports"), [(None, (20000, 20010)), (None, None)], ids=["cluster-only", "neither"]
)
def test_an_attached_backend_without_a_driver_host_refuses_a_managed_start(
    service_ports: tuple[int, int] | None, worker_ports: tuple[int, int] | None, children: Any
) -> None:
    """Leg 3 on an attached profile without ``"driver"``: m68a has no ``host_service``, so the managed
    leg is refused naming the attribute; a profile with neither range leaves legs 1-2 only."""
    services = services_api()
    profile = synthetic("m68a-nodriver", service_ports=service_ports, worker_ports=worker_ports, services={})
    backend = run_bounded(
        lambda: backend_api().HTCondorBackend(
            NoStartLauncher(profile), 1, host="127.0.0.1", port_range=(0, 0)
        ),
        BOUND_S,
    )
    report = children.report()
    try:
        assert backend.service_hosts == profile.service_hosts
        assert dict(backend.site_services) == {}
        assert not callable(getattr(backend, "host_service", None))
        with pytest.raises(services.ServiceUnavailable) as excinfo:
            run_bounded(
                services.ServiceSet([child_spec("web", report, ports=free_range())], backend).start, BOUND_S
            )
    finally:
        run_bounded(backend.close, BOUND_S)
    assert set(excinfo.value.legs) == {"user", "site", "managed"}
    assert "host_service" in excinfo.value.legs["managed"], excinfo.value.legs
    assert not report.exists(), "a managed child started on a profile without a driver host"


@pytest.mark.parametrize("bare", ["triton.m68a.example:443", "grpcs://triton.m68a.example", "triton://h:1"])
def test_a_services_value_that_is_not_an_endpoint_fails_construction_naming_the_row(bare: str) -> None:
    with pytest.raises(ValueError) as excinfo:
        synthetic("m68a-bare-row", service_ports=None, worker_ports=None, services={"triton": bare})
    assert "m68a-bare-row" in str(excinfo.value), str(excinfo.value)
    ok = synthetic("m68a-good-row", services={"triton": "grpcs://triton.m68a.example:443"})
    assert dict(ok.services) == {"triton": "grpcs://triton.m68a.example:443"}


# ---- the recipes -----------------------------------------------------------------------------------------


def test_the_triton_recipe_is_grpc_only() -> None:
    spec = recipes_api().triton("triton", "/cvmfs/unpacked.example/tritonserver:24.11-py3", "models/")
    assert (spec.name, spec.kind, spec.check) == ("triton", "triton", "grpc:")
    argv = spec.launch.argv
    assert argv[0] == "tritonserver"
    for flag in (
        "--model-repository=models/",
        "--grpc-port={port}",
        "--allow-http=false",
        "--allow-metrics=false",
    ):
        assert flag in argv, argv
    assert spec.launch.inputs == ("models/",)
    assert spec.launch.image == "/cvmfs/unpacked.example/tritonserver:24.11-py3"
    assert spec.launch.resources["gpus"] == 1
    assert recipes_api().triton("t", "img", "m/", gpus=0).launch.resources.get("gpus", 0) == 0


def test_the_http_server_recipe() -> None:
    spec = recipes_api().http_server("web")
    assert (spec.name, spec.kind, spec.check) == ("web", "http", "http:/")
    assert spec.launch.argv == ("{python}", "-m", "http.server", "{port}")
    assert spec.launch.image is None and spec.launch.resources.get("gpus", 0) == 0


@pytest.mark.parametrize(
    ("rule", "scheme", "mode"),
    [("tcp", "tcp", "serve"), ("http:/", "http", "serve"), ("grpc:", "grpc", "grpc")],
)
def test_a_managed_endpoint_is_minted_with_the_check_scheme(
    rule: str, scheme: str, mode: str, children: Any
) -> None:
    if mode == "grpc":
        require_module("grpc")
        require_module("grpc_health.v1.health")
    services = services_api()
    report = children.report()
    spec = child_spec("svc", report, mode=mode, check=rule, ports=free_range())
    backend = submit_api().ThreadBackend(1)
    try:
        with bounded_set(services.ServiceSet([spec], backend)) as eps:
            endpoint = eps["svc"]
            assert endpoint.startswith(f"{scheme}://"), endpoint
            assert split_endpoint(endpoint) == (scheme, f"127.0.0.1:{endpoint.rsplit(':', 1)[1]}")
            pid = read_report(report)["pid"]
        assert pid_gone(pid)
    finally:
        run_bounded(backend.close, BOUND_S)


def test_python_is_rendered_as_the_driver_interpreter(children: Any) -> None:
    services = services_api()
    report = children.report()
    backend = submit_api().ThreadBackend(1)
    try:
        with bounded_set(services.ServiceSet([child_spec("svc", report, ports=free_range())], backend)):
            assert read_report(report)["python"] == sys.executable
    finally:
        run_bounded(backend.close, BOUND_S)


# ---- host identity -------------------------------------------------------------------------------------


def test_an_attached_backend_names_the_host_condor_writes_as_machine(monkeypatch: pytest.MonkeyPatch) -> None:
    record_bindings(monkeypatch, RecordingSchedd(), full_hostname="login42.m68a.example")
    backend = run_bounded(
        lambda: backend_api().HTCondorBackend(NoStartLauncher(), 1, host="127.0.0.1", port_range=(0, 0)),
        BOUND_S,
    )
    try:
        assert run_bounded(backend.host_identity, BOUND_S) == "login42.m68a.example"
    finally:
        run_bounded(backend.close, BOUND_S)


def driver_run(site: str, tmp_path: Path) -> dict[str, Any]:
    return {
        "pilots": "local",
        "n_pilots": 1,
        "site": site,
        "image": None,
        "log_dir": str(tmp_path / "pilots"),
        "request_memory_mb": 1024,
        "min_pilots": 1,
        "retries": 0,
        "max_in_flight": 1,
        "schedd_locate": None,
        "user_modules": [],
        "endpoints": {},
    }


def job_runner(site: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    """``driver._runner`` as a driver job calls it: a machine ad, no bindings."""
    monkeypatch.setitem(sys.modules, "htcondor2", None)
    monkeypatch.setenv("_CONDOR_MACHINE_AD", str(write_machine_ad(tmp_path / "machine.ad", MACHINE)))
    with open(tmp_path / "driver.log", "a") as log:
        return run_bounded(lambda: driver_api()._runner(driver_run(site, tmp_path), tmp_path, log), BOUND_S)


def test_the_lpc_driver_job_backend_reads_its_own_row(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = job_runner("lpc", tmp_path, monkeypatch)
    try:
        backend = runner.backend
        assert dict(backend.site_services) == dict(htcondor_api().SITES["lpc"].services)
        assert tuple(backend.service_hosts) == ("driver",)
        assert run_bounded(backend.host_identity, BOUND_S) == MACHINE
    finally:
        run_bounded(runner.close, BOUND_S)


def test_the_lxplus_driver_job_hosts_a_service_beside_the_driver(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    services = services_api()
    backstop = popen_backstop()  # http.server writes no pid report: stop it here if the set did not
    backstop.__enter__()
    runner = job_runner("lxplus", tmp_path, monkeypatch)
    try:
        backend = runner.backend
        assert tuple(backend.service_hosts) == ("driver",)
        assert run_bounded(backend.host_identity, BOUND_S) == MACHINE
        run_bounded(runner.wait_for_pilots, BOUND_S)
        spec = with_ports(recipes_api().http_server("web", root=str(tmp_path)), free_range())
        service_set = services.ServiceSet([spec], backend)
        with caplog.at_level(logging.INFO):
            eps = run_bounded(service_set.start, BOUND_S)
        try:
            endpoint = eps["web"]
            assert endpoint.startswith("http://127.0.0.1:"), endpoint
            assert http_get(to_http(endpoint) + "/")
            (status,) = statuses_named(caplog.records, "web")
            assert (status.leg, status.host, status.identity) == ("managed", "driver", MACHINE)
        finally:
            run_bounded(service_set.close, BOUND_S)
        assert port_free(int(endpoint.rsplit(":", 1)[1]))
    finally:
        try:
            run_bounded(runner.close, BOUND_S)
        finally:
            backstop.__exit__(None, None, None)


# ---- the m66 acquirers: a failure leaves nothing ---------------------------------------------------------


def _sleepers(monkeypatch: pytest.MonkeyPatch, fail_at: int | None) -> list[subprocess.Popen[bytes]]:
    """Replace ``subprocess.Popen`` (as the launcher reaches it) by one that starts a sleeper per pilot
    and raises on spawn number ``fail_at``."""
    real = subprocess.Popen
    spawned: list[subprocess.Popen[bytes]] = []

    def popen(cmd: Any, *args: Any, **kwargs: Any) -> subprocess.Popen[bytes]:
        if fail_at is not None and len(spawned) + 1 == fail_at:
            raise OSError(f"pilot spawn {fail_at} refused (injected)")
        proc = real([sys.executable, "-c", "import time; time.sleep(120)"])
        spawned.append(proc)
        return proc

    monkeypatch.setattr(launch_api().subprocess, "Popen", popen)
    return spawned


def _stop_all(procs: list[subprocess.Popen[bytes]]) -> None:
    for proc in procs:
        if proc.poll() is None:
            proc.kill()
        proc.wait(30)


def test_a_failed_second_pilot_spawn_leaves_no_pilot_and_no_port(monkeypatch: pytest.MonkeyPatch) -> None:
    spawned = _sleepers(monkeypatch, fail_at=2)
    port = closed_port()
    try:
        with pytest.raises(OSError, match="pilot spawn 2 refused"):
            run_bounded(
                lambda: backend_api().HTCondorBackend(
                    launch_api().LocalPilots(), 2, host="127.0.0.1", port_range=(port, port)
                ),
                BOUND_S,
            )
        assert len(spawned) == 1
        assert pid_gone(spawned[0].pid), "the first pilot outlived the refused start"
        assert port_free(port)
    finally:
        monkeypatch.undo()
        _stop_all(spawned)


def test_a_failed_spool_leaves_no_cluster_and_no_port(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    schedd = RecordingSchedd(
        queue=[[{"JobStatus": 5}]], spool_raises=RuntimeError("spool: connection reset (injected)")
    )
    record_bindings(monkeypatch, schedd)
    profile = dataclasses.replace(htcondor_api().SITES["generic"], spool=True)
    pilots = launch_api().CondorPilots(profile, log_dir=tmp_path / "pilots")
    port = closed_port()
    with pytest.raises(RuntimeError, match="spool"):
        run_bounded(
            lambda: backend_api().HTCondorBackend(
                pilots, 2, host="127.0.0.1", port_range=(port, port), service_hosts=("driver",)
            ),
            BOUND_S,
        )
    assert [e for e in schedd.log if e[0] == "submit"], schedd.log
    removes = [e for e in schedd.log if e[0] == "act" and "Remove" in e[1]]
    assert any(re.search(rf"ClusterId\s*==\s*{FAKE_CLUSTER}\b", e[2]) for e in removes), schedd.log
    assert port_free(port)


def test_a_failed_runner_in_the_driver_leaves_no_pilot_and_no_port(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    launch, driver = launch_api(), driver_api()
    spawned = _sleepers(monkeypatch, fail_at=None)
    urls: list[str] = []
    real_start = launch.LocalPilots.start

    def start(self: Any, url: str, secret: bytes, n: int) -> None:
        urls.append(url)
        real_start(self, url, secret, n)

    refused: list[bool] = []

    def refuse(*args: Any, **kwargs: Any) -> Any:
        refused.append(True)
        raise RuntimeError("runner refused (injected)")

    monkeypatch.setattr(launch.LocalPilots, "start", start)
    monkeypatch.setattr(driver, "HTCondorRunner", refuse)
    try:
        with pytest.raises(RuntimeError, match="runner refused"):
            job_runner("generic", tmp_path, monkeypatch)
        assert refused and len(urls) == 1 and spawned, (refused, urls, spawned)
        assert all(pid_gone(p.pid) for p in spawned), "a pilot outlived the refused runner"
        port = int(urls[0].rsplit(":", 1)[1])
        assert port_free(port), f"the task server still holds {port}"
    finally:
        monkeypatch.undo()
        _stop_all(spawned)


def test_close_frees_the_task_server_port_when_the_launcher_stop_raises() -> None:
    launcher = NoStartLauncher(stop_raises=RuntimeError("launcher stop raised (injected)"))
    port = closed_port()
    backend = run_bounded(
        lambda: backend_api().HTCondorBackend(launcher, 1, host="127.0.0.1", port_range=(port, port)), BOUND_S
    )
    assert not port_free(port)
    try:
        run_bounded(backend.close, BOUND_S)
    except RuntimeError as exc:
        assert "launcher stop raised" in str(exc)
    assert launcher.stops == 1
    assert port_free(port), "a raising launcher.stop left the task server's port bound"
