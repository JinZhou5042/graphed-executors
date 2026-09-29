"""m68a ``ServiceSet`` and the engine's service phase over ``ThreadBackend`` and small duck-typed
backends: each leg, each refusal, the probe's identity rule and bound, the release of what a failed
start acquired, and the engine's bind/resolve around a run. The frozen suite pins these through its
harness; these witnesses keep ``submit/`` gated in the ``test-dask`` job, which runs no frozen m68a
protocol file, and reach the branches a correct run never takes (a child that dies after its check
passes, a child that ignores ``terminate``, graphed's resolve walk once it lands)."""

from __future__ import annotations

import contextlib
import logging
import socket
import subprocess
import sys
import threading
import time
import urllib.request
from collections.abc import Iterator, Mapping, Sequence
from concurrent.futures import Future
from dataclasses import dataclass, replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, cast

import graphed.services as graphed_services
import pytest
from graphed.core.execution import Partition, Plan, Task
from graphed.services import Launch, ServiceSpec

from graphed_executors.submit import SubmitRunner, ThreadBackend
from graphed_executors.submit import services as svc
from graphed_executors.submit.recipes import http_server

# ---- servers, plans, backends ------------------------------------------------------------------------


@contextlib.contextmanager
def serving(status: int = 200, ok_first: int | None = None) -> Iterator[str]:
    """An HTTP server on loopback answering ``status`` (after ``ok_first`` 200s, 503)."""
    gets = [0]

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            gets[0] += 1
            refused = ok_first is not None and gets[0] > ok_first
            self.send_response(503 if refused else status)
            self.send_header("content-length", "4")
            self.end_headers()
            self.wfile.write(b"body")

        def log_message(self, format: str, *args: object) -> None:
            return None

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(5.0)


def free_ports() -> tuple[int, int]:
    """A range of one port that was free a moment ago."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = int(sock.getsockname()[1])
    return (port, port)


def bare(name: str = "web", kind: str = "http") -> ServiceSpec:
    return ServiceSpec(name, kind, check="http:/", launch=None, timeout_s=5.0)


def child(
    argv: tuple[str, ...], *, check: str = "tcp", timeout_s: float = 10.0, **launch: Any
) -> ServiceSpec:
    spec_launch = Launch(argv, **launch)
    return ServiceSpec(
        "web", "http", check=check, ports=free_ports(), launch=spec_launch, timeout_s=timeout_s
    )


@dataclass(frozen=True)
class GetProcess:
    """Bindable and resolvable: a leaf GETs the bound endpoint; resolve GETs it again."""

    endpoint: str | None = None

    def bind_services(self, endpoints: Mapping[str, str]) -> GetProcess:
        return replace(self, endpoint=endpoints["web"])

    def __call__(self, partition: Partition, resources: object) -> tuple[str, ...]:
        assert self.endpoint is not None
        with urllib.request.urlopen(self.endpoint + "/", timeout=5) as resp:
            return (resp.read().decode(),)

    def resolve_services(self, value: tuple[str, ...]) -> tuple[str, ...]:
        return ("resolved", *value)


def cat(a: tuple[str, ...], b: tuple[str, ...]) -> tuple[str, ...]:
    return a + b


def none() -> tuple[str, ...]:
    return ()


def leaf_uri(partition: Partition, resources: object) -> tuple[str, ...]:
    return (partition.uri,)


def plan_of(process: Any, specs: Sequence[ServiceSpec], n: int = 2) -> Plan[Any]:
    tasks = tuple(Task(i, Partition(f"mem://m68a-extra/{i}", "", i, i + 1)) for i in range(n))
    return Plan(process=process, combine=cat, empty=none, tasks=tasks, services=tuple(specs))


class Duck(ThreadBackend):
    """``ThreadBackend`` with the duck-typed service attributes a test sets."""

    def __init__(self, **attrs: Any) -> None:
        super().__init__(2)
        self.cancelled: list[Any] = []
        for name, value in attrs.items():
            setattr(self, name, value)

    def cancel(self, futures: Sequence[Any]) -> None:
        self.cancelled.extend(futures)
        super().cancel(futures)


class Hosting(Duck):
    """Hosts a service on the cluster as an in-process HTTP server reported on ``service_identity``."""

    def __init__(self, service_identity: str, driver_identity: str, *, release_raises: bool = False) -> None:
        super().__init__()
        self.service_identity = service_identity
        self.driver_identity = driver_identity
        self.release_raises = release_raises
        self.released: list[str] = []
        self._servers: dict[str, contextlib.ExitStack] = {}

    def host_identity(self) -> str:
        return self.driver_identity

    def host_service(self, spec: ServiceSpec, scope: str) -> tuple[str, str, str]:
        stack = contextlib.ExitStack()
        endpoint = stack.enter_context(serving())
        key = f"{scope}-{len(self._servers)}"
        self._servers[key] = stack
        return endpoint, self.service_identity, key

    def release_service(self, key: str) -> None:
        self.released.append(key)
        self._servers.pop(key).close()
        if self.release_raises:
            raise RuntimeError("release fault (extra)")


class Silent(Duck):
    """A backend whose probe tasks no worker ever answers."""

    def submit(self, fn: Any, /, *args: object, key: str, **hints: Any) -> Any:
        if key.startswith("svc-"):
            return Future()
        return super().submit(fn, *args, key=key, **hints)


def statuses(caplog: pytest.LogCaptureFixture) -> list[svc.ServiceStatus]:
    return [r.status for r in caplog.records if hasattr(r, "status")]


# ---- the legs through the engine -----------------------------------------------------------------------


def test_a_given_endpoint_binds_and_the_value_is_resolved(caplog: pytest.LogCaptureFixture) -> None:
    with (
        serving() as endpoint,
        caplog.at_level(logging.INFO),
        SubmitRunner(ThreadBackend(2), services={"web": endpoint}) as runner,
    ):
        result = runner.run(plan_of(GetProcess(), [bare()]))
    assert result.value == ("resolved", "body", "body")
    (status,) = statuses(caplog)
    assert (status.leg, status.endpoint, status.identity) == ("user", endpoint, None)


def test_a_failing_given_endpoint_is_never_replaced() -> None:
    spec = child((sys.executable, "-c", "pass"), check="http:/")
    with (
        serving(503) as endpoint,
        SubmitRunner(ThreadBackend(2), services={"web": endpoint}) as runner,
        pytest.raises(svc.ServiceUnavailable) as excinfo,
    ):
        runner.run(plan_of(GetProcess(), [spec]))
    assert list(excinfo.value.legs) == ["user"] and endpoint in str(excinfo.value)


def test_the_site_leg_passes_or_falls_through(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)
    with serving() as site:
        backend = Duck(site_services={"http": site})
        with svc.ServiceSet([bare()], backend) as eps:
            assert dict(eps) == {"web": site}
        backend.close()
    with serving(503) as dead:
        backend = Duck(site_services={"http": dead}, service_hosts=("driver",), advertise_host="127.0.0.1")
        spec = replace(http_server("web"), ports=free_ports(), timeout_s=30.0)
        with svc.ServiceSet([spec], backend) as eps:
            assert eps["web"].startswith("http://127.0.0.1:")
        backend.close()
    site_leg, managed = statuses(caplog)
    assert site_leg.leg == "site" and managed.leg == "managed" and dead in managed.detail


def test_no_leg_and_a_recipe_that_needs_the_cluster_are_refused() -> None:
    backend = ThreadBackend(1)
    try:
        with pytest.raises(svc.ServiceUnavailable) as excinfo:
            svc.ServiceSet([bare()], backend).start()
        assert set(excinfo.value.legs) == {"user", "site", "managed"}
        gpu = child((sys.executable, "-c", "pass"), resources={"gpus": 1})
        with pytest.raises(svc.ServiceUnavailable, match="host_service"):
            svc.ServiceSet([gpu], backend).start()
        cluster_only = Duck(service_hosts=("cluster",))
        with pytest.raises(svc.ServiceUnavailable, match=r"service_hosts=\('cluster',\)"):
            svc.ServiceSet([child((sys.executable, "-c", "pass"))], cluster_only).start()
        cluster_only.close()
    finally:
        backend.close()


# ---- managed beside the driver -------------------------------------------------------------------------


def test_a_child_that_exits_before_its_check_passes() -> None:
    backend = ThreadBackend(1)
    try:
        with pytest.raises(svc.ServiceUnavailable, match="returncode 3"):
            svc.ServiceSet([child(("{python}", "-c", "raise SystemExit(3)"))], backend).start()
    finally:
        backend.close()


def test_a_child_that_exits_after_its_check_passed(monkeypatch: pytest.MonkeyPatch) -> None:
    def late_pass(endpoint: str, check: str, timeout: float) -> None:
        time.sleep(1.0)  # the child exits meanwhile

    monkeypatch.setattr(svc, "check_ready", late_pass)
    backend = ThreadBackend(1)
    try:
        with pytest.raises(svc.ServiceUnavailable, match="passed but the service exited"):
            svc.ServiceSet([child(("{python}", "-c", "pass"))], backend).start()
    finally:
        backend.close()


def test_a_check_that_never_passes_names_its_last_reason() -> None:
    backend = ThreadBackend(1)
    spec = child(("{python}", "-c", "import time; time.sleep(60)"), timeout_s=0.5)
    try:
        with pytest.raises(svc.ServiceUnavailable, match="never passed") as excinfo:
            svc.ServiceSet([spec], backend).start()
        assert "tcp connect" in excinfo.value.legs["managed"]
    finally:
        backend.close()


def test_a_held_range_is_the_bind_error() -> None:
    with socket.socket() as held:
        held.bind(("127.0.0.1", 0))
        held.listen(1)
        port = int(held.getsockname()[1])
        with pytest.raises(OSError):
            svc._free_port("127.0.0.1", (port, port))


class Stubborn:
    """A ``Popen`` stand-in that ignores ``terminate``."""

    pid = 4242

    def __init__(self) -> None:
        self.calls: list[str] = []

    def poll(self) -> int | None:
        return None

    def terminate(self) -> None:
        self.calls.append("terminate")

    def wait(self, timeout: float | None = None) -> int:
        self.calls.append(f"wait({timeout})")
        if timeout is not None:
            raise subprocess.TimeoutExpired("stubborn", timeout)
        return -9

    def kill(self) -> None:
        self.calls.append("kill")


def test_a_child_that_ignores_terminate_is_killed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(svc, "_GRACE_S", 0.01)
    proc = Stubborn()
    svc._stop_child(cast("subprocess.Popen[bytes]", proc), "web")
    assert proc.calls == ["terminate", "wait(0.01)", "kill", "wait(None)"]


# ---- on the cluster, and the probe ---------------------------------------------------------------------


def hosted() -> ServiceSpec:
    return ServiceSpec("web", "http", check="http:/", launch=Launch(("serve",), image="img"), timeout_s=10.0)


def test_a_hosted_service_passes_off_its_host_and_is_released(caplog: pytest.LogCaptureFixture) -> None:
    backend = Hosting("elsewhere.example", "driver.example")
    with caplog.at_level(logging.INFO), svc.ServiceSet([hosted()], backend, scope="s1") as eps:
        assert eps["web"].startswith("http://127.0.0.1:")
    assert backend.released == ["s1-0"]
    (status,) = statuses(caplog)
    assert (status.leg, status.host, status.identity) == ("managed", "cluster", "elsewhere.example")
    backend.close()


def test_a_hosted_service_only_its_own_host_reaches_is_refused(caplog: pytest.LogCaptureFixture) -> None:
    backend = Hosting(svc.host_identity(), "driver.example", release_raises=True)
    with caplog.at_level(logging.INFO), pytest.raises(svc.ServiceUnreachable) as excinfo:
        svc.ServiceSet([hosted()], backend, scope="s2").start()
    assert excinfo.value.reason == "only same-host workers answered"
    assert backend.released == ["s2-0"]
    assert any("release fault (extra)" in str(r.exc_info[1]) for r in caplog.records if r.exc_info)
    backend.close()


def test_a_probe_reason_and_an_unanswered_probe() -> None:
    backend = Duck()
    with serving(ok_first=1) as once, pytest.raises(svc.ServiceUnreachable, match="503") as excinfo:
        svc.ServiceSet([bare()], backend, endpoints={"web": once}).start()  # the leg check, then 503
    assert excinfo.value.worker == svc.host_identity()
    backend.close()
    silent = Silent()
    with serving() as ok, pytest.raises(svc.ServiceUnreachable, match="no worker answered"):
        svc.ServiceSet([replace(bare(), timeout_s=0.2)], silent, endpoints={"web": ok}).start()
    assert len(silent.cancelled) == 1 and silent.cancelled[0].cancelled()
    silent.close()


def test_statuses_are_stamped_when_the_set_closes() -> None:
    backend = ThreadBackend(1)
    with serving() as ok:
        service_set = svc.ServiceSet([bare()], backend, endpoints={"web": ok})
        with service_set:
            (open_status,) = service_set.statuses()
            assert open_status.closed_at is None
        (closed,) = service_set.statuses()
    assert closed.ready_at is not None and closed.closed_at is not None
    assert closed.closed_at >= closed.ready_at
    backend.close()


# ---- the engine ----------------------------------------------------------------------------------------


def test_graphed_s_resolve_walk_is_what_a_run_returns(monkeypatch: pytest.MonkeyPatch) -> None:
    """The engine hands the run's value to ``graphed.services.resolve_services`` with the bound plan
    and returns what it gives back; a process without the hook comes back unchanged."""
    seen: list[Any] = []
    real = graphed_services.resolve_services

    def walk(plan: Plan[Any], value: Any) -> Any:
        seen.append((plan.process, value))
        return ("walked", real(plan, value))

    plain = plan_of(leaf_uri, [])
    assert graphed_services.resolve_services(plain, ("v",)) == ("v",)  # no hook: the value itself
    monkeypatch.setattr(graphed_services, "resolve_services", walk)
    with serving() as endpoint, SubmitRunner(ThreadBackend(2), services={"web": endpoint}) as runner:
        result = runner.run(plan_of(GetProcess(), [bare()]))
    assert result.value == ("walked", ("resolved", "body", "body"))
    ((process, value),) = seen
    assert process == GetProcess(endpoint) and value == ("body", "body")


def test_a_run_without_services_cancels_nothing() -> None:
    backend = Duck()
    with SubmitRunner(backend) as runner:
        result = runner.run(plan_of(leaf_uri, []))
    assert result.n_partitions == 2 and backend.cancelled == []


def test_on_close_callbacks_run_first_and_a_raising_one_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    backend = ThreadBackend(1)
    ran: list[str] = []

    def raising() -> None:
        ran.append("raising")
        raise RuntimeError("on_close fault (extra)")

    with serving() as ok:
        with svc.ServiceSet([bare()], backend, endpoints={"web": ok}) as eps:
            eps.on_close(lambda: ran.append("first"))
            eps.on_close(raising)
        assert ran == ["raising", "first"]
    assert any("on_close fault (extra)" in str(r.exc_info[1]) for r in caplog.records if r.exc_info)
    backend.close()


def test_the_http_server_recipe_serves_its_root(tmp_path: Any) -> None:
    """``root`` is the served directory beside the driver too, not the driver's cwd."""
    (tmp_path / "marker.txt").write_text("from root")
    spec = replace(http_server("web", root=str(tmp_path)), ports=free_ports(), timeout_s=30.0)
    backend = ThreadBackend(1)
    try:
        with svc.ServiceSet([spec], backend) as eps, urllib.request.urlopen(eps["web"] + "/marker.txt") as r:
            assert r.read() == b"from root"
    finally:
        backend.close()


def test_the_scan_skips_a_port_something_answers_on(monkeypatch: pytest.MonkeyPatch) -> None:
    """A port whose bind passes (as on macOS/BSD beside a wildcard listener) is still skipped when a
    connect is answered; the next one is handed out."""
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        taken = int(listener.getsockname()[1])

        class Binds(socket.socket):
            def bind(self, address: Any) -> None:  # the BSD outcome: the bind itself passes
                if address[1] != taken:
                    super().bind(address)

        monkeypatch.setattr(socket, "socket", Binds)
        with pytest.raises(OSError, match="answers a connect"):
            svc._free_port("127.0.0.1", (taken, taken))
