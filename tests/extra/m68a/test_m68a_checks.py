"""m68a ``check_ready``, ``host_identity`` and the small pieces of ``submit/services.py`` the frozen suite
reaches only through the whole path: every readiness form against in-process servers (the gRPC ones when
grpcio and its reference health servicer are installed), the endpoint form, the minted scheme, the
request encoding, a missing grpcio, and the ``Endpoints`` map. These run in the ``test-dask`` job too,
which gates ``submit/`` on its own suite."""

from __future__ import annotations

import contextlib
import importlib
import pickle
import socket
import sys
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest

from graphed_executors.submit import ThreadBackend, recipes
from graphed_executors.submit import services as svc

TIMEOUT_S = 5.0


@contextlib.contextmanager
def http_server(status: int = 200, ctype: str = "text/plain") -> Iterator[str]:
    """An HTTP server on loopback answering every GET with ``status`` and ``ctype``; yields its
    ``http://`` endpoint."""

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self.send_response(status)
            self.send_header("content-type", ctype)
            self.send_header("content-length", "2")
            self.end_headers()
            self.wfile.write(b"ok")

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


def closed_endpoint(scheme: str) -> str:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    return f"{scheme}://127.0.0.1:{port}"


def test_http_checks_by_answer() -> None:
    with http_server() as ok:
        assert svc.check_ready(ok, "http:/", TIMEOUT_S) is None
        assert svc.check_ready(ok, "tcp", TIMEOUT_S) is None
        https = "https" + ok.removeprefix("http")
        assert "failed" in str(svc.check_ready(https, "http:/", TIMEOUT_S))
    with http_server(503) as refusing:
        assert "503" in str(svc.check_ready(refusing, "http:/ready", TIMEOUT_S))
    with http_server(200, "application/grpc") as gateway:
        assert "gRPC endpoint" in str(svc.check_ready(gateway, "http:/", TIMEOUT_S))


def test_a_closed_port_and_forms_that_are_refused_undialled() -> None:
    assert "tcp connect" in str(svc.check_ready(closed_endpoint("tcp"), "tcp", TIMEOUT_S))
    assert "failed" in str(svc.check_ready(closed_endpoint("http"), "http:/", TIMEOUT_S))
    assert "not dialled" in str(svc.check_ready("grpc://127.0.0.1:1", "http:/", TIMEOUT_S))
    assert "not dialled" in str(svc.check_ready("http://127.0.0.1:1", "grpc:", TIMEOUT_S))
    assert "unknown check" in str(svc.check_ready("http://127.0.0.1:1", "http:no-slash", TIMEOUT_S))
    assert "grpcs" in str(svc.check_ready("127.0.0.1:1", "tcp", TIMEOUT_S))  # not an endpoint


def test_a_missing_grpcio_is_a_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "grpc", None)
    assert "needs grpcio" in str(svc.check_ready("grpc://127.0.0.1:1", "grpc:", TIMEOUT_S))


def test_the_health_request_encodes_the_service_name() -> None:
    assert svc._varint(1) == b"\x01"
    assert svc._varint(300) == b"\xac\x02"


def gil_or_skip(name: str) -> Any:
    gil_enabled = getattr(sys, "_is_gil_enabled", None)
    if gil_enabled is None or gil_enabled():
        return importlib.import_module(name)
    return pytest.importorskip(name)


@contextlib.contextmanager
def health_server() -> Iterator[int]:
    grpc = gil_or_skip("grpc")
    health = gil_or_skip("grpc_health.v1.health")
    health_pb2 = gil_or_skip("grpc_health.v1.health_pb2")
    health_pb2_grpc = gil_or_skip("grpc_health.v1.health_pb2_grpc")
    futures = importlib.import_module("concurrent.futures")
    server = grpc.server(futures.ThreadPoolExecutor(2))
    servicer = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(servicer, server)
    servicer.set("", health_pb2.HealthCheckResponse.SERVING)
    servicer.set("x" * 200, health_pb2.HealthCheckResponse.SERVING)
    servicer.set("down", health_pb2.HealthCheckResponse.NOT_SERVING)
    port = int(server.add_insecure_port("127.0.0.1:0"))
    server.start()
    try:
        yield port
    finally:
        server.stop(0).wait(5.0)


def test_grpc_health_checks() -> None:
    with health_server() as port:
        endpoint = f"grpc://127.0.0.1:{port}"
        assert svc.check_ready(endpoint, "grpc:", TIMEOUT_S) is None
        assert svc.check_ready(endpoint, "grpc:" + "x" * 200, TIMEOUT_S) is None  # a two-byte length
        assert "not SERVING" in str(svc.check_ready(endpoint, "grpc:down", TIMEOUT_S))
        assert "failed" in str(svc.check_ready(endpoint, "grpc:unknown", TIMEOUT_S))
        assert "failed" in str(svc.check_ready(f"grpcs://127.0.0.1:{port}", "grpc:", TIMEOUT_S))


def test_host_identity_reads_machine_from_the_ad(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ad = tmp_path / "machine.ad"
    ad.write_text('Name = "slot1@x"\nCpus = 1\n')
    monkeypatch.setenv("_CONDOR_MACHINE_AD", str(ad))
    assert svc.host_identity() == socket.getfqdn()  # an ad without Machine
    ad.write_text('Name = "slot1@x"\nMachine = "wn9.example"\n')
    assert svc.host_identity() == "wn9.example"
    monkeypatch.setenv("_CONDOR_MACHINE_AD", str(tmp_path / "absent.ad"))
    assert svc.host_identity() == socket.getfqdn()


def test_minted_endpoints_take_the_check_wire() -> None:
    assert svc.minted_endpoint("http:/x", "h", 1) == "http://h:1"
    assert svc.minted_endpoint("grpc:", "::1", 2) == "grpc://[::1]:2"
    assert svc.minted_endpoint("tcp", "h", 3) == "tcp://h:3"


def test_the_probe_task_answers_identity_and_reasons() -> None:
    with http_server() as ok:
        identity, reasons = svc._probe_services([(ok, "http:/"), (closed_endpoint("tcp"), "tcp")])
    assert identity == svc.host_identity()
    assert reasons[0] is None and "tcp connect" in str(reasons[1])


def test_the_endpoints_map() -> None:
    backend = ThreadBackend(1)
    try:
        service_set = svc.ServiceSet((), backend)
        eps = service_set.start()
        assert dict(eps) == {} and len(eps) == 0 and repr(eps) == "Endpoints({})"
        assert pickle.loads(pickle.dumps(eps)) == {}
        with pytest.raises(RuntimeError, match="already started"):
            service_set.start()
        service_set.close()
        service_set.close()  # a second close is a no-op
        with pytest.raises(RuntimeError, match="not open"):
            eps.on_close(lambda: None)
    finally:
        backend.close()


def test_refusals_carry_their_fields_through_pickle() -> None:
    unavailable = svc.ServiceUnavailable("s", {"user": "no endpoint given", "managed": "no recipe"})
    back = pickle.loads(pickle.dumps(unavailable))
    assert (back.name, back.legs) == ("s", unavailable.legs)
    assert "user: no endpoint given; managed: no recipe" in str(back)
    unreachable = svc.ServiceUnreachable("s", "tcp://h:1", "", "no worker answered")
    assert str(unreachable) == "service 's' at tcp://h:1 is unreachable: no worker answered"
    assert "from worker w" in str(svc.ServiceUnreachable("s", "tcp://h:1", "w", "refused"))


def test_the_recipes_are_data() -> None:
    spec = recipes.triton("t", "img", "models/", gpus=0)
    assert spec.launch is not None and (spec.kind, spec.check) == ("triton", "grpc:")
    assert spec.launch.inputs == ("models/",) and spec.launch.resources["gpus"] == 0
    default = ("{python}", "-m", "http.server", "{port}")
    web = recipes.http_server("web")
    assert web.launch is not None and (web.launch.argv, web.launch.inputs) == (default, ())
    web = recipes.http_server("web", root="site/")
    assert web.launch is not None and web.launch.inputs == ("site/",)
    assert web.launch.argv == (*default, "--directory", "site/")
