"""m68a §3.1 ``check_ready`` and D1's wire rule, against in-process servers, on every OS.

``check_ready(endpoint, check, timeout) -> None | reason`` is the one readiness check every caller runs:
``tcp`` connects; ``http:<path>`` GETs and is ready on a 2xx that is not ``application/grpc*`` (the EAF
gateway answers 200 ``application/grpc`` on every path, P8); ``grpc:<service>`` is the standard
``grpc.health.v1`` Check, ready iff SERVING, over TLS exactly for ``grpcs``. A check runs only over a wire
that carries it; a mismatch or an unknown form is refused without dialling.

Witnesses: the reason (``None`` or text), the accept counter of each server, and the first bytes a
sniffing proxy saw (``0x16`` = a TLS ClientHello, so a ``grpcs``/``https`` check attempted TLS).

The gRPC cases need ``grpcio`` and ``grpcio-health-checking`` (the reference servicer): they skip only
on a free-threaded interpreter (no wheel); wherever the GIL is enabled a missing package fails.

Discriminates: an HTTP check passing a gRPC gateway, a scheme-blind TLS choice, a connect-only probe.
"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest
from services_harness import (
    CountingHTTPServer,
    CountingListener,
    SniffProxy,
    grpc_bare_server,
    grpc_health_server,
    run_bounded,
    services_api,
    submit_api,
)

TIMEOUT_S = 5.0


def check(endpoint: str, rule: str) -> str | None:
    reason: str | None = run_bounded(lambda: services_api().check_ready(endpoint, rule, TIMEOUT_S), 60.0)
    return reason


@contextmanager
def plain_http_server(root: Path) -> Iterator[int]:
    """``http.server``'s own handler over ``root``: what ``recipes.http_server`` runs."""

    class Quiet(SimpleHTTPRequestHandler):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, directory=str(root), **kwargs)

        def log_message(self, format: str, *args: object) -> None:
            return None

    server = ThreadingHTTPServer(("127.0.0.1", 0), Quiet)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
    thread.start()
    try:
        yield int(server.server_address[1])
    finally:
        server.shutdown()
        server.server_close()
        thread.join(5.0)


# ---- gRPC health ------------------------------------------------------------------------------------

HEALTH = {"": "SERVING", "svc.A": "SERVING", "svc.B": "NOT_SERVING"}


def test_grpc_check_passes_on_serving_and_fails_otherwise() -> None:
    with grpc_health_server(HEALTH) as port, SniffProxy(port) as proxy:
        assert check(proxy.endpoint("grpc"), "grpc:") is None
        assert check(proxy.endpoint("grpc"), "grpc:svc.A") is None
        not_serving = check(proxy.endpoint("grpc"), "grpc:svc.B")
        unknown = check(proxy.endpoint("grpc"), "grpc:svc.unknown")
        assert proxy.accepts >= 1 and not proxy.tls_attempted(), proxy.first_bytes
    assert not_serving, "NOT_SERVING passed"
    assert unknown, "an unknown service passed"


def test_grpcs_on_a_plaintext_server_attempts_tls_and_fails() -> None:
    with grpc_health_server(HEALTH) as port, SniffProxy(port) as proxy:
        reason = check(proxy.endpoint("grpcs"), "grpc:")
        assert reason, "grpcs passed against a plaintext server"
        assert proxy.tls_attempted(), f"no TLS ClientHello was sent: {proxy.first_bytes}"


def test_a_grpc_server_without_health_fails() -> None:
    with grpc_bare_server() as port:
        assert check(f"grpc://127.0.0.1:{port}", "grpc:")


# ---- HTTP -------------------------------------------------------------------------------------------


def test_an_http_check_fails_on_a_grpc_gateway_answering_200_everywhere() -> None:
    with CountingHTTPServer("grpcish") as gateway:
        assert check(gateway.endpoint("http"), "http:/v2/health/ready")
        assert gateway.gets >= 1, "the check never asked"


def test_http_check_passes_on_http_server_and_fails_as_https(tmp_path: Path) -> None:
    with plain_http_server(tmp_path) as port, SniffProxy(port) as proxy:
        assert check(proxy.endpoint("http"), "http:/") is None
        assert not proxy.tls_attempted()
        reason = check(proxy.endpoint("https"), "http:/")
        assert reason, "https passed against a plaintext server"
        assert proxy.tls_attempted(), f"no TLS ClientHello was sent: {proxy.first_bytes}"


@pytest.mark.parametrize(
    ("scheme", "rule"),
    [("grpc", "http:/"), ("grpcs", "http:/"), ("http", "grpc:"), ("https", "grpc:")],
)
def test_a_check_over_a_wire_that_cannot_carry_it_fails_undialled(scheme: str, rule: str) -> None:
    with CountingListener() as listener:
        reason = check(listener.endpoint(scheme), rule)
        assert reason, f"{rule} passed over {scheme}://"
        assert rule.rstrip(":/").split(":")[0] in reason and scheme in reason, reason
        assert listener.settled() == 0, "a mismatched check dialled"


@pytest.mark.parametrize("scheme", ["tcp", "http", "https", "grpc", "grpcs"])
def test_tcp_passes_on_every_scheme(scheme: str) -> None:
    with CountingListener() as listener:
        assert check(listener.endpoint(scheme), "tcp") is None
        assert listener.settled() == 1


@pytest.mark.parametrize("rule", ["udp", "ping:/", "https:/", "tcp:/"])
def test_an_unknown_check_form_is_refused_undialled(rule: str) -> None:
    with CountingListener() as listener:
        assert check(listener.endpoint("tcp"), rule), f"{rule!r} was accepted"
        assert listener.settled() == 0


def test_a_closed_port_fails_tcp() -> None:
    with CountingListener() as listener:
        endpoint = listener.endpoint("tcp")
    assert check(endpoint, "tcp")


# ---- the probe task and endpoint form -----------------------------------------------------------------


def test_the_probe_reports_not_serving_where_a_connect_would_pass() -> None:
    services = services_api()
    with grpc_health_server(HEALTH) as port:
        endpoint = f"grpc://127.0.0.1:{port}"
        assert check(endpoint, "tcp") is None  # a connect-only probe would pass here
        _identity, reasons = run_bounded(lambda: services._probe_services([(endpoint, "grpc:svc.B")]), 60.0)
        got = list(reasons.values() if hasattr(reasons, "values") else reasons)
        assert len(got) == 1 and isinstance(got[0], str) and got[0], reasons
        _identity, reasons = run_bounded(lambda: services._probe_services([(endpoint, "grpc:svc.A")]), 60.0)
        assert list(reasons.values() if hasattr(reasons, "values") else reasons) == [None]


@pytest.mark.parametrize("endpoint", ["h:1", "127.0.0.1:8001", "triton://h:1", "http://h", "http://h:1/path"])
def test_a_service_set_refuses_an_endpoint_without_a_scheme_naming_the_schemes(endpoint: str) -> None:
    services = services_api()
    backend = submit_api().ThreadBackend(1)
    try:
        with pytest.raises(ValueError) as excinfo:
            run_bounded(lambda: services.ServiceSet((), backend, endpoints={"x": endpoint}), 60.0)
    finally:
        run_bounded(backend.close, 60.0)
    assert all(s in str(excinfo.value) for s in ("tcp", "http", "https", "grpc", "grpcs")), str(excinfo.value)
