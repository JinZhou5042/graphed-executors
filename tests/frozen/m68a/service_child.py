"""A managed service for the m68a frozen suite, run as ``{python} service_child.py <mode> <port> <report>``.

Stdlib only (gRPC mode imports grpcio lazily), so a ``Launch`` recipe can start it from any interpreter.
Before anything else it writes ``<report>`` = ``{"pid": <its pid>, "python": <the interpreter argv[0]>}``,
so a test learns the pid of a child it never spawned itself, and which ``{python}`` the recipe rendered.

Modes:

- ``serve``: HTTP on ``("", port)``; every GET answers 200 ``text/plain`` with the child's pid.
- ``never``: binds nothing and sleeps; a readiness check never passes.
- ``grpc``: a gRPC server with the reference ``grpc.health.v1`` servicer answering SERVING.
"""

from __future__ import annotations

import importlib
import json
import os
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LIFETIME_S = 300.0  # a child the suite fails to stop still ends on its own


class _PidHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        body = str(os.getpid()).encode()
        self.send_response(200)
        self.send_header("content-type", "text/plain")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        return None


def _report(path: str) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w") as f:
        json.dump({"pid": os.getpid(), "python": sys.orig_argv[0]}, f)
    os.replace(tmp, path)


def _serve_http(port: int) -> None:
    server = ThreadingHTTPServer(("", port), _PidHandler)
    server.daemon_threads = True
    server.timeout = 0.5
    deadline = time.monotonic() + LIFETIME_S
    while time.monotonic() < deadline:
        server.handle_request()
    server.server_close()


def _serve_grpc(port: int) -> None:
    grpc = importlib.import_module("grpc")
    health = importlib.import_module("grpc_health.v1.health")
    health_pb2 = importlib.import_module("grpc_health.v1.health_pb2")
    health_pb2_grpc = importlib.import_module("grpc_health.v1.health_pb2_grpc")
    futures = importlib.import_module("concurrent.futures")
    server = grpc.server(futures.ThreadPoolExecutor(2))
    servicer = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(servicer, server)
    servicer.set("", health_pb2.HealthCheckResponse.SERVING)
    server.add_insecure_port(f"0.0.0.0:{port}")
    server.start()
    server.wait_for_termination(LIFETIME_S)


def main(argv: list[str]) -> int:
    mode, port, report = argv[0], int(argv[1]), argv[2]
    _report(report)
    if mode == "serve":
        _serve_http(port)
    elif mode == "grpc":
        _serve_grpc(port)
    elif mode == "never":
        time.sleep(LIFETIME_S)
    else:
        raise SystemExit(f"unknown mode {mode!r}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
