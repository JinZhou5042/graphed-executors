"""The managed child of the m68b ``announce.py`` legs: ``<python> m68b_child.py <mode> <port> <report> [code]``.

Stdlib only and python >= 3.9 (the no-``PATH`` leg runs it under the host's ``python3``). At start it appends
a line to ``<report>.starts`` and then writes ``<report>``: its pid, ``sys.executable``, its cwd's entries,
whether ``../graphed-secret`` exists, whether SIGTERM is blocked, and the two environment variables the
legs set. Modes:

- ``serve``: HTTP on ``("", port)`` serving its cwd's files.
- ``ignore``: ``serve`` with SIGTERM ignored.
- ``grpcish``: HTTP answering 200 ``application/grpc`` to every path (a gRPC gateway's shape).
- ``exit``: exits at once with ``code``.
- ``once``: answers one GET with 503, then exits with ``code``.
"""

from __future__ import annotations

import json
import os
import signal
import socketserver
import sys
import time
from http.server import BaseHTTPRequestHandler, SimpleHTTPRequestHandler, ThreadingHTTPServer

LIFETIME_S = 300.0  # a child the suite fails to stop still ends on its own
ENV_NAMES = ("M68B_JOB_VAR", "M68B_RECIPE_VAR")


class _Server(ThreadingHTTPServer):
    daemon_threads = True

    def server_bind(self) -> None:
        # HTTPServer.server_bind's getfqdn is a reverse lookup that can stall on macOS
        socketserver.TCPServer.server_bind(self)
        self.server_name = "localhost"
        self.server_port = self.server_address[1]


class _Quiet(SimpleHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:
        return None


class _Grpcish(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        self.send_response(200)
        self.send_header("content-type", "application/grpc")
        self.send_header("content-length", "0")
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:
        return None


class _Refuse(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        self.send_response(503)
        self.send_header("content-length", "0")
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:
        return None


def _sigterm_blocked() -> bool | None:
    if sys.platform == "win32":
        return None
    return signal.SIGTERM in signal.pthread_sigmask(signal.SIG_BLOCK, [])


def _report(path: str) -> None:
    with open(path + ".starts", "a") as starts:
        starts.write("start\n")
    state = {
        "pid": os.getpid(),
        "executable": sys.executable,
        "cwd": os.path.basename(os.getcwd()),
        "entries": sorted(os.listdir(".")),
        "secret_beside": os.path.exists(os.path.join("..", "graphed-secret")),
        "sigterm_blocked": _sigterm_blocked(),
        "env": {name: os.environ.get(name) for name in ENV_NAMES},
    }
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f)
    os.replace(tmp, path)


def _serve(port: int, handler: type[BaseHTTPRequestHandler]) -> None:
    server = _Server(("", port), handler)
    server.timeout = 0.5
    deadline = time.monotonic() + LIFETIME_S
    while time.monotonic() < deadline:
        server.handle_request()
    server.server_close()


def main(argv: list[str]) -> int:
    mode, port, report = argv[0], int(argv[1]), argv[2]
    code = int(argv[3]) if len(argv) > 3 else 0
    _report(report)
    if mode == "ignore":
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    if mode in ("serve", "ignore"):
        _serve(port, _Quiet)
    elif mode == "grpcish":
        _serve(port, _Grpcish)
    elif mode == "once":
        server = _Server(("", port), _Refuse)
        server.timeout = LIFETIME_S
        server.handle_request()
        server.server_close()
    elif mode != "exit":
        raise SystemExit("unknown mode " + mode)
    return code


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
