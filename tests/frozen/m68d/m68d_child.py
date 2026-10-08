"""The managed child of the m68d legs: ``<python> m68d_child.py <mode> <port> <report> [arg]``.

Stdlib only, python >= 3.9. Each start appends ``start <pid> <port>`` to ``<report>.starts``. A file
``<report>.exit.<pid>`` holding an integer makes that child exit with it. Modes:

- ``serve``: HTTP 200 on ``("", port)`` at once.
- ``gated``: waits for the file ``arg``, then binds ``("", port)``, retrying while another socket holds it.
- ``slow``: the first start in its cwd sleeps ``arg`` seconds, then binds as ``gated`` does; later starts
  bind at once.
"""

from __future__ import annotations

import os
import socketserver
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LIFETIME_S = 300.0  # a child the suite fails to stop still ends on its own
SLEPT = "m68d-slept"


class _Server(ThreadingHTTPServer):
    daemon_threads = True

    def server_bind(self) -> None:
        # HTTPServer.server_bind's getfqdn is a reverse lookup that can stall on macOS
        socketserver.TCPServer.server_bind(self)
        self.server_name = "localhost"
        self.server_port = self.server_address[1]


class _Ok(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        body = b"m-child"
        self.send_response(200)
        self.send_header("content-type", "text/plain")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        return None


def _exit_code(report: str) -> int | None:
    path = f"{report}.exit.{os.getpid()}"
    if not os.path.exists(path):
        return None
    with open(path) as f:
        text = f.read().strip()
    return int(text) if text else None


def _bind(port: int, report: str, deadline: float) -> _Server:
    while True:
        try:
            return _Server(("", port), _Ok)
        except OSError:
            code = _exit_code(report)
            if code is not None:
                sys.exit(code)
            if time.monotonic() > deadline:
                raise
            time.sleep(0.1)


def main(argv: list[str]) -> int:
    mode, port, report = argv[0], int(argv[1]), argv[2]
    arg = argv[3] if len(argv) > 3 else ""
    with open(report + ".starts", "a") as starts:
        starts.write(f"start {os.getpid()} {port}\n")
    deadline = time.monotonic() + LIFETIME_S
    if mode == "gated":
        while not os.path.exists(arg) and time.monotonic() < deadline:
            time.sleep(0.05)
    elif mode == "slow" and not os.path.exists(SLEPT):
        open(SLEPT, "w").close()
        time.sleep(float(arg))
    elif mode not in ("serve", "slow"):
        raise SystemExit("unknown mode " + mode)
    server = _bind(port, report, deadline)
    server.timeout = 0.1
    try:
        while time.monotonic() < deadline:
            server.handle_request()
            code = _exit_code(report)
            if code is not None:
                return code
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
