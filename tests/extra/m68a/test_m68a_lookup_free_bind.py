"""Every product HTTP server binds and answers without ``socket.getfqdn``, whose reverse lookup stalls on
macOS CI runners and leaves a bound server not listening."""

from __future__ import annotations

import ast
import contextlib
import socket
import threading
import urllib.request
from collections.abc import Callable, Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest

import graphed_executors
from graphed_executors.common.http_plane import _DualRouteServer
from graphed_executors.htcondor_backend import server as condor_server
from graphed_executors.local import shuffle
from graphed_executors.local._transport import LookupFreeHTTPServer, _InboxServer

SRC = Path(graphed_executors.__file__).parent


@pytest.fixture
def no_fqdn(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*_args: object) -> str:
        raise AssertionError("socket.getfqdn called while binding")

    monkeypatch.setattr(socket, "getfqdn", refuse)


class _Ok(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        self.send_response(200)
        self.send_header("content-length", "0")
        self.end_headers()

    def log_message(self, *_args: Any) -> None:
        return None


@contextlib.contextmanager
def serving(server: ThreadingHTTPServer) -> Iterator[int]:
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield int(server.server_address[1])
    finally:
        server.shutdown()
        server.server_close()


def test_the_patched_lookup_is_live(no_fqdn: None) -> None:
    with pytest.raises(AssertionError, match="getfqdn"):
        ThreadingHTTPServer(("127.0.0.1", 0), _Ok)


def test_the_base_binds_and_answers_without_a_lookup(no_fqdn: None) -> None:
    server = LookupFreeHTTPServer(("127.0.0.1", 0), _Ok)
    assert server.server_name == "127.0.0.1"
    with serving(server) as port, urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=5) as resp:
        assert resp.status == 200


def _node(tmp: Path) -> tuple[ThreadingHTTPServer, Callable[[], None]]:
    node = shuffle._HttpNode("127.0.0.1", tmp)  # serves from construction
    return node._server, node.close


def _bare(server: ThreadingHTTPServer) -> tuple[ThreadingHTTPServer, Callable[[], None]]:
    return server, server.server_close


BUILDERS: dict[str, Callable[[Path], tuple[ThreadingHTTPServer, Callable[[], None]]]] = {
    "transport inbox": lambda _: _bare(_InboxServer(("127.0.0.1", 0))),
    "http plane": lambda _: _bare(
        _DualRouteServer(("127.0.0.1", 0), epoch="e", inbox_maxsize=None, recv_failures=None)
    ),
    "condor task server": lambda _: _bare(condor_server._bind((45100, 45199))),
    "shuffle node": _node,
}


@pytest.mark.parametrize("name", sorted(BUILDERS))
def test_each_product_server_binds_without_a_lookup(name: str, no_fqdn: None, tmp_path: Path) -> None:
    server, close = BUILDERS[name](tmp_path)
    try:
        assert isinstance(server, LookupFreeHTTPServer)
        with socket.create_connection(("127.0.0.1", int(server.server_address[1])), timeout=5):
            pass  # listening: a stalled bind would have raised above, before listen()
    finally:
        close()


def test_no_product_module_builds_a_plain_http_server() -> None:
    found = []
    for path in sorted(SRC.rglob("*.py")):
        tree = ast.parse(path.read_text(), str(path))
        plain = {"HTTPServer", "ThreadingHTTPServer"}
        plain |= {
            alias.asname
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module == "http.server"
            for alias in node.names
            if alias.name in plain and alias.asname
        }
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and node.name != "LookupFreeHTTPServer":
                refs = node.bases
            elif isinstance(node, ast.Call):
                refs = [node.func]
            else:
                continue
            for ref in refs:
                name = ref.attr if isinstance(ref, ast.Attribute) else getattr(ref, "id", "")
                if name in plain:
                    found.append(f"{path.relative_to(SRC)}:{node.lineno}")
    assert found == []
