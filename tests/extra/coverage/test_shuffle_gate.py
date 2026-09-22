"""Coverage-policy rollout (ci/per-file-coverage-policy): closes `local/shuffle.py` toward the
per-file >=90% gate (the node-local block/object HTTP handlers + the routable store-child launcher).
Not frozen; may be extended or replaced by later coverage work."""

from __future__ import annotations

import os
import queue as queue_mod
import tempfile
import threading
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

import pytest

import graphed_executors.local.shuffle as shuffle_mod
from graphed_executors.local.shuffle import _make_block_handler, _make_cluster, _store_server_handler


def test_make_cluster_rejects_an_unknown_comms_kind() -> None:
    with pytest.raises(ValueError, match="unknown comms"):
        _make_cluster("carrier-pigeon", 1, None, None)


def test_block_handler_returns_404_for_an_unknown_digest() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_block_handler({}))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = str(server.server_address[0]), server.server_address[1]
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(f"http://{host}:{port}/block/nope", timeout=5)
        assert exc_info.value.code == 404
    finally:
        server.shutdown()
        server.server_close()


def test_store_handler_serves_put_then_get_and_404s_an_unknown_hash(tmp_path: Path) -> None:
    handler_cls = _store_server_handler(tmp_path, served_pid=4242, served_host="127.0.0.1")
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = str(server.server_address[0]), server.server_address[1]
    try:
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(f"http://{host}:{port}/blob/nope", timeout=5)
        assert exc_info.value.code == 404

        req = urllib.request.Request(f"http://{host}:{port}/blob", data=b"payload", method="PUT")
        with urllib.request.urlopen(req, timeout=5) as resp:
            digest = resp.read().decode()
        assert digest and (tmp_path / digest).read_bytes() == b"payload"

        with urllib.request.urlopen(f"http://{host}:{port}/blob/{digest}", timeout=5) as resp:
            assert resp.read() == b"payload"
            assert resp.headers["X-Served-Pid"] == "4242"
            assert resp.headers["X-Served-Host"] == "127.0.0.1"
    finally:
        server.shutdown()
        server.server_close()


def test_routable_store_child_serves_over_a_real_socket_and_announces_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[ThreadingHTTPServer] = []

    class _CapturingServer(ThreadingHTTPServer):
        def __init__(self, *a: object, **k: object) -> None:
            super().__init__(*a, **k)  # type: ignore[arg-type]
            captured.append(self)

    monkeypatch.setattr(shuffle_mod, "ThreadingHTTPServer", _CapturingServer)
    # Fix the advertise host instead of letting the child auto-detect one: detection dials the
    # runner's real network (select_advertise_host(None)) and has no bound on a sandboxed/offline
    # CI host, which timed out the ready_q.get() below on macOS runners.
    monkeypatch.setattr(shuffle_mod, "select_advertise_host", lambda _host: "127.0.0.1")
    ready_q: queue_mod.Queue[tuple[int, str, int, int]] = queue_mod.Queue()
    errors: list[BaseException] = []

    def _run(tmp: str) -> None:
        try:
            # `advertise_host` is annotated `str` but select_advertise_host (monkeypatched above)
            # accepts None for auto-detect; real callers always pass a concrete host.
            shuffle_mod._routable_store_child(0, tmp, None, ready_q)  # type: ignore[arg-type]
        except BaseException as exc:  # surfaced by the test instead of a silent hang on ready_q
            errors.append(exc)

    with tempfile.TemporaryDirectory() as tmp:
        thread = threading.Thread(target=_run, args=(tmp,), daemon=True)
        thread.start()
        try:
            try:
                node_id, host, port, pid = ready_q.get(timeout=10)
            except queue_mod.Empty:
                thread.join(timeout=1)
                if errors:
                    raise errors[0] from None
                pytest.fail(
                    f"_routable_store_child never announced readiness (thread alive={thread.is_alive()})"
                )
            assert (node_id, pid) == (0, os.getpid())  # threaded (not spawned), same PID
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(f"http://{host}:{port}/blob/nope", timeout=5)
            assert exc_info.value.code == 404
        finally:
            captured[0].shutdown()
            captured[0].server_close()
