"""m66 task-server and pilot paths the frozen suite does not drive: a cancelled queued task, an idle long poll,
a stale or unusable result, close with held tasks, an exhausted port range, a malformed signature header,
pilots sharing a hostname and pid, and a pilot orphaned by its driver, idle or mid-task."""

from __future__ import annotations

import os
import pickle
import signal
import socket
import subprocess
import sys
import threading
import time
from concurrent.futures import CancelledError, Future
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from graphed_executors.htcondor_backend import HTCondorBackend, LocalPilots, server
from graphed_executors.htcondor_backend.launch import write_secret
from graphed_executors.parsl_backend.backend import _ParslFuture

PORTS = (10000, 10100)
HERE = str(Path(__file__).resolve().parent)


def double(x: int) -> int:
    return 2 * x


def raise_unpicklable() -> None:
    raise ValueError(threading.Lock())


def task_server() -> server.TaskServer:
    return server.TaskServer("127.0.0.1", PORTS, SimpleNamespace(alive=lambda: 1))


def test_a_cancelled_queued_task_is_skipped_and_fails_its_dependent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(server, "POLL_S", 0.2)
    ts = task_server()
    try:
        first: Future[Any] = Future()
        ts.add("k1", double, (1,), first)
        second: Future[Any] = Future()
        ts.add("k2", double, (_ParslFuture(first, {}),), second)
        assert first.cancel()
        assert ts.lease("pilot-a") is None  # the cancelled task is dropped, then the poll times out
        assert isinstance(second.exception(0), CancelledError)
    finally:
        ts.close()
        ts.shutdown()


def test_stale_and_unusable_results_and_close(monkeypatch: pytest.MonkeyPatch) -> None:
    ts = task_server()
    try:
        done: Future[Any] = Future()
        held: Future[Any] = Future()
        ts.add("k1", double, (1,), done)
        ts.add("k2", double, (2,), held)
        leased = ts.lease("pilot-a")
        assert leased is not None
        ts.result("pilot-b", leased[0], True, pickle.dumps(2))
        assert not done.done(), "a result from a pilot without the lease was accepted"
        ts.result("pilot-a", leased[0], True, b"not a pickle")
        assert "does not unpickle" in str(done.exception(0))
        ts.close()
        assert "unfinished" in str(held.exception(0))
        assert ts.lease("pilot-a") is None
    finally:
        ts.close()
        ts.shutdown()


def test_an_exhausted_port_range_is_an_error() -> None:
    ts = task_server()
    try:
        port = int(ts.url.rsplit(":", 1)[1])
        with pytest.raises(OSError, match="no free port"):
            server.TaskServer("127.0.0.1", (port, port), SimpleNamespace(alive=lambda: 1))
    finally:
        ts.close()
        ts.shutdown()


KILLED = (-signal.SIGTERM, signal.SIGTERM)  # POSIX reports the signal negated; Windows as the exit code


def sleep_for(seconds: float, marker: str) -> None:
    Path(marker).touch()
    time.sleep(seconds)


def die_once(marker: str) -> str:
    if not os.path.exists(marker):
        Path(marker).touch()
        os._exit(1)
    return "survived"


def start_pilot(url: str, secret: Path, prelude: str = "") -> subprocess.Popen[str]:
    """A pilot on this interpreter with this directory importable; ``prelude`` runs first."""
    code = f"{prelude}\nimport sys\nfrom graphed_executors.htcondor_backend import pilot\nsys.exit(pilot.main(sys.argv[1:]))"
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([HERE, os.environ.get("PYTHONPATH", "")])}
    return subprocess.Popen(
        [sys.executable, "-c", code, url, str(secret)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
    )


def wait_until(predicate: Any, timeout_s: float = 60.0) -> None:
    deadline = time.monotonic() + timeout_s
    while not predicate() and time.monotonic() < deadline:
        time.sleep(0.05)


@pytest.mark.parametrize("busy", [False, True], ids=["idle", "mid-task"])
def test_a_pilot_whose_driver_is_gone_exits_at_once(
    busy: bool, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(server, "LEASE_S", 1.0)
    monkeypatch.setattr(server, "POLL_S", 0.2)
    ts = task_server()
    secret = tmp_path / "graphed-secret"
    write_secret(secret, ts.secret)
    started = tmp_path / "started"
    task_s = 5 * server.LEASE_S + 5.0
    if busy:
        ts.add("k", sleep_for, (task_s, str(started)), Future())
    pilot = start_pilot(ts.url, secret)
    wait_until(started.exists if busy else lambda: ts.live_pilots() == 1)
    assert started.exists() if busy else ts.live_pilots() == 1
    ts.shutdown()  # the socket goes away without a 410: the pilot cannot tell a crash from a partition
    gone_at = time.monotonic()
    out, _ = pilot.communicate(timeout=60)
    ts.close()
    assert pilot.returncode in KILLED, out
    assert "unreachable" in out, out
    assert time.monotonic() - gone_at < task_s - 1.0, "the pilot waited for its task to end"


def test_pilots_sharing_a_hostname_and_pid_stay_distinct(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(server, "LEASE_S", 1.0)
    ts = task_server()
    secret = tmp_path / "graphed-secret"
    write_secret(secret, ts.secret)
    same = "import os, socket; os.getpid = lambda: 14; socket.gethostname = lambda: 'pilot-box'"
    pilots = [start_pilot(ts.url, secret, same) for _ in range(2)]
    try:
        wait_until(lambda: ts.live_pilots() == 2)
        assert ts.live_pilots() == 2
        marker = tmp_path / "died"
        fut: Future[Any] = Future()
        ts.add("k", die_once, (str(marker),), fut)
        assert fut.result(timeout=60)[0] == "survived"  # the (result, events) envelope
        assert marker.exists(), "the task never killed a pilot: the scenario did not run"
    finally:
        ts.close()
        for p in pilots:
            p.communicate(timeout=60)
        ts.shutdown()


def test_a_malformed_signature_header_is_refused(capfd: pytest.CaptureFixture[str]) -> None:
    ts = task_server()
    try:
        host, port = ts.url.removeprefix("http://").split(":")
        with socket.create_connection((host, int(port)), timeout=30) as conn:
            conn.sendall(
                b"POST /hello HTTP/1.1\r\nHost: x\r\nX-Graphed-Sig: \xe9\r\nContent-Length: 0\r\n\r\n"
            )
            # to EOF: closing early resets the server's write
            reply = b"".join(iter(lambda: conn.recv(4096), b""))
    finally:
        ts.close()
        ts.shutdown()
    assert reply.startswith(b"HTTP/1.0 403"), reply
    assert "Traceback" not in capfd.readouterr().err


def test_an_unpicklable_task_error_still_settles_the_future(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(server, "POLL_S", 0.2)  # the idle pilot also sees 204s
    backend = HTCondorBackend(LocalPilots(pythonpath=[Path(__file__).parent]), 1, host="127.0.0.1")
    try:
        backend.wait_for_pilots(1, timeout=60)
        time.sleep(0.5)
        err = backend.submit(raise_unpicklable, key="graphed-m66-unpicklable").exception(timeout=60)
    finally:
        backend.close()
    assert isinstance(err, RuntimeError), err
    assert "ValueError" in str(err) and "Traceback" in str(err), err
