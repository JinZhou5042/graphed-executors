"""A pool worker exits once the driver process that spawned it dies."""

from __future__ import annotations

import multiprocessing
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from graphed_executors.local import _pinned_pool

HERE = Path(__file__).parent


def _alive(pid: int) -> bool:
    if sys.platform == "win32":  # os.kill(pid, 0) would terminate the process there
        listing = subprocess.run(["tasklist", "/FI", f"PID eq {pid}", "/NH"], capture_output=True, text=True)
        return str(pid) in listing.stdout.split()
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


DRIVER = """
import sys
from _release_payloads import pid_plan
from graphed_executors.local import PinnedPoolExecutor, ProcessPoolExecutor
route = sys.argv[2]
if route == "pinned":
    ex = PinnedPoolExecutor(max_workers=2, steal=False, monitor=None)
else:
    ex = ProcessPoolExecutor(max_workers=2, steal=False, monitor=None, comms=None if route == "hub" else route)
ex.run(pid_plan(sys.argv[1], (200, 3000)))
"""


@pytest.mark.parametrize("route", ["ipc", "http", "pinned", "hub"])
def test_workers_of_a_killed_driver_leave(route: str, tmp_path: Path) -> None:
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(HERE), os.environ.get("PYTHONPATH", "")])}
    driver = subprocess.Popen([sys.executable, "-c", DRIVER, str(tmp_path), route], env=env)
    pids: list[int] = []
    try:
        deadline = time.monotonic() + 60.0
        while len(pids) < 2 and time.monotonic() < deadline:
            pids = [int(p.read_text()) for p in tmp_path.glob("*.pid") if p.read_text()]
            time.sleep(0.05)
        assert len(set(pids)) == 2, pids
        time.sleep(1.0)  # leaf 0 is done: its worker waits for the driver
        assert driver.poll() is None
        driver.kill()
        driver.wait()
        deadline = time.monotonic() + 15.0
        while any(_alive(p) for p in pids) and time.monotonic() < deadline:
            time.sleep(0.1)
        assert not [p for p in pids if _alive(p)], "a worker outlived its driver"
    finally:
        driver.kill()
        for p in pids:
            if _alive(p):
                os.kill(p, signal.SIGTERM)


def test_exit_with_driver_exits_once_the_parent_is_gone(monkeypatch: pytest.MonkeyPatch) -> None:
    exited = threading.Event()
    codes: list[int] = []

    class _GoneParent:
        def join(self) -> None:
            return None

    def fake_exit(code: int) -> None:
        codes.append(code)
        exited.set()

    monkeypatch.setattr(os, "_exit", fake_exit)
    assert multiprocessing.parent_process() is None
    _pinned_pool.exit_with_driver()  # the test process has no parent to watch
    assert not exited.wait(0.2)
    monkeypatch.setattr(multiprocessing, "parent_process", lambda: _GoneParent())
    _pinned_pool.exit_with_driver()
    assert exited.wait(5)
    assert codes == [1]
