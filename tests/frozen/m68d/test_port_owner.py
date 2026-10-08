"""m68d port race (plan §2 "Port race — a port counts only when the child holds its listener"), rows
P1-P4 of §4. P1-P3 replay P1d on Linux: the child starts on a scanned port, another process binds that
port, and only then does the child try to bind it (``m68d_child.py gated``). P4 reads a fixture proc tree
on every OS."""

from __future__ import annotations

import os
import shlex
import sys
import tempfile
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest
from graphed.services import Launch, ServiceSpec
from m68d_harness import (
    CHILD_FILE,
    SERVICE,
    Web,
    announce_api,
    child_starts,
    end_child,
    free_range,
    raised_by,
    run_bounded,
    services_api,
    wait_for,
)

from graphed_executors.submit.threadpool import ThreadBackend

linux = pytest.mark.skipif(sys.platform != "linux", reason="the race is replayed against /proc (Linux)")


@contextmanager
def foreign_after_start(report: Path, gate: Path, status: int) -> Iterator[list[Web]]:
    """Once the child's first start is recorded, a server answering ``status`` binds that port on every
    interface; then ``gate`` opens. Yields the list the server lands in."""
    bound: list[Web] = []

    def bind() -> None:
        if wait_for(lambda: bool(child_starts(report)), 60.0):
            bound.append(Web(child_starts(report)[0][1], host="", status=status))
        gate.touch()

    thread = threading.Thread(target=bind, daemon=True)
    thread.start()
    try:
        yield bound
    finally:
        thread.join(70.0)
        for web in bound:
            web.close()


def announce_cfg(argv: list[str], check: str, ports: tuple[int, int]) -> dict[str, Any]:
    return {"argv": argv, "env": {}, "check": check, "ports": list(ports), "python": sys.executable, "timeout_s": 8.0}


def listener_pids(port: int) -> set[int]:
    """The pids whose open fds include a LISTEN socket on ``port`` (read from /proc)."""
    inodes = set()
    for name in ("/proc/net/tcp", "/proc/net/tcp6"):
        with open(name) as f:
            for row in f.read().splitlines()[1:]:
                col = row.split()
                if col[3] == "0A" and int(col[1].rsplit(":", 1)[1], 16) == port:
                    inodes.add(f"socket:[{col[9]}]")
    pids = set()
    for entry in os.listdir("/proc"):
        if entry.isdigit():
            try:
                links = {os.readlink(f"/proc/{entry}/fd/{fd}") for fd in os.listdir(f"/proc/{entry}/fd")}
            except OSError:
                continue
            if links & inodes:
                pids.add(int(entry))
    return pids


# ---- P1: announce.py moves past a port another process took after the scan -------------------------------


@linux
@pytest.mark.parametrize("check", ["http:/", "tcp"])
def test_a_port_another_process_takes_after_the_scan_is_skipped(
    check: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "service").mkdir()
    report, gate = tmp_path / "child", tmp_path / "gate"
    low, high = free_range(3)
    cfg = announce_cfg(["{python}", CHILD_FILE, "gated", "{port}", str(report), str(gate)], check, (low, high))
    announce = announce_api()
    with foreign_after_start(report, gate, 501) as foreign:
        got = run_bounded(lambda: announce.start(cfg, "127.0.0.1"), 60.0)
        if not isinstance(got, str):
            announce.reap(got[0])
    log = capsys.readouterr().out
    assert foreign and foreign[0].port == low, "the foreign listener never bound the scanned port"
    assert not isinstance(got, str), f"not ready: {got}\n{log}"
    assert got[1] == low + 1, f"ready on {got[1]}; the foreign listener holds {low}\n{log}"
    assert f"port {low} is held by another process" in log, log


# ---- P2: a listener a grandchild holds is the child's own ------------------------------------------------------


@linux
def test_a_child_that_listens_through_a_shell_is_its_own_listener(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "service").mkdir()
    report = tmp_path / "child"
    low, high = free_range(3)
    script = f"{{python}} {shlex.quote(CHILD_FILE)} serve {{port}} {shlex.quote(str(report))}; exit $?"
    cfg = announce_cfg(["sh", "-c", script], "http:/", (low, high))
    announce = announce_api()
    calls: list[tuple[int, Any, bool]] = []
    held_by = announce.held_by

    def spy(pid: int, inodes: Any, *args: Any, **kwargs: Any) -> bool:
        held = bool(held_by(pid, inodes, *args, **kwargs))
        calls.append((pid, set(inodes), held))
        return held

    monkeypatch.setattr(announce, "held_by", spy)
    got = run_bounded(lambda: announce.start(cfg, "127.0.0.1"), 60.0)
    try:
        assert not isinstance(got, str), got
        child, port = got
        holders = listener_pids(port)
        assert port == low, f"ready on {port}, not the first port {low}"
        assert holders and child.pid not in holders, f"listener held by {holders}, the child is {child.pid}"
        assert any(pid == child.pid and held for pid, _inodes, held in calls), calls
    finally:
        if not isinstance(got, str):
            announce.reap(got[0])
        for pid, _port in child_starts(report):
            end_child(report, pid, 0)  # the shell's python outlives a reap of the shell


# ---- P3: a driver-hosted service refuses a port another process holds ----------------------------------------


@linux
def test_a_driver_hosted_service_refuses_a_port_held_by_another_process(tmp_path: Path) -> None:
    report, gate = tmp_path / "child", tmp_path / "gate"
    low, high = free_range(3)
    argv = ("{python}", CHILD_FILE, "gated", "{port}", str(report), str(gate))
    spec = ServiceSpec(SERVICE, "http", check="http:/", ports=(low, high), launch=Launch(argv=argv), timeout_s=10.0)
    backend = ThreadBackend(1)
    service_set = services_api().ServiceSet((spec,), backend)
    try:
        with foreign_after_start(report, gate, 200) as foreign:
            err = raised_by(service_set.start, 60.0)
    finally:
        run_bounded(service_set.close, 60.0)
        backend.close()
    assert foreign and foreign[0].port == low, "the foreign listener never bound the scanned port"
    assert isinstance(err, services_api().ServiceUnavailable), repr(err)
    managed = err.legs.get("managed", "")
    assert f"port {low} " in managed and "held by another process" in managed, err.legs


# ---- P4: the ownership helpers over a fixture proc tree -------------------------------------------------------

PORT = 0x5BA1
HEADER = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode"
TAIL = "1 0000000000000000 100 0 0 10 0"


def tcp_row(n: int, local: str, remote: str, state: str, inode: int) -> str:
    return f"   {n}: {local} {remote} {state} 00000000:00000000 00:00000000 00000000  1000        0 {inode} {TAIL}"


def can_symlink() -> bool:
    with tempfile.TemporaryDirectory() as d:
        try:
            os.symlink("socket:[1]", os.path.join(d, "link"))
        except (OSError, NotImplementedError):
            return False
        return True


def proc_tree(root: Path, links: bool) -> Path:
    """Listeners on ``PORT``: inode 7101 (tcp) and 7104 (tcp6); 7102 is an established connection on it
    and 7103 a listener on the next port. Pids: 1 ← 100 (the child) ← 200 ← 300 (holds 7101); 1 ← 900
    (holds 7104); 400's stat and 500's fd (a child of 100) cannot be read."""
    any4, any6 = "00000000", "0" * 32
    (root / "net").mkdir(parents=True)
    (root / "net" / "tcp").write_text(
        "\n".join(
            [
                HEADER,
                tcp_row(0, f"{any4}:{PORT:04X}", f"{any4}:0000", "0A", 7101),
                tcp_row(1, f"0100007F:{PORT:04X}", "0100007F:D431", "01", 7102),
                tcp_row(2, f"{any4}:{PORT + 1:04X}", f"{any4}:0000", "0A", 7103),
            ]
        )
        + "\n"
    )
    (root / "net" / "tcp6").write_text(
        "\n".join([HEADER, tcp_row(0, f"{any6}:{PORT:04X}", f"{any6}:0000", "0A", 7104)]) + "\n"
    )
    tree = {1: (0, None), 100: (1, None), 200: (100, None), 300: (200, 7101), 900: (1, 7104), 500: (100, None)}
    for pid, (ppid, inode) in tree.items():
        (root / str(pid)).mkdir()
        comm = "python3 child" if pid == 200 else f"p{pid}"
        (root / str(pid) / "stat").write_text(f"{pid} ({comm}) S {ppid} {pid} {pid} 0 -1 4194560 0 0 0\n")
        if pid == 500:
            (root / str(pid) / "fd").write_text("not a directory")
            continue
        (root / str(pid) / "fd").mkdir()
        for fd, target in ((0, "/dev/null"), (3, None if inode is None else f"socket:[{inode}]")):
            if target is None:
                continue
            path = root / str(pid) / "fd" / str(fd)
            if links:
                os.symlink(target, path)
            else:
                path.write_text(target)
    (root / "400").mkdir()
    (root / "400" / "stat").mkdir()
    (root / "self").mkdir()
    return root


@pytest.mark.parametrize("where", ["services", "announce"])
def test_listener_ownership_reads_a_proc_tree(where: str, tmp_path: Path) -> None:
    module = services_api() if where == "services" else announce_api()
    links = can_symlink()
    proc = str(proc_tree(tmp_path / "proc", links))
    assert module.listeners(PORT, proc=proc) == {"7101", "7104"}
    assert module.listeners(PORT, proc=str(tmp_path / "absent")) == set()
    assert module.held_by(100, {"7104"}, proc=proc) is False
    assert module.held_by(100, {"7101", "7104"}, proc=proc) is False
    if links:
        assert module.held_by(100, {"7101"}, proc=proc) is True
        assert module.held_by(900, {"7104"}, proc=proc) is True
