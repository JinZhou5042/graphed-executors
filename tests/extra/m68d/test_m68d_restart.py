"""m68d B paths the frozen suite does not reach: a watch-mode restart that finds no port it may use ends the
job as a start that never became ready (exit 3). ``announce.py`` runs in process."""

from __future__ import annotations

import json
import socket
import sys
from pathlib import Path

import pytest

from graphed_executors.htcondor_backend import announce as ann

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "frozen" / "m68d"))
from m68d_harness import free_range, write_machine_ad

posix = pytest.mark.skipif(
    sys.platform == "win32", reason="announce.py is POSIX-only: Windows delivers SIGTERM as TerminateProcess"
)
# listens on argv[1] for a second, then exits 5
SHORT_LIVED = (
    "import socket, sys, time; s = socket.create_server(('', int(sys.argv[1]))); time.sleep(1); sys.exit(5)"
)


@posix
def test_a_restart_with_no_port_left_ends_the_job_with_3(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    # a slot's job checks its child at its ad's Machine, never at an fqdn that may resolve to nothing
    monkeypatch.setattr(socket, "getfqdn", lambda name="": "unresolvable.invalid")
    monkeypatch.setenv("_CONDOR_MACHINE_AD", str(write_machine_ad(tmp_path / "machine.ad", "127.0.0.1")))
    port, _ = free_range(1)
    cfg: dict[str, object] = {
        "argv": ["{python}", "-c", SHORT_LIVED, "{port}"],
        "env": {},
        "check": "tcp",
        "ports": [port, port],
        "key": "svc0",
        "url": None,
        "watch": str(tmp_path / "dag"),  # never written: nothing is announced
        "python": sys.executable,
        "timeout_s": 20.0,
        "lease_s": 30.0,
        "beat_s": 1.0,
    }
    (tmp_path / "service.json").write_text(json.dumps(cfg))
    monkeypatch.setattr(sys, "argv", ["announce.py", "service.json"])
    assert ann.serve() == 3
    out = capsys.readouterr().out
    assert "the child exited 5" in out, out
    assert (
        f"port {port} served an earlier child, next" in out
        and f"not ready: no free port in {port}-{port}" in out
    )
