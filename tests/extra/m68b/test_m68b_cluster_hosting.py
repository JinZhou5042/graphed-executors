"""m68b B1 paths the frozen suite does not reach: ``announce.py``'s identity fallback, an exhausted port
range, a ``service/`` mount point, a port another process took after the scan, the watched pair while it
is missing or torn, a child that exits after it was ready; ``ServiceJob``'s refusals of a job it cannot
build, and a ``stop()`` before any submit. No bindings and no pool; ``announce.py`` runs in process."""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from graphed.services import Launch, ServiceSpec

from graphed_executors.htcondor_backend import SITES, CondorPilots
from graphed_executors.htcondor_backend import announce as ann
from graphed_executors.htcondor_backend.services import ServiceJob

posix = pytest.mark.skipif(sys.platform == "win32", reason="announce.py starts its child with a preexec_fn")

# a child that listens on its port (argv[1]) and exits with argv[2] after argv[3] seconds, or at once on
# port argv[4]
LISTENER = (
    "import socket, sys, time\n"
    "port, code, after, quit_on = int(sys.argv[1]), int(sys.argv[2]), float(sys.argv[3]), int(sys.argv[4])\n"
    "if port == quit_on:\n"
    "    sys.exit(code)\n"
    "s = socket.socket()\n"
    "s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)\n"
    "s.bind(('', port))\n"
    "s.listen(8)\n"
    "time.sleep(after)\n"
    "sys.exit(code)\n"
)


def free_ports(n: int) -> list[int]:
    """``n`` consecutive ports ``free()`` accepts, scanned from a per-process base so concurrent runs differ."""
    for low in range(31000 + os.getpid() % 1000 * 20, 60000, 97):
        ports = list(range(low, low + n))
        if all(ann.free(p) for p in ports):
            return ports
    raise AssertionError("no free ports")


@contextmanager
def listeners(ports: list[int]) -> Iterator[None]:
    with ExitStack() as stack:
        for port in ports:
            sock = stack.enter_context(socket.socket())
            # bound as free() binds: a TIME_WAIT left on the port does not refuse it
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("", port))
            sock.listen(1)
        yield


@pytest.fixture
def job_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """A job dir as the cwd on a machine named ``localhost``, and ``CHILD`` restored (and any child it
    names reaped) afterwards."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "service").mkdir()
    (tmp_path / "machine.ad").write_text('Machine = "localhost"\n')
    monkeypatch.setenv("_CONDOR_MACHINE_AD", str(tmp_path / "machine.ad"))
    saved = ann.CHILD[0]
    try:
        yield tmp_path
    finally:
        child, ann.CHILD[0] = ann.CHILD[0], saved
        if child is not None and child is not saved:
            ann.reap(child)


def config(ports: list[int], **fields: Any) -> dict[str, Any]:
    return {
        "argv": [],
        "env": {},
        "check": "tcp",
        "ports": [ports[0], ports[-1]],
        "key": "k",
        "url": None,
        "watch": None,
        "python": sys.executable,
        "timeout_s": 30.0,
        "lease_s": 60.0,
        "beat_s": 1.0,
        **fields,
    }


# ---- announce.py ----------------------------------------------------------------------------------------


def test_identity_is_the_machine_ad_s_else_the_fqdn(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(socket, "getfqdn", lambda: "fqdn.example")
    monkeypatch.delenv("_CONDOR_MACHINE_AD", raising=False)
    assert ann.host_identity() == "fqdn.example"
    ad = tmp_path / "machine.ad"
    ad.write_text('Name = "slot1@wn"\nCpus = 2\n')
    monkeypatch.setenv("_CONDOR_MACHINE_AD", str(ad))
    assert ann.host_identity() == "fqdn.example"
    ad.write_text('Name = "slot1@wn"\nMachine = "wn.example"\n')
    assert ann.host_identity() == "wn.example"


@posix
def test_an_exhausted_range_names_it_and_starts_nothing(job_dir: Path) -> None:
    ports = free_ports(2)
    marker = job_dir / "started"
    cfg = config(ports, argv=["{python}", "-c", f"open({str(marker)!r}, 'w').close()"])
    with listeners(ports):
        assert ann.start(cfg, "localhost") == f"no free port in {ports[0]}-{ports[1]}"
    assert not marker.exists()


def test_a_mount_point_service_dir_exits_3_before_any_start(
    job_dir: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    (job_dir / "service.json").write_text(json.dumps(config([1, 1])))
    (job_dir / "graphed-secret").write_text(os.urandom(32).hex())
    monkeypatch.setattr(sys, "argv", ["announce.py", "service.json"])
    monkeypatch.setattr(os.path, "ismount", lambda path: path == "service")
    monkeypatch.setattr(ann, "start", lambda *a: pytest.fail("a child was started"))
    assert ann.serve() == 3
    assert "service is a mount point" in capsys.readouterr().out


@posix
def test_a_port_taken_after_the_scan_moves_on_to_the_next(
    job_dir: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    low, high = free_ports(2)
    real_free, asked = ann.free, {low: 0}

    def free(port: int) -> bool:
        if port != low:
            return real_free(port)
        asked[low] += 1  # the scan finds it free; after the child exits, another process holds it
        return asked[low] == 1

    monkeypatch.setattr(ann, "free", free)
    cfg = config([low, high], argv=["{python}", "-c", LISTENER, "{port}", "0", "60", str(low)])
    started = ann.start(cfg, "localhost")
    assert not isinstance(started, str), started
    child, port = started
    assert port == high and child.poll() is None
    assert f"port {low} taken after the scan (the child exited 0), next" in capsys.readouterr().out
    assert asked[low] == 2


def test_the_watched_pair_is_read_only_when_whole(tmp_path: Path) -> None:
    assert ann._watched(str(tmp_path)) is None
    (tmp_path / "driver.url").write_text("http://login.example:10000\n")
    assert ann._watched(str(tmp_path)) is None
    (tmp_path / "graphed-secret").write_text("abc")  # torn: not whole hex
    assert ann._watched(str(tmp_path)) is None
    (tmp_path / "graphed-secret").write_text("00ff\n")
    assert ann._watched(str(tmp_path)) == ("http://login.example:10000", b"\x00\xff")
    (tmp_path / "driver.url").write_text("")
    assert ann._watched(str(tmp_path)) is None


@posix
def test_watch_mode_retries_a_refused_pair_and_stops_once_it_answers_200(
    job_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ports = free_ports(1)
    pair = ("http://login.example:10000", b"\x01" * 32)
    reads = iter([None])  # nothing written yet, then the same pair on every read
    monkeypatch.setattr(ann, "_watched", lambda watch: next(reads, pair))
    answers, posted = iter([403]), []

    def post(url: str, secret: bytes, body: bytes) -> int:
        posted.append((url, secret, body))
        return next(answers, 200)

    monkeypatch.setattr(ann, "post", post)
    cfg = config(ports, argv=["{python}", "-c", LISTENER, "{port}", "7", "5", "0"], key="svc0", watch="dag")
    (job_dir / "service.json").write_text(json.dumps(cfg))
    monkeypatch.setattr(sys, "argv", ["announce.py", "service.json"])
    assert ann.serve() == 7
    body = f"svc0 localhost:{ports[0]} localhost".encode()
    assert posted == [(*pair, body), (*pair, body)], posted


@posix
def test_the_pid_reap_of_a_child_reaped_elsewhere_returns_at_once() -> None:
    child = subprocess.Popen([sys.executable, "-c", "pass"])
    os.waitpid(child.pid, 0)  # reaped behind Popen's back: its returncode is still unset
    started = time.monotonic()
    ann.hard_reap(child.pid)
    assert time.monotonic() - started < 1.0


@posix
def test_a_child_that_exits_after_ready_ends_the_job_with_its_code(
    job_dir: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    ports = free_ports(1)
    with socket.socket() as s:  # a url nothing answers: no 200 and no 403, well inside the lease
        s.bind(("127.0.0.1", 0))
        url = f"http://127.0.0.1:{s.getsockname()[1]}"
    cfg = config(ports, argv=["{python}", "-c", LISTENER, "{port}", "5", "1.5", "0"], url=url)
    (job_dir / "service.json").write_text(json.dumps(cfg))
    (job_dir / "graphed-secret").write_text(os.urandom(32).hex())
    monkeypatch.setattr(sys, "argv", ["announce.py", "service.json"])
    assert ann.serve() == 5
    out = capsys.readouterr().out
    assert "ready pid=" in out and "the child exited 5" in out and "orphaned" not in out


# ---- ServiceJob -------------------------------------------------------------------------------------------


def web(**launch: Any) -> ServiceSpec:
    return ServiceSpec("web", "http", check="http:/", launch=Launch(argv=("{python}",), **launch))


def test_a_service_job_it_cannot_build_is_refused(tmp_path: Path) -> None:
    pilots = CondorPilots("generic", log_dir=tmp_path)
    with pytest.raises(ValueError, match="no launch recipe"):
        ServiceJob(ServiceSpec("ext", "http"), pilots, key="k", url="http://h:1", secret=b"s")
    for kwargs in ({}, {"url": "http://h:1"}, {"url": "http://h:1", "secret": b"s", "watch": str(tmp_path)}):
        with pytest.raises(ValueError, match="either to url with secret"):
            ServiceJob(web(), pilots, key="k", **kwargs)
    no_workers = CondorPilots(replace(SITES["generic"], worker_ports=None), log_dir=tmp_path)
    with pytest.raises(ValueError, match="no worker_ports"):
        ServiceJob(web(), no_workers, key="k", url="http://h:1", secret=b"s")
    assert ServiceJob(web(), pilots, key="k", watch=str(tmp_path)).watch == str(tmp_path)


def test_a_submit_through_an_unstarted_launcher_is_refused_before_any_file(tmp_path: Path) -> None:
    job = ServiceJob(web(), CondorPilots("generic", log_dir=tmp_path), key="k", url="http://h:1", secret=b"s")
    with pytest.raises(AssertionError, match="start the launcher"):
        job.submit()
    assert job.dir is None and list(tmp_path.iterdir()) == []


def test_stop_before_any_submit_touches_nothing(tmp_path: Path) -> None:
    job = ServiceJob(web(), CondorPilots("generic", log_dir=tmp_path), key="k", url="http://h:1", secret=b"s")
    job.stop()
    assert job.dir is None and job.cluster is None and list(tmp_path.iterdir()) == []
