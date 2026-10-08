"""m68d port-race decisions the frozen suite reaches only on Linux: a driver-hosted start and ``announce.py``'s
start refuse a port whose listener the child's process tree does not hold. The ``/proc`` probe answers here
are stand-ins (the probe itself is the frozen suite's), so these run on every OS."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest
from graphed.services import Launch, ServiceSpec

from graphed_executors.htcondor_backend import announce as ann
from graphed_executors.submit import ThreadBackend
from graphed_executors.submit import services as svc

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "frozen" / "m68d"))
from m68d_harness import free_range

posix = pytest.mark.skipif(sys.platform == "win32", reason="announce.py starts its child with a preexec_fn")
LISTEN = "import socket, sys, time; s = socket.create_server(('', int(sys.argv[1]))); time.sleep(60)"


def owner(held: bool, asked: list[tuple[int, set[str]]]) -> Any:
    def held_by(pid: int, inodes: set[str], proc: str = "/proc") -> bool:
        asked.append((pid, set(inodes)))
        return held

    return held_by


@pytest.mark.parametrize("held", [False, True])
def test_a_driver_hosted_start_takes_its_port_only_when_its_child_holds_the_listener(
    held: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    asked: list[tuple[int, set[str]]] = []
    monkeypatch.setattr(svc, "listeners", lambda port, proc="/proc": {"4242"})
    monkeypatch.setattr(svc, "held_by", owner(held, asked))
    low, high = free_range(3)
    launch = Launch(argv=("{python}", "-c", LISTEN, "{port}"))
    spec = ServiceSpec("web", "tcp", check="tcp", ports=(low, high), launch=launch, timeout_s=20.0)
    backend = ThreadBackend(1)
    services = svc.ServiceSet((spec,), backend)
    try:
        if held:
            assert dict(services.start()) == {"web": f"tcp://127.0.0.1:{low}"}
        else:
            with pytest.raises(svc.ServiceUnavailable) as info:
                services.start()
            assert info.value.legs["managed"] == f"port {low} on 127.0.0.1 is held by another process"
    finally:
        services.close()
        backend.close()
    assert asked and all(inodes == {"4242"} for _pid, inodes in asked), asked


@posix
def test_announce_moves_past_a_port_whose_listener_its_child_does_not_hold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "service").mkdir()
    low, high = free_range(3)
    asked: list[tuple[int, set[str]]] = []
    monkeypatch.setattr(ann, "listeners", lambda port, proc="/proc": {"4242"} if port == low else set())
    monkeypatch.setattr(ann, "held_by", owner(False, asked))
    cfg = {"argv": ["{python}", "-c", LISTEN, "{port}"], "env": {}, "check": "tcp", "ports": [low, high]}
    started = ann.start({**cfg, "python": sys.executable, "timeout_s": 20.0}, "127.0.0.1")
    assert not isinstance(started, str), started
    child, port = started
    ann.reap(child)
    assert port == low + 1
    assert [inodes for _pid, inodes in asked] == [{"4242"}], asked
    assert f"port {low} is held by another process, next" in capsys.readouterr().out


def bound_during_the_dial(module: Any, check: str, monkeypatch: pytest.MonkeyPatch, port: int) -> list[bool]:
    """``module``'s ``check`` passes, and ``port`` gains a listener nobody's tree holds as the first dial runs."""
    bound: list[bool] = []

    def dial(*args: Any, **kwargs: Any) -> None:
        bound.append(True)

    monkeypatch.setattr(module, check, dial)
    monkeypatch.setattr(
        module, "listeners", lambda p, proc="/proc": {"4242"} if bound and p == port else set()
    )
    monkeypatch.setattr(module, "held_by", owner(False, []))
    return bound


def test_a_driver_hosted_start_judges_the_listener_its_dial_reached(monkeypatch: pytest.MonkeyPatch) -> None:
    low, high = free_range(3)
    bound = bound_during_the_dial(svc, "check_ready", monkeypatch, low)
    launch = Launch(argv=("{python}", "-c", LISTEN, "{port}"))
    spec = ServiceSpec("web", "tcp", check="tcp", ports=(low, high), launch=launch, timeout_s=20.0)
    backend = ThreadBackend(1)
    services = svc.ServiceSet((spec,), backend)
    try:
        with pytest.raises(svc.ServiceUnavailable) as info:
            services.start()
    finally:
        services.close()
        backend.close()
    assert bound and info.value.legs["managed"] == f"port {low} on 127.0.0.1 is held by another process"


@posix
def test_announce_judges_the_listener_its_dial_reached(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "service").mkdir()
    low, high = free_range(3)
    bound = bound_during_the_dial(ann, "self_check", monkeypatch, low)
    cfg = {"argv": ["{python}", "-c", LISTEN, "{port}"], "env": {}, "check": "tcp", "ports": [low, high]}
    started = ann.start({**cfg, "python": sys.executable, "timeout_s": 20.0}, "127.0.0.1")
    assert not isinstance(started, str), started
    child, port = started
    ann.reap(child)
    assert bound and port == low + 1
    assert f"port {low} is held by another process, next" in capsys.readouterr().out
