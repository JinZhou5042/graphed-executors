"""Coverage-policy rollout (ci/per-file-coverage-policy): closes `local/_transport.py` to the
per-file >=90% gate. Not frozen; may be extended or replaced by later coverage work."""

from __future__ import annotations

import multiprocessing
import socket
import threading
import time

import pytest

from graphed_executors.local import _transport as T
from graphed_executors.local._transport import HttpTransport, PipeInbox, build_transports


def test_is_routable_host_rejects_a_non_ip_string() -> None:
    assert T.is_routable_host("not-an-ip") is False


def test_detect_routable_host_prefers_the_hostname_address_when_routable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(socket, "gethostbyname", lambda _name: "10.0.0.5")
    assert T._detect_routable_host() == "10.0.0.5"


def test_detect_routable_host_falls_back_to_the_udp_probe_when_hostname_is_loopback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(socket, "gethostbyname", lambda _name: "127.0.0.1")

    class _FakeSocket:
        def __init__(self, *a: object, **k: object) -> None:
            pass

        def connect(self, addr: object) -> None:
            pass

        def getsockname(self) -> tuple[str, int]:
            return ("172.16.0.9", 0)

        def close(self) -> None:
            pass

    monkeypatch.setattr(socket, "socket", lambda *a, **k: _FakeSocket())
    assert T._detect_routable_host() == "172.16.0.9"


def test_detect_routable_host_returns_none_when_the_udp_probe_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(socket, "gethostbyname", lambda _name: "127.0.0.1")

    class _FailSocket:
        def __init__(self, *a: object, **k: object) -> None:
            pass

        def connect(self, addr: object) -> None:
            raise OSError("no route")

        def close(self) -> None:
            pass

    monkeypatch.setattr(socket, "socket", lambda *a, **k: _FailSocket())
    assert T._detect_routable_host() is None


def test_select_advertise_host_raises_when_nothing_is_detected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(T, "_detect_routable_host", lambda: None)
    with pytest.raises(ValueError, match="no routable"):
        T.select_advertise_host()


def test_select_advertise_host_returns_the_detected_host(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(T, "_detect_routable_host", lambda: "10.1.2.3")
    assert T.select_advertise_host() == "10.1.2.3"


def test_pipe_inbox_get_returns_a_ready_item() -> None:
    ctx = multiprocessing.get_context("spawn")
    inbox = PipeInbox(ctx)
    try:
        inbox.put_nowait("hello")
        assert inbox.get(timeout=5) == "hello"  # the item-ready branch, not the queue.Empty timeout
    finally:
        inbox.close()


def test_pipe_inbox_get_nowait_returns_a_ready_item() -> None:
    ctx = multiprocessing.get_context("spawn")
    inbox = PipeInbox(ctx)
    try:
        inbox.put_nowait("hello")
        time.sleep(0.05)  # let the pipe write land before the non-blocking poll
        assert inbox.get_nowait() == "hello"  # the item-ready branch, not the queue.Empty raise
    finally:
        inbox.close()


def test_build_transports_rejects_an_unknown_kind() -> None:
    with pytest.raises(ValueError, match="unknown transport kind"):
        build_transports("carrier-pigeon", ("a", "b"))


def test_http_send_returns_false_when_the_lane_is_full(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = HttpTransport("w0", maxsize=1)
    monkeypatch.setattr(transport, "_sender", lambda dest, lane: None)  # never drains the lane
    transport.set_registry({"peer": ("127.0.0.1", 9)})
    try:
        assert transport.send("peer", "m1") is True  # fills the maxsize=1 lane
        assert transport.send("peer", "m2") is False  # queue.Full
    finally:
        transport.close()


def test_http_recv_returns_none_on_timeout_with_nothing_delivered() -> None:
    transport = HttpTransport("w0")
    try:
        assert transport.recv(timeout=0.05) is None
    finally:
        transport.close()


def test_http_recv_returns_none_when_closed_while_waiting() -> None:
    transport = HttpTransport("w0")
    result: list[object] = []

    def _blocking_recv() -> None:
        result.append(transport.recv(timeout=5))

    waiter = threading.Thread(target=_blocking_recv)
    waiter.start()
    time.sleep(0.05)  # let recv() enter its poll loop before closing
    transport.close()  # sets `_stop`, which wakes the waiting recv() with None (no deadline reached)
    waiter.join(timeout=5)
    assert result == [None]


def test_http_sender_skips_a_message_whose_dest_left_the_registry() -> None:
    transport = HttpTransport("w0")
    try:
        lane = transport._lane("ghost")  # starts the real sender thread; "ghost" is not in the registry
        lane.put("orphan")
        time.sleep(0.3)  # the sender wakes (0.1s poll), target lookup misses, `continue`s
        # neither counter moved: the message was skipped before the deliver-or-drop accounting ran
        assert (transport.deliveries, transport.drops) == (0, 0)
    finally:
        transport.close()
