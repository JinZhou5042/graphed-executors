"""Every peer wait that only the driver can end observes the run's release pipe (#70, #72)."""

from __future__ import annotations

import _thread
import multiprocessing
import threading
import time

import pytest
from _big_partial import BIG, plan

from graphed_executors.local import ThreadExecutor, _peer
from graphed_executors.local._transport import HttpTransport


@pytest.mark.parametrize("comms", ["ipc", "http"])
def test_an_interrupted_thread_run_returns_promptly_and_leaves_no_actor(comms: str) -> None:
    ex = ThreadExecutor(max_workers=2, comms=comms, steal=True, monitor=None)
    threading.Timer(1.0, _thread.interrupt_main).start()
    start = time.monotonic()
    with pytest.raises(KeyboardInterrupt):
        ex.run(plan(BIG, (3000, 3000)))
    assert time.monotonic() - start < 15.0
    deadline = time.monotonic() + 5.0
    while (
        any(t.name.startswith("graphed-peer-") for t in threading.enumerate()) and time.monotonic() < deadline
    ):
        time.sleep(0.05)
    assert not [t.name for t in threading.enumerate() if t.name.startswith("graphed-peer-")]


def test_an_http_actor_leaves_its_registry_wait_when_the_run_is_released() -> None:
    driver = HttpTransport("driver")  # takes the hello, then never sends a registry
    port = driver.port
    release_r, release_w = multiprocessing.Pipe(duplex=False)
    raised: list[BaseException] = []

    def actor() -> None:
        try:
            _peer.http_peer_actor(
                "w0",
                "127.0.0.1",
                port,
                1,
                [0, 1],
                ("w0",),
                lambda p, r: None,
                lambda a, b: a,
                [],
                release=release_r,
            )
        except BaseException as exc:
            raised.append(exc)

    t = threading.Thread(target=actor, daemon=True)
    t.start()
    try:
        got = driver.recv(timeout=10)
        assert got is not None, "the actor never said hello"
        message = got[1]
        assert isinstance(message, tuple) and message[0] == "hello", got
        time.sleep(0.5)
        assert t.is_alive(), "the actor must wait for its registry while the run is live"
        release_w.close()
        t.join(5)
    finally:
        driver.close()
    assert not t.is_alive(), "the released actor is still waiting for a registry"
    assert [type(e) for e in raised] == [RuntimeError]
    assert "left before sending the registry" in str(raised[0])


class _BrokenPipe:
    def poll(self) -> bool:
        raise BrokenPipeError("the pipe has been ended")


def test_released_reads_eof_and_a_broken_pipe_as_released() -> None:
    release_r, release_w = multiprocessing.Pipe(duplex=False)
    assert (_peer.released(None), _peer.released(release_r)) == (False, False)
    release_w.close()
    assert _peer.released(release_r) is True
    assert _peer.released(_BrokenPipe()) is True
