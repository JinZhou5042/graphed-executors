"""m38 HTTP route: hand-offs that beat a worker's registry are applied, not parked."""

from __future__ import annotations

import threading
import time
from typing import Any

import m65a_probe as mp
import pytest
from graphed.core import StopReason, TaskPhase

from graphed_executors.local import executors
from graphed_executors.local._peer import http_driver_handshake

ONES = (1,) * mp.N
W0_REGISTRY_DELAY_S = 0.3


def test_nodes_that_beat_a_workers_registry_still_form_the_root(monkeypatch: pytest.MonkeyPatch) -> None:
    # w1 gets its registry at once and settles its whole subtree (three ``node`` hand-offs to w0)
    # while w0 is still waiting for its own registry.
    w0_registry_sent_at: list[float] = []
    w1_finished: list[tuple[int, float]] = []  # (leaf, driver-clock time)

    def late_w0(driver_t: Any, addrs: tuple[str, ...], **kw: Any) -> None:
        real_send = driver_t.send

        def send(dest: str, message: Any) -> bool:
            if dest == "w0" and message[0] == "registry":

                def later() -> None:
                    w0_registry_sent_at.append(time.monotonic())
                    real_send(dest, message)

                threading.Timer(W0_REGISTRY_DELAY_S, later).start()
                return True
            return bool(real_send(dest, message))

        driver_t.send = send
        http_driver_handshake(driver_t, addrs, **kw)

    monkeypatch.setattr(executors, "http_driver_handshake", late_w0)
    rec = mp.Recorder()
    real_on_task = rec.on_task

    def on_task(event: Any) -> None:
        real_on_task(event)
        if event.phase is TaskPhase.FINISHED and event.worker == "w1":
            w1_finished.append((event.key, time.monotonic()))

    rec.on_task = on_task
    ex = mp.ROUTES["proc-http"].make(rec)
    res = mp.Background(lambda: ex.run(mp.make_plan(mp.Probe()))).result(timeout_s=20.0)
    assert (res.value, res.n_partitions, res.stopped) == (ONES, mp.N, StopReason.EXHAUSTED)
    # the race happened: w1's own leaves (it may steal w0's later) all finished before w0's
    # registry went out, so its subtree hand-offs reached w0 first
    own = {leaf: t for leaf, t in w1_finished if leaf >= mp.N // 2}
    assert set(own) == set(range(mp.N // 2, mp.N))
    assert max(own.values()) < w0_registry_sent_at[0]
