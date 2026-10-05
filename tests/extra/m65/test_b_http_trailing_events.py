"""m65 B: a thread-http peer run delivers every worker event, even ones still in flight at the root."""

from __future__ import annotations

import time
import urllib.request
from typing import Any

import m65a_probe as mp
import pytest
from graphed.core import TaskPhase


def test_slow_worker_event_posts_still_reach_the_monitor(monkeypatch: pytest.MonkeyPatch) -> None:
    real = urllib.request.urlopen
    slowed: list[int] = []

    def slow(req: urllib.request.Request, *args: Any, **kwargs: Any) -> Any:
        data = req.data if isinstance(req.data, bytes) else b""
        if b"events" in data and b"w1" in data:  # w1's events land after w0 forms the root
            slowed.append(1)
            time.sleep(0.1)
        return real(req, *args, **kwargs)

    monkeypatch.setattr(urllib.request, "urlopen", slow)
    rec = mp.Recorder()
    n = 12
    mp.ROUTES["thread-http"].make(rec).run(mp.make_plan(mp.Probe(n=n)))
    assert slowed
    assert rec.keys(TaskPhase.FINISHED) == set(range(n))
