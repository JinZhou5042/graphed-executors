"""m65 B: ``SubmitRunner(ThreadBackend(2))`` emits lean events and pushes worker events through the
monitor's per-worker factory, on the fixed and adaptive paths."""

from __future__ import annotations

import os
import time
from pathlib import Path

import m65a_probe as mp
import m65b_probe as bp
import pytest
from graphed.core import RunControl, TaskPhase

from graphed_executors.submit import SubmitRunner, ThreadBackend

LEGS = {"fixed": False, "adaptive": True}
TERMINAL = (TaskPhase.FINISHED, TaskPhase.ERRORED)


@pytest.mark.parametrize("controlled", [False, True], ids=["plain", "control"])
@pytest.mark.parametrize("leg", LEGS)
def test_lean_submit_runner_emits_submitted_and_terminal_only(leg: str, controlled: bool) -> None:
    probe = mp.Probe()
    bare = SubmitRunner(ThreadBackend(2)).run(mp.make_plan(probe, adaptive=LEGS[leg]))
    rec = bp.LeanRecorder()
    runner = SubmitRunner(ThreadBackend(2), monitor=rec)
    if controlled:
        runner.control = RunControl()
    t0 = time.perf_counter()
    res = runner.run(mp.make_plan(probe, adaptive=LEGS[leg]))
    assert time.perf_counter() - t0 < 10.0
    assert res.value == bare.value
    assert rec.count(TaskPhase.STARTED) == 0
    terminal = [e for e in rec.events if e.phase in TERMINAL]
    assert sorted(e.key for e in terminal) == list(range(mp.N))
    assert all(e.partition == "" for e in terminal)
    assert all(e.partition for e in rec.events if e.phase is TaskPhase.SUBMITTED)


def test_submit_runner_per_worker_push(tmp_path: Path) -> None:
    for leg, adaptive in LEGS.items():
        d = tmp_path / leg
        d.mkdir()
        driver = bp.PushRecorder(str(d), per_worker=True)
        t0 = time.perf_counter()
        res = SubmitRunner(ThreadBackend(2), monitor=driver).run(mp.make_plan(mp.Probe(), adaptive=adaptive))
        assert time.perf_counter() - t0 < 10.0
        assert res.value == (1,) * mp.N
        conns = bp.connections(str(d))
        assert len(conns) == 2
        assert {pid for pid, _, _ in conns} == {os.getpid()}
        (worker,) = [
            evs
            for (_, _, evs), name in zip(conns, sorted(os.listdir(d)), strict=True)
            if name != os.path.basename(driver.path)
        ]
        assert sorted(e["key"] for e in worker if e["phase"] == "finished") == list(range(mp.N))
        assert not [e for e in driver.events if e.phase is TaskPhase.FINISHED]
