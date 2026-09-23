"""A monitor reassigned while a submitted plan runs reaches the next plan, not the running one."""

from __future__ import annotations

import sys
import tempfile
from collections import Counter
from collections.abc import Callable
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "frozen" / "m63"))

import m63_harness as h

from graphed_executors.local import ProcessPoolExecutor, ThreadExecutor
from graphed_executors.local.executors import _BaseExecutor


class CombineCounter(h.RecordingMonitor):  # type: ignore[misc]
    def __init__(self) -> None:
        super().__init__()
        self.combines = 0

    def on_combine(self, leaves_done: int) -> None:
        self.combines += 1


def _phases(m: CombineCounter) -> Counter[str]:
    return Counter(e.phase.name for e in list(m.events))


@pytest.mark.parametrize(
    ("make", "exact"),
    [
        (lambda m: ThreadExecutor(2, monitor=m), True),
        (lambda m: ThreadExecutor(2, monitor=m, comms=None), True),
        (lambda m: ProcessPoolExecutor(2, persistent=True, monitor=m), False),
        (lambda m: ProcessPoolExecutor(2, persistent=True, monitor=m, comms=None), False),
    ],
    ids=["thread-peer", "thread-hub", "process-peer", "process-hub-collector"],
)
def test_reassigned_monitor_does_not_split_running_plan(
    make: Callable[[CombineCounter], _BaseExecutor], exact: bool
) -> None:
    m1, m2 = CombineCounter(), CombineCounter()
    ex = make(m1)
    n = h.N_LEAVES
    try:
        with tempfile.TemporaryDirectory() as tmp:
            gate = h.new_gate(tmp, "g")
            fut = ex.submit(h.fixed_plan(1, gate_dir=gate))
            h.wait_for(lambda: _phases(m1)["SUBMITTED"] >= n)
            assert _phases(m1)["SUBMITTED"] == n
            ex.monitor = m2
            h.open_gate(gate)
            fut.result(timeout=h.GATE_TIMEOUT_S)
            h.wait_for(lambda: _phases(m1)["FINISHED"] >= n, timeout_s=5.0)
    finally:
        ex.close()
    assert not m2.events
    assert m2.combines == 0
    got = _phases(m1)
    if exact:
        assert (got["STARTED"], got["FINISHED"], m1.combines) == (n, n, n - 1)
    else:  # process workers ship events best-effort (drop-on-full)
        assert got["STARTED"] <= n and got["FINISHED"] <= n
        assert got["FINISHED"] > 0 and m1.combines == n - 1
