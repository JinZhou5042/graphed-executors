"""m65 A2.2: the peer routes' pause deadline, late-cancel teardown, driver fold and idempotent handlers."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import m65a_probe as mp
import pytest
from graphed.core import Partition, RunControl, RunState, StopReason
from graphed.core.execution import LocalResources

from graphed_executors.local import ProcessPoolExecutor, _peer, executors
from graphed_executors.local._reduce import lazy_tree_reduce, running_fold

ONES = (1,) * mp.N


@pytest.mark.parametrize("route", ["thread-ipc", "proc-ipc"])
def test_paused_time_does_not_count_toward_the_root_deadline(
    route: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(executors, "_PEER_ROOT_TIMEOUT_S", 3.0)
    ctl = RunControl()
    rec = mp.Recorder(on_first_finished=ctl.pause)
    ex = mp.ROUTES[route].make(rec, control=ctl)
    run = mp.Background(lambda: ex.run(mp.make_plan(mp.Probe(sleep_s=0.01))))
    assert rec.first_finished.wait(30)
    time.sleep(4.0)
    ctl.resume()
    res = run.result()
    assert (res.value, res.stopped) == (ONES, StopReason.EXHAUSTED)


@pytest.mark.parametrize("route", ["thread-ipc", "proc-ipc"])
def test_the_root_timeout_message_follows_the_constant(route: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(executors, "_PEER_ROOT_TIMEOUT_S", 0.5)
    ex = mp.ROUTES[route].make()
    run = mp.Background(lambda: ex.run(mp.make_plan(mp.Probe(sleep_s=1.0))))
    with pytest.raises(TimeoutError, match=r"within 0\.5s"):
        run.result(30)


def test_a_cancel_after_the_root_leaves_a_persistent_pool_clean(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    told: list[RunState] = []
    relay = _peer.PeerControl.relay

    def spy(self: _peer.PeerControl[Any]) -> bool:
        paused = relay(self)
        told.append(self._told)
        return paused

    monkeypatch.setattr(_peer.PeerControl, "relay", spy)
    entered, release = str(tmp_path / "entered"), str(tmp_path / "release")
    ctl = RunControl()
    # Leaf 19 (w0's last, which forms the root) is held until the cancel has gone out.
    probe = mp.Probe(hold_key=19, entered_path=entered, release_path=release)
    with ProcessPoolExecutor(max_workers=2, persistent=True, steal=False, control=ctl) as ex:
        run = mp.Background(lambda: ex.run(mp.make_plan(probe)))
        assert mp._await_file(entered)
        ctl.cancel()
        deadline = time.monotonic() + 10
        while RunState.CANCELLED not in told and time.monotonic() < deadline:
            time.sleep(0.01)
        mp.touch(release)
        run.result()
        ex.control = None
        again = mp.Background(lambda: ex.run(mp.make_plan(mp.Probe()))).result(30)
    assert RunState.CANCELLED in told  # the cancel reached the workers of the first run
    assert (again.value, again.stopped) == (ONES, StopReason.EXHAUSTED)


def _node(level: int, pos: int) -> float:
    """The plan-tree value of node ``(level, pos)`` over the float partials (odd levels carry up)."""
    if level == 0:
        return float(mp.partial("float", mp.N, pos))
    size = (mp.N + (1 << (level - 1)) - 1) >> (level - 1)
    left = _node(level - 1, 2 * pos)
    return left + _node(level - 1, 2 * pos + 1) if 2 * pos + 1 < size else left


class _Peers:
    def peers(self) -> tuple[str, ...]:
        return ("w0", "w1")


def test_the_driver_fold_of_hand_ins_is_the_tree_root() -> None:
    # The sibling pairs (0,26)/(0,27), (0,30)/(0,31) and (2,8)/(2,9) were never settled by a worker;
    # w0's message and the item (0, 27) arrive twice.
    w0 = ("cancelled", "w0", 20, [(level, pos, _node(level, pos)) for level, pos in ((4, 0), (3, 2))])
    items = [(level, pos, _node(level, pos)) for level, pos in ((1, 12), (0, 26), (0, 27), (0, 27))]
    w1_nodes = ((1, 14), (0, 30), (0, 31), (2, 8), (2, 9))
    w1 = ("cancelled", "w1", 20, [(level, pos, _node(level, pos)) for level, pos in w1_nodes])
    ctl: _peer.PeerControl[float] = _peer.PeerControl(RunControl(), _Peers(), mp.N, mp.add, float)
    got = [ctl.take(m) for m in (w0, w0, *[("item", *i) for i in items], w1)]
    root, _ = lazy_tree_reduce(mp.N, ((k, mp.partial("float", mp.N, k)) for k in range(mp.N)), mp.add, float)
    assert got[:-1] == [None] * (len(got) - 1)
    assert got[-1] == (root, mp.N)
    pieces = sorted((pos << level, v) for level, pos, v in {*w0[3], *items, *w1[3]})
    assert running_fold(iter(pieces), mp.add, float)[0] != root  # a first-leaf fold alone misses it


class _Script:
    """A worker transport replaying scripted polls; records every send."""

    address = "w1"

    def __init__(self, polls: list[list[Any]]) -> None:
        self.polls = polls
        self.sent: list[tuple[str, Any]] = []

    def send(self, dest: str, message: Any) -> bool:
        self.sent.append((dest, message))
        return True

    def poll(self) -> list[tuple[str, Any]]:
        return [("w0", m) for m in self.polls.pop(0)] if self.polls else [("driver", ("done",))]

    def recv(self, timeout: float | None = None) -> tuple[str, Any] | None:
        return None


def test_retried_grant_and_cancel_act_once() -> None:
    grant = ("steal_resp", [(0, Partition("m65-0", "t", 0, 1))], (-1, 1))
    transport = _Script([[grant, grant], [], [], [("cancel",), ("cancel",)]])
    stats = _peer.process_and_reduce(
        "w1",
        transport,
        4,
        _peer.make_bounds(4, 2),
        ("w0", "w1"),
        lambda part, res: 1.0,
        mp.add,
        [],
        LocalResources(),
    )
    cancelled = [m for d, m in transport.sent if m[0] == "cancelled"]
    assert stats["processed"] == 1
    assert [m for d, m in transport.sent if m[0] == "leaf"] == [("leaf", 0, 1.0)]
    assert cancelled == [("cancelled", "w1", 1, [])]
