"""m65 C: a ``complete_events`` monitor holds each run's events by the time ``run()`` returns or raises
(plan-C C-8). Plain-Python processes; the graphed C API is read as module attributes inside the legs that use it."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterable
from concurrent.futures import Future
from pathlib import Path
from typing import Any

import awkward as ak
import graphed.debug as gd
import graphed.preserve as gp
import pytest
from graphed import Session
from graphed.awkward import AwkwardBackend, from_awkward
from graphed.core import ExecContext, Partition, Plan, Task, TaskEvent, TaskPhase

import graphed_executors.local.executors as ex_mod
from graphed_executors.local import PinnedPoolExecutor, ProcessPoolExecutor, ThreadExecutor

TERMINAL = (TaskPhase.FINISHED, TaskPhase.ERRORED)
PHASES = (TaskPhase.SUBMITTED, TaskPhase.STARTED, TaskPhase.FINISHED)


def _add(a: float, b: float) -> float:
    return a + b


def _zero() -> float:
    return 0.0


def _good(p: Partition, _r: object) -> float:
    time.sleep(0.02)
    return 1.0


def _fail(p: Partition, _r: object) -> float:
    if p.blind_step == 1:
        raise ValueError("m65c boom")
    time.sleep(0.2)
    return 1.0


def _fail_slow(p: Partition, _r: object) -> float:
    if p.blind_step == 1:
        raise ValueError("m65c boom")
    time.sleep(0.8)
    return 1.0


def _tasks(n: int, base: int) -> list[Task]:
    return [Task(base + k, Partition.blind("u", "t", k, n)) for k in range(n)]


def _plan(fn: Any, n: int = 4, base: int = 0) -> Plan[float]:
    return Plan(process=fn, combine=_add, empty=_zero, tasks=_tasks(n, base))


class _Once:
    def __init__(self, n: int, base: int) -> None:
        self.n, self.base, self.done = n, base, False

    def __call__(self, _ctx: ExecContext) -> Iterable[Task] | None:
        if self.done:
            return None
        self.done = True
        return _tasks(self.n, self.base)


def _adaptive(fn: Any, n: int = 4, base: int = 0) -> Plan[float]:
    return Plan(process=fn, combine=_add, empty=_zero, next_tasks=_Once(n, base))


class Dash:
    def __init__(self) -> None:
        self.events: list[TaskEvent] = []
        self.lock = threading.Lock()

    def on_task(self, event: TaskEvent) -> None:
        with self.lock:
            self.events.append(event)

    def on_profile(self, worker: str, payload: bytes) -> None:
        pass

    def on_combine(self, leaves_done: int) -> None:
        pass

    def worker_profiler_factory(self) -> None:
        return None

    def take(self) -> list[TaskEvent]:
        with self.lock:
            got, self.events = self.events, []
        return got


class Rec(Dash):
    complete_events = True


class SlowRec(Rec):
    def on_task(self, event: TaskEvent) -> None:
        if event.phase in TERMINAL:
            time.sleep(0.03)
        super().on_task(event)


class RaisingRec(Rec):
    def on_task(self, event: TaskEvent) -> None:
        super().on_task(event)
        raise RuntimeError("dashboard gone")


def _count(events: list[TaskEvent], *phases: TaskPhase) -> tuple[int, ...]:
    return tuple(sum(e.phase is ph for e in events) for ph in phases)


HUB_FAILING: dict[str, tuple[Callable[[Rec], Any], Callable[..., Plan[float]]]] = {
    "proc-hub": (lambda m: ProcessPoolExecutor(2, comms=None, persistent=True, monitor=m), _plan),
    "thread-hub": (lambda m: ThreadExecutor(2, comms=None, persistent=True, monitor=m), _plan),
    "thread-adaptive": (lambda m: ThreadExecutor(2, persistent=True, monitor=m), _adaptive),
    "proc-adaptive": (lambda m: ProcessPoolExecutor(2, persistent=True, monitor=m), _adaptive),
}


def _failed_then_good(ex: Any, m: Rec, failing: Plan[float]) -> None:
    ex.run(_plan(_good, 2))
    time.sleep(0.3)
    m.take()
    with pytest.raises(ValueError, match="m65c boom"):
        ex.run(failing)
    held = m.take()
    assert _count(held, TaskPhase.FINISHED, TaskPhase.ERRORED) == (3, 1)
    assert [e.key for e in held if e.phase is TaskPhase.ERRORED] == [1]
    time.sleep(0.5)
    ex.run(_plan(_good, 2))
    after = m.take()
    assert sorted(e.key for e in after if e.phase in TERMINAL) == [0, 1]
    assert {e.key for e in after} == {0, 1}


@pytest.mark.parametrize("route", list(HUB_FAILING))
def test_hub_failing_run_holds_its_events(route: str) -> None:
    make, shape = HUB_FAILING[route]
    m = Rec()
    ex = make(m)
    try:
        _failed_then_good(ex, m, shape(_fail))
    finally:
        ex.close()


def test_hub_wait_follows_the_leaf_futures(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ex_mod, "_HUB_EVENT_DRAIN_S", 0.3)
    m = Rec()
    ex = ProcessPoolExecutor(2, comms=None, persistent=True, monitor=m)
    try:
        _failed_then_good(ex, m, _plan(_fail_slow))
    finally:
        ex.close()


def _five_runs(m: Rec, *, warm: bool = False, bound_s: float | None = None) -> list[TaskEvent]:
    ex = ProcessPoolExecutor(2, comms=None, persistent=True, monitor=m)
    seen: list[TaskEvent] = []
    try:
        if warm:
            ex.run(_plan(_good))
            time.sleep(0.3)
            m.take()
        for _ in range(5):
            t0 = time.perf_counter()
            ex.run(_plan(_good))
            took = time.perf_counter() - t0
            got = m.take()
            assert _count(got, *PHASES) == (4, 4, 4)
            if bound_s is not None:
                assert took < bound_s
            seen += got
    finally:
        ex.close()
    return seen


def test_persistent_hub_runs_hold_their_events_on_a_kept_pool() -> None:
    seen = _five_runs(Rec())
    assert len({e.worker for e in seen if e.phase is TaskPhase.STARTED}) <= 2


def test_hub_counts_after_a_slow_monitor_returns() -> None:
    _five_runs(SlowRec())


def test_hub_counts_when_the_monitor_raises() -> None:
    _five_runs(RaisingRec(), warm=True, bound_s=1.0)


SWITCH_IN = {"dash": 5, "dash-none": 3, "failing-dash": 3, "adaptive-peer": 3}


@pytest.mark.parametrize("before", list(SWITCH_IN))
def test_switch_in_run_holds_only_its_own_events(before: str) -> None:
    for _ in range(SWITCH_IN[before]):
        d = Dash()
        peer = before == "adaptive-peer"
        ex: Any = ProcessPoolExecutor(2, persistent=True, monitor=d, **({} if peer else {"comms": None}))
        try:
            ex.run(_adaptive(_good, base=10) if peer else _plan(_good, base=10))
            time.sleep(0.3)
            if before == "failing-dash":
                with pytest.raises(ValueError, match="m65c boom"):
                    ex.run(_plan(_fail, base=10))
            elif peer:
                ex.run(_adaptive(_good, base=10))
            else:
                ex.run(_plan(_good, base=10))
            if before == "dash-none":
                ex.monitor = None
                ex.run(_plan(_good, base=20))
            m = Rec()
            ex.monitor = m
            ex.run(_plan(_good))
            got = m.take()
            assert _count(got, *PHASES) == (4, 4, 4)
            assert {e.key for e in got} == {0, 1, 2, 3}
        finally:
            ex.close()


PEER_FAILING: dict[str, Callable[[Any], Any]] = {
    "proc-http": lambda m: ProcessPoolExecutor(2, comms="http", monitor=m),
    "pinned": lambda m: PinnedPoolExecutor(2, monitor=m),
    "thread-ipc": lambda m: ThreadExecutor(2, comms="ipc", monitor=m),
}


@pytest.mark.parametrize("route", list(PEER_FAILING))
def test_peer_failing_run_reports_its_failing_task(route: str, tmp_path: Path) -> None:
    reports = []
    for _ in range(5):
        rec = gd.RunRecorder()
        ex = PEER_FAILING[route](rec)
        try:
            with pytest.raises(ValueError, match="m65c boom") as info:
                ex.run(_plan(_fail))
            r = rec.report(error=info.value)
        finally:
            ex.close()
        assert {t.key: t.state for t in r.tasks}[1] == "errored"
        assert r.failed_keys == (1,)
        reports.append(r)

    events = ak.Array({"x": [1.0, 2.0, 3.0]})
    s = Session(AwkwardBackend())
    bundle = gp.build_bundle(
        tmp_path / "b", session=s, value=from_awkward(s, "events", events).x * 2, datasets={"events": events}
    )
    digests = [gp.attach_run_report(bundle, r.to_json()) for r in reports]
    text = gp.inspect(bundle)
    for digest in digests:
        (head,) = [ln for ln in text.splitlines() if ln.startswith(f"    {digest[:12]} ")]
        assert "errored=1" in head


class _FakeDriver:
    def __init__(self, items: list[tuple[str, object]]) -> None:
        self.items = items
        self.sent: list[tuple[str, object]] = []
        self.lock = threading.Lock()

    def recv(self, timeout: float | None = None) -> tuple[str, object] | None:
        return self.items.pop(0) if self.items else None

    def peers(self) -> tuple[str, ...]:
        return ("w0", "w1")

    def send(self, dest: str, message: object) -> bool:
        with self.lock:
            self.sent.append((dest, message))
        return True

    def done_sent_to(self) -> list[str]:
        with self.lock:
            return sorted(dest for dest, msg in self.sent if msg == ("done",))


def _drain_leg(m: Dash) -> list[TaskPhase]:
    def ev(phase: TaskPhase, key: int, worker: str) -> TaskEvent:
        return TaskEvent(phase, key, worker, time.perf_counter(), "", 1)

    drv = _FakeDriver(
        [
            ("w1", ("events", [ev(TaskPhase.FINISHED, 2, "w1")])),
            ("w0", ("events", [ev(TaskPhase.STARTED, 1, "w0"), ev(TaskPhase.ERRORED, 1, "w0")])),
        ]
    )
    fut: Future[Any] = Future()
    fut.set_exception(ValueError("m65c boom"))
    ex = ProcessPoolExecutor(2, monitor=m)
    ex._run_monitor = m
    try:
        with pytest.raises(ValueError, match="m65c boom"):
            ex._collect_peer(drv, _plan(_fail), 4, [fut])
        deadline = time.monotonic() + 2.0
        while drv.done_sent_to() != ["w0", "w1"] and time.monotonic() < deadline:
            time.sleep(0.01)
        assert drv.done_sent_to() == ["w0", "w1"]
    finally:
        ex.close()
    return [e.phase for e in m.take() if e.key == 1]


def test_collect_peer_drains_the_failing_batch() -> None:
    assert _drain_leg(Rec()) == [TaskPhase.STARTED, TaskPhase.ERRORED]
    assert _drain_leg(Dash()) == []
