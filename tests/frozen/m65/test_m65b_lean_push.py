"""m65 B: lean events, lazy labels and per-worker push on the local executor routes."""

from __future__ import annotations

import atexit
import functools
import os
import pickle
import queue
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

import m65a_probe as mp
import m65b_probe as bp
import pytest
from graphed.core import Partition, Task, TaskPhase
from test_inprocess_paths import _reset_globals

import graphed_executors.local.executors as ex
from graphed_executors.local import ProcessPoolExecutor

TERMINAL = (TaskPhase.FINISHED, TaskPhase.ERRORED)
EXACT = [
    "thread-hub",
    "thread-ipc",
    "thread-http",
    "thread-pooled",
    "thread-adaptive",
    "proc-ipc",
    "proc-http",
    "pinned",
]
BEST_EFFORT = ["proc-hub", "proc-pooled", "proc-adaptive"]
PUSH_ROUTES = ["proc-hub", "proc-ipc", "proc-http", "pinned"]
PEER_ROUTES = ["proc-ipc", "proc-http", "pinned"]


def _plan(route: str, probe: mp.Probe, **kw: Any) -> Any:
    return mp.make_plan(probe, adaptive=mp.ROUTES[route].adaptive, **kw)


def _terminal_keys(rec: mp.Recorder) -> list[int]:
    with rec._lock:
        return [e.key for e in rec.events if e.phase in TERMINAL]


@pytest.mark.parametrize("route", EXACT + BEST_EFFORT)
def test_lean_routes_emit_submitted_and_terminal_only(route: str) -> None:
    probe = mp.Probe()
    bare = mp.ROUTES[route].make().run(_plan(route, probe))
    rec = bp.LeanRecorder()
    res = mp.ROUTES[route].make(rec).run(_plan(route, probe))
    assert res.value == bare.value
    assert rec.count(TaskPhase.STARTED) == 0
    assert all(e.partition == "" for e in rec.events if e.phase in TERMINAL)
    assert all(e.partition for e in rec.events if e.phase is TaskPhase.SUBMITTED)
    keys = _terminal_keys(rec)
    if route in EXACT:
        assert sorted(keys) == list(range(mp.N))
    else:
        assert len(keys) <= mp.N
        assert set(keys) <= set(range(mp.N))


def _worker_files(d: Path, driver: bp.PushRecorder) -> list[tuple[int, frozenset[str], list[dict[str, Any]]]]:
    name = os.path.basename(driver.path)
    return [c for c, f in zip(bp.connections(str(d)), sorted(os.listdir(d)), strict=True) if f != name]


def _finished(conns: list[tuple[int, frozenset[str], list[dict[str, Any]]]]) -> list[int]:
    return [e["key"] for _, _, evs in conns for e in evs if e["phase"] == "finished"]


@pytest.mark.parametrize("route", PUSH_ROUTES)
def test_per_worker_push_bypasses_the_driver(route: str, tmp_path: Path) -> None:
    n = 16
    probe = mp.Probe(n=n)
    push_dir, route_dir = tmp_path / "push", tmp_path / "route"
    push_dir.mkdir()
    route_dir.mkdir()

    driver = bp.PushRecorder(str(push_dir), per_worker=True)
    res = mp.ROUTES[route].make(driver).run(_plan(route, probe))
    assert res.value == (1,) * n
    assert sum(e.phase is TaskPhase.SUBMITTED for e in driver.events) == n
    assert not [e for e in driver.events if e.phase is not TaskPhase.SUBMITTED]
    conns = bp.connections(str(push_dir))
    workers = _worker_files(push_dir, driver)
    assert sorted(_finished(workers)) == list(range(n))
    names = frozenset().union(*(w for _, w, _ in workers))
    assert len(names) >= 1
    assert len(conns) == 1 + len(names)
    pids = [pid for pid, _, _ in workers]
    assert len(set(pids)) == len(pids)
    assert os.getpid() not in pids

    routed = bp.PushRecorder(str(route_dir), per_worker=False)
    res = mp.ROUTES[route].make(routed).run(_plan(route, probe))
    assert res.value == (1,) * n
    assert len(bp.connections(str(route_dir))) == 1
    finished = [e.key for e in routed.events if e.phase is TaskPhase.FINISHED]
    if route in PEER_ROUTES:
        assert sorted(finished) == list(range(n))
    else:
        assert 1 <= len(finished) <= n
        assert set(finished) <= set(range(n))


@pytest.mark.parametrize("comms", [None, "ipc"], ids=["hub", "ipc"])
def test_persistent_pool_pushes_each_run_to_its_own_monitor(comms: str | None, tmp_path: Path) -> None:
    n = 16
    probe = mp.Probe(n=n)
    dirs = [tmp_path / "run1", tmp_path / "run2"]
    for d in dirs:
        d.mkdir()
    first, second = (bp.PushRecorder(str(d), per_worker=True) for d in dirs)
    with ProcessPoolExecutor(max_workers=2, persistent=True, comms=comms, monitor=first) as pool:
        assert pool.run(mp.make_plan(probe)).value == (1,) * n
        pool.monitor = second
        assert pool.run(mp.make_plan(probe)).value == (1,) * n
        pool.monitor = None
        assert pool.run(mp.make_plan(probe)).value == (1,) * n
    assert sorted(_finished(_worker_files(dirs[0], first))) == list(range(n))
    assert sorted(_finished(_worker_files(dirs[1], second))) == list(range(n))


class _ClosingRecorder(bp._FileRecorder):
    """Registers its own exit flush when built, as a worker monitor does; counts profiles after it."""

    def __init__(self, directory: str) -> None:
        super().__init__(directory, False)
        self.closed = False
        self.late_profiles = 0
        atexit.register(self.close)

    def close(self) -> None:
        self.closed = True

    def on_profile(self, worker: str, payload: bytes) -> None:
        if self.closed:
            self.late_profiles += 1
        else:
            super().on_profile(worker, payload)


def _drain_threads() -> list[threading.Thread]:
    return [t for t in threading.enumerate() if t.name == "graphed-dash-worker-drain" and t.is_alive()]


def _push(factory: Callable[[], Any], lean: bool = False) -> dict[str, Any]:
    return {"monitor_factory": factory, "lean": lean}


def test_inprocess_worker_push_and_lean(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    built: list[Any] = []

    def factory(make: Callable[[], Any]) -> Any:
        mon = make()
        built.append(mon)
        return mon

    process = mp.Probe(n=1, variant="int")
    task = Task(0, Partition("m65b-0", "t", 0, 1))
    token = "m65b-tok"
    ex._prime_shared(token, pickle.dumps(process))
    lean_f = functools.partial(factory, functools.partial(bp.worker_recorder, str(tmp_path), True))
    try:
        ex._proc_init(None, None, **_push(lean_f, lean=True))
        assert ex._proc_task_shared(token, task) == 0
        (conn,) = bp.connections(str(tmp_path))
        assert [(e["phase"], e["partition"]) for e in conn[2]] == [("finished", "")]

        ex._proc_init(bp.fake_profiler, queue.Queue())
        assert _drain_threads()

        ex._proc_init()
        assert _drain_threads() == []
        labels: list[Partition] = []
        real = ex.partition_label

        def counting(p: Partition) -> str:
            labels.append(p)
            return real(p)

        monkeypatch.setattr(ex, "partition_label", counting)
        assert ex._proc_task_shared(token, task) == 0
        assert ex._thread_task(process, task, None) == 0
        assert labels == []
        monkeypatch.undo()

        prof_dir = tmp_path / "prof"
        prof_dir.mkdir()
        built.clear()
        ex._proc_init(
            bp.fake_profiler,
            None,
            **_push(functools.partial(factory, functools.partial(bp.worker_recorder, str(prof_dir), False))),
        )
        ex._proc_last_flush = 0.0
        assert ex._proc_task_shared(token, task) == 0
        ex._proc_drain_final()
        assert sum(m.profiles for m in built) >= 1

        hooks: list[Callable[[], Any]] = []

        def register(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Callable[..., Any]:
            hooks.append(functools.partial(fn, *args, **kwargs))
            return fn

        monkeypatch.setattr(atexit, "register", register)
        monkeypatch.setattr(ex, "_atexit_registered", ex._atexit_registered, raising=False)
        exit_dir = tmp_path / "exit"
        exit_dir.mkdir()
        built.clear()
        ex._proc_init(
            bp.fake_profiler,
            None,
            **_push(functools.partial(factory, functools.partial(_ClosingRecorder, str(exit_dir)))),
        )
        for hook in reversed(hooks):
            hook()
        assert built
        assert sum(m.profiles for m in built) >= 1
        assert sum(m.late_profiles for m in built) == 0
    finally:
        monkeypatch.undo()
        ex._proc_init()
        _reset_globals()


@pytest.mark.parametrize("lean", [False, True], ids=["default", "lean"])
@pytest.mark.parametrize("route", ["thread-ipc", "thread-http"])
def test_peer_events_carry_task_keys(route: str, lean: bool) -> None:
    n = 12
    batch = [Task(100 + 2 * i, Partition(f"m65b-{i}", "t", i, i + 1)) for i in range(n)]
    rec = bp.LeanRecorder() if lean else mp.Recorder()
    res = mp.ROUTES[route].make(rec).run(mp.make_plan(mp.Probe(n=n), batch=batch))
    assert res.value == (1,) * n
    submitted = rec.keys(TaskPhase.SUBMITTED)
    assert submitted == {t.key for t in batch}
    assert set(_terminal_keys(rec)) == submitted
    assert len(_terminal_keys(rec)) == n
    assert rec.keys(TaskPhase.STARTED) <= submitted
