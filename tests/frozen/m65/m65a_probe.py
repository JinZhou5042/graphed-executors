"""Module-level (picklable) plan pieces, monitor and route set for the m65 run-control suites."""

from __future__ import annotations

import functools
import os
import threading
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

from graphed.core import (
    ExecContext,
    ExecResult,
    Partition,
    Plan,
    RunControl,
    StopCondition,
    Task,
    TaskEvent,
    TaskPhase,
)

from graphed_executors.local import PinnedPoolExecutor, ProcessPoolExecutor, ThreadExecutor

N = 40
CONTROLS: dict[str, RunControl] = {}


def _await_file(path: str, timeout_s: float = 10.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while not os.path.exists(path):
        if time.monotonic() > deadline:
            return False
        time.sleep(0.005)
    return True


def touch(path: str) -> None:
    with open(path, "w"):
        pass


@dataclass(frozen=True)
class Probe:
    """The task body: returns the partial for the key in ``partition.entry_start``.

    ``variant``: ``"onehot"`` (a tuple naming the key), ``"float"`` (a scalar whose sum depends on the
    fold order at 40 tasks) or ``"int"`` (the key itself).
    """

    n: int = N
    variant: str = "onehot"
    sleep_s: float = 0.0
    fail_key: int | None = None
    hold_key: int | None = None
    cancel_key: int | None = None
    token: str = ""
    entered_path: str = ""
    release_path: str = ""

    def __call__(self, partition: Partition, resources: object) -> Any:
        key = partition.entry_start
        if key in (self.fail_key, self.hold_key):
            touch(self.entered_path)
            _await_file(self.release_path)
            if key == self.fail_key:
                raise RuntimeError(f"m65 fail {key}")
        elif self.sleep_s:
            time.sleep(self.sleep_s)
        if key == self.cancel_key:
            CONTROLS[self.token].cancel()
        return partial(self.variant, self.n, key)


def partial(variant: str, n: int, key: int) -> Any:
    if variant == "onehot":
        return tuple(int(i == key) for i in range(n))
    if variant == "float":
        return 0.1 * key + (1e15 if key == 0 else 0.0)
    if variant == "int":
        return key
    raise ValueError(variant)


def add_tuples(a: tuple[int, ...], b: tuple[int, ...]) -> tuple[int, ...]:
    return tuple(x + y for x, y in zip(a, b, strict=True))


def add(a: Any, b: Any) -> Any:
    return a + b


def _zeros(n: int) -> tuple[int, ...]:
    return (0,) * n


def zeros(n: int) -> Callable[[], tuple[int, ...]]:
    return functools.partial(_zeros, n)


def _float_zero() -> float:
    return 0.0


def _int_zero() -> int:
    return 0


def tasks(n: int = N, entries: dict[int, int] | None = None) -> list[Task]:
    """Task ``k`` reads entries ``[k, k + entries.get(k, 1))``, so ``entry_start`` is its key."""
    sizes = entries or {}
    return [Task(k, Partition(f"m65-{k}", "t", k, k + sizes.get(k, 1))) for k in range(n)]


class OneBatch:
    """An adaptive ``next_tasks``: every task on the first call, ``None`` after; counts its calls."""

    def __init__(self, batch: Iterable[Task]) -> None:
        self.batch = list(batch)
        self.calls = 0

    def __call__(self, ctx: ExecContext) -> list[Task] | None:
        self.calls += 1
        return self.batch if self.calls == 1 else None


def make_plan(
    probe: Probe,
    *,
    adaptive: bool = False,
    batch: Iterable[Task] | None = None,
    stop: StopCondition | None = None,
) -> Plan[Any]:
    """Fixed plan over ``batch`` (default ``tasks(probe.n)``), or an adaptive one handing it out once."""
    todo = list(tasks(probe.n) if batch is None else batch)
    combine, empty = {
        "onehot": (add_tuples, zeros(probe.n)),
        "float": (add, _float_zero),
        "int": (add, _int_zero),
    }[probe.variant]
    if adaptive:
        return Plan(process=probe, combine=combine, empty=empty, next_tasks=OneBatch(todo), stop=stop)
    return Plan(process=probe, combine=combine, empty=empty, tasks=todo, stop=stop)


class Recorder:
    """A thread-safe monitor; runs ``on_first_finished`` once, in the thread delivering that event."""

    def __init__(self, on_first_finished: Callable[[], None] | None = None) -> None:
        self.events: list[TaskEvent] = []
        self.first_finished = threading.Event()
        self.submitted_at_first_finished = -1
        self._on_first_finished = on_first_finished
        self._lock = threading.Lock()

    def on_task(self, event: TaskEvent) -> None:
        fire = False
        with self._lock:
            self.events.append(event)
            if event.phase is TaskPhase.FINISHED and not self.first_finished.is_set():
                self.submitted_at_first_finished = sum(e.phase is TaskPhase.SUBMITTED for e in self.events)
                self.first_finished.set()
                fire = True
        if fire and self._on_first_finished is not None:
            self._on_first_finished()

    def on_profile(self, worker: str, payload: bytes) -> None:
        pass

    def on_combine(self, leaves_done: int) -> None:
        pass

    def worker_profiler_factory(self) -> None:
        return None

    def keys(self, phase: TaskPhase) -> set[int]:
        with self._lock:
            return {e.key for e in self.events if e.phase is phase}

    def count(self, phase: TaskPhase) -> int:
        with self._lock:
            return sum(e.phase is phase for e in self.events)


class Background:
    """Runs ``fn`` in a daemon thread; ``result`` re-raises its error or returns its value."""

    def __init__(self, fn: Callable[[], ExecResult[Any]]) -> None:
        self._value: ExecResult[Any] | None = None
        self._error: BaseException | None = None
        self._thread = threading.Thread(target=self._run, args=(fn,), daemon=True)
        self._thread.start()

    def _run(self, fn: Callable[[], ExecResult[Any]]) -> None:
        try:
            self._value = fn()
        except BaseException as exc:
            self._error = exc

    def result(self, timeout_s: float = 30.0) -> ExecResult[Any]:
        self._thread.join(timeout_s)
        assert not self._thread.is_alive(), f"run() did not return within {timeout_s} s"
        if self._error is not None:
            raise self._error
        assert self._value is not None
        return self._value


@dataclass(frozen=True)
class Route:
    cls: type[Any]
    kwargs: dict[str, Any]
    adaptive: bool = False

    @property
    def thread(self) -> bool:
        return self.cls is ThreadExecutor

    def make(self, monitor: Any = None, **extra: Any) -> Any:
        return self.cls(max_workers=2, monitor=monitor, **self.kwargs, **extra)


ROUTES: dict[str, Route] = {
    "thread-hub": Route(ThreadExecutor, {"comms": None}),
    "thread-ipc": Route(ThreadExecutor, {}),
    "thread-http": Route(ThreadExecutor, {"comms": "http"}),
    "proc-hub": Route(ProcessPoolExecutor, {"comms": None}),
    "proc-ipc": Route(ProcessPoolExecutor, {}),
    "proc-http": Route(ProcessPoolExecutor, {"comms": "http"}),
    "pinned": Route(PinnedPoolExecutor, {}),
    "thread-pooled": Route(ThreadExecutor, {"comms": None, "pooled_combines": True}),
    "proc-pooled": Route(ProcessPoolExecutor, {"comms": None, "pooled_combines": True}),
    "thread-adaptive": Route(ThreadExecutor, {}, adaptive=True),
    "proc-adaptive": Route(ProcessPoolExecutor, {}, adaptive=True),
}
