"""Module-level (picklable) recording monitors for the m65 B push suites: every monitor writes one
connection file ``<dir>/<pid>.<uuid>.jsonl`` on its first event, so a test sees which process built
how many worker monitors, without any dashboard stack."""

from __future__ import annotations

import functools
import json
import os
import threading
import uuid
from collections.abc import Callable
from typing import Any

import m65a_probe as mp
from graphed.core import TaskEvent


class _FileRecorder:
    def __init__(self, directory: str, lean: bool) -> None:
        self.lean_events = lean
        self.path = os.path.join(directory, f"{os.getpid()}.{uuid.uuid4().hex}.jsonl")
        self.profiles = 0
        self._fh: Any = None
        self._lock = threading.Lock()

    def on_task(self, event: TaskEvent) -> None:
        line = json.dumps(
            {
                "phase": event.phase.value,
                "key": event.key,
                "worker": event.worker,
                "partition": event.partition,
            }
        )
        with self._lock:
            if self._fh is None:
                self._fh = open(self.path, "a")  # noqa: SIM115
            self._fh.write(line + "\n")
            self._fh.flush()

    def on_profile(self, worker: str, payload: bytes) -> None:
        with self._lock:
            self.profiles += 1

    def on_combine(self, leaves_done: int) -> None:
        return None

    def worker_profiler_factory(self) -> None:
        return None


def worker_recorder(directory: str, lean: bool) -> _FileRecorder:
    return _FileRecorder(directory, lean)


class PushRecorder(_FileRecorder):
    """The driver monitor: also keeps its events in memory."""

    def __init__(self, directory: str, *, per_worker: bool, lean: bool = False) -> None:
        super().__init__(directory, lean)
        self.directory, self.per_worker = directory, per_worker
        self.events: list[TaskEvent] = []

    def on_task(self, event: TaskEvent) -> None:
        with self._lock:
            self.events.append(event)
        super().on_task(event)

    def worker_monitor_factory(self) -> Callable[[], _FileRecorder] | None:
        if not self.per_worker:
            return None
        return functools.partial(worker_recorder, self.directory, self.lean_events)


def connections(directory: str) -> list[tuple[int, frozenset[str], list[dict[str, Any]]]]:
    out = []
    for name in sorted(os.listdir(directory)):
        with open(os.path.join(directory, name)) as f:
            events = [json.loads(line) for line in f]
        out.append((int(name.split(".")[0]), frozenset(e["worker"] for e in events), events))
    return out


class LeanRecorder(mp.Recorder):
    lean_events = True


class FakeProfiler:
    def start(self) -> None:
        return None

    def flush(self) -> bytes | None:
        return b"prof-bytes"

    def stop(self) -> bytes | None:
        return b"prof-bytes"


def fake_profiler() -> FakeProfiler:
    return FakeProfiler()
