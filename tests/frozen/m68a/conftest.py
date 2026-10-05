"""Fixtures for the m68a frozen suite: a fresh shared trace per test, and a teardown backstop that stops
any managed child a failing implementation left running (the tests themselves assert it is gone)."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path

import pytest
from services_harness import kill_quietly, spy_reset, trace_reset


@pytest.fixture(autouse=True)
def fresh_trace() -> Iterator[None]:
    trace_reset()
    spy_reset()
    yield
    trace_reset()
    spy_reset()


class Children:
    """Report files and pids of managed children a test expects; each is killed at teardown if alive."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.pids: list[int] = []
        self._n = 0

    def report(self, name: str = "child") -> Path:
        self._n += 1
        return self.root / f"{name}-{self._n}.json"

    def add(self, pid: int) -> int:
        self.pids.append(pid)
        return pid

    def cleanup(self) -> None:
        for path in self.root.glob("*.json"):
            try:
                self.pids.append(int(json.loads(path.read_text())["pid"]))
            except (OSError, ValueError, KeyError):
                continue
        for pid in self.pids:
            kill_quietly(pid)


@pytest.fixture
def children(tmp_path: Path) -> Iterator[Children]:
    root = tmp_path / "children"
    root.mkdir()
    kids = Children(root)
    try:
        yield kids
    finally:
        kids.cleanup()
