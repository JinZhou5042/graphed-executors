"""Plan parts the m68d extras ship to local pilots by module name (stdlib and graphed only)."""

from __future__ import annotations

import time
from pathlib import Path

from graphed.core.execution import Partition

GATE_S = 60.0


def gated_leaf(partition: Partition, resources: object) -> tuple[int, ...]:
    """Leaf ``i`` returns ``(i,)``; from leaf 2 on it first waits for the file ``partition.tree`` names."""
    i = partition.entry_start
    deadline = time.monotonic() + GATE_S
    while i >= 2 and not Path(partition.tree).exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    return (i,)


def failing_combine(a: tuple[int, ...], b: tuple[int, ...]) -> tuple[int, ...]:
    if a == (0,):
        raise ValueError("m-combine-of-leaf-zero-failed")
    return a + b


def no_leaves() -> tuple[int, ...]:
    return ()
