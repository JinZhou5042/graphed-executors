"""A transport failure on the outbox thread must reach the actor (tests/extra — NOT frozen).

Sends no longer happen on the actor thread, so a raising ``transport.send`` would otherwise die
silently in the outbox thread and the worker would report success with partials never delivered.
"""

from __future__ import annotations

from typing import Any

import pytest
from graphed.core.execution import LocalResources, Partition

from graphed_executors.local._peer import make_bounds, process_and_reduce


class _BrokenTransport:
    """Every send raises; ``recv`` releases the actor with the driver's ``done``."""

    address = "w0"

    def __init__(self) -> None:
        self.attempts = 0
        self.recvs = 0

    def send(self, dest: str, message: Any) -> bool:
        self.attempts += 1
        raise RuntimeError("wire down")

    def poll(self) -> list[tuple[str, Any]]:
        return []

    def recv(self, timeout: float | None = None) -> tuple[str, Any] | None:
        self.recvs += 1
        return ("driver", ("done",))


def _process(partition: Partition, resources: LocalResources) -> int:
    return 1


def test_outbox_send_failure_surfaces_in_the_actor() -> None:
    transport = _BrokenTransport()
    with pytest.raises(RuntimeError, match="wire down"):
        process_and_reduce(
            "w0",
            transport,
            1,
            make_bounds(1, 1),
            ("w0",),
            _process,
            lambda a, b: a + b,
            [(0, Partition("f.root", "Events", 0, 1))],
            LocalResources(),
            steal=False,
            emit=True,  # a second queued send (the batched events) follows the root
        )
    assert transport.attempts == 1  # a broken transport is not re-tried; the queue drains unsent
