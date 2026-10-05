"""A worker holding stolen leaves never grants one on, so every stolen partial reaches its owner over
the pinned pool's bounded overlay."""

from __future__ import annotations

import queue
import threading
from typing import Any

from graphed.core.execution import LocalResources, Partition

from graphed_executors.local._peer import (
    DRIVER,
    lifeline_neighbors,
    make_bounds,
    process_and_reduce,
    worker_outbox_addresses,
)
from graphed_executors.local._transport import QueueTransport


class _Recording(QueueTransport):
    def __init__(self, address: str, inbox: Any, outboxes: dict[str, Any], log: list[Any]) -> None:
        super().__init__(address, inbox, outboxes)
        self._log = log

    def send(self, dest: str, message: object) -> bool:
        ok = super().send(dest, message)
        self._log.append((self.address, dest, message, ok))
        return ok


def _part(k: int) -> tuple[int, Partition]:
    return k, Partition("mem://x", "t", k, k + 1)


def test_a_thief_denies_a_steal_while_holding_only_stolen_leaves() -> None:
    n, w = 12, 4
    addrs = tuple(f"w{i}" for i in range(w))
    bounds = make_bounds(n, w)
    overlay = worker_outbox_addresses(n, bounds, addrs)
    assert "w0" not in overlay["w3"]  # w3 can only return a leaf of w0's through a direct grant

    items = {a: [_part(k) for k in range(bounds[i], bounds[i + 1])] for i, a in enumerate(addrs)}
    stolen, items["w0"] = items["w0"][-2:], items["w0"][:-2]
    own, items["w1"] = items["w1"], []
    inbox: dict[str, queue.Queue[Any]] = {a: queue.Queue() for a in (DRIVER, *addrs)}
    # w1 has settled its own leaves, then two grants from w0 land in one batch ahead of w3's steal.
    inbox["w1"].put(("w3", ("steal_req", "w3")))
    prebuffered = {
        "w1": [
            *(("w1", ("leaf", k, float(p.entry_start + 1))) for k, p in own),
            *(("w0", ("steal_resp", [leaf], (-1, g))) for g, leaf in enumerate(stolen, 1)),
        ]
    }
    log: list[Any] = []

    def run(i: int, a: str) -> None:
        process_and_reduce(
            a,
            _Recording(a, inbox[a], {t: inbox[t] for t in overlay[a]}, log),
            n,
            bounds,
            addrs,
            lambda part, _res: float(part.entry_start + 1),
            lambda x, y: x + y,
            items[a],
            LocalResources(),
            steal_peers=tuple(addrs[j] for j in lifeline_neighbors(i, w)),
            prebuffered=prebuffered.get(a, ()),
        )

    threads = [threading.Thread(target=run, args=(i, a), daemon=True) for i, a in enumerate(addrs)]
    for t in threads:
        t.start()
    root = None
    try:
        while root is None:
            _src, msg = inbox[DRIVER].get(timeout=10.0)
            if msg[0] == "root":
                root = msg[1]
    except queue.Empty:
        pass
    finally:
        for a in addrs:
            inbox[a].put((DRIVER, ("done",)))
        for t in threads:
            t.join(10.0)

    assert [e[:3] for e in log if not e[3]] == []
    assert root == sum(range(1, n + 1))
    to_w3 = [m for src, dest, m, _ok in log if (src, dest) == ("w1", "w3") and m[0] == "steal_resp"]
    assert to_w3[0][1] == []
