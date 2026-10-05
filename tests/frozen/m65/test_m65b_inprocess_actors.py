"""m65 B: the peer actors push task events and profiles to the worker monitor, in lean form, and
ship neither stream to the driver. Actors run in threads by the m38 in-process patterns."""

from __future__ import annotations

import queue
import threading
import time
from collections.abc import Callable
from typing import Any

import m65b_probe as bp
import pytest
from graphed.core import Partition

import graphed_executors.local._peer as _peer
from graphed_executors.local._peer import (
    DRIVER,
    collect_peer_root,
    http_driver_handshake,
    http_peer_actor,
    ipc_peer_actor,
    make_bounds,
    peer_pool_init,
    pinned_peer_actor,
    pinned_peer_init,
    pooled_peer_actor,
    slice_items,
)
from graphed_executors.local._reduce import tree_reduce
from graphed_executors.local._transport import HttpTransport, QueueTransport


def _cat(a: str, b: str) -> str:
    return f"({a}+{b})"


def _empty() -> str:
    return "e"


def _leaf_value(part: Partition, _resources: object) -> str:
    return str(part.entry_start)


def _flat(n: int) -> str:
    return tree_reduce(n, [(i, str(i)) for i in range(n)], _cat, _empty)[0]


def _partitions(n: int) -> list[Partition]:
    return [Partition(f"f{i}", "Events", i, i + 1) for i in range(n)]


class _Tap:
    """Wraps a driver transport's ``recv`` so every payload it hands out is kept."""

    def __init__(self, transport: Any) -> None:
        self.payloads: list[Any] = []
        self._recv = transport.recv
        transport.recv = self

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        got = self._recv(*args, **kwargs)
        if got is not None:
            self.payloads.append(got[1])
        return got

    def drain(self) -> None:
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:
            if self(timeout=0.05) is None:
                return


def _kw(mon: Any) -> dict[str, Any]:
    return {"emit": True, "lean": True, "profiler_factory": bp.fake_profiler, "monitor_factory": lambda: mon}


def _run(
    n: int, driver_t: Any, actors: dict[str, Callable[[], Any]], handshake: Callable[[], None] | None = None
) -> str:
    tap = _Tap(driver_t)
    errors: list[BaseException] = []

    def guarded(fn: Callable[[], Any]) -> None:
        try:
            fn()
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=guarded, args=(fn,), daemon=True) for fn in actors.values()]
    for t in threads:
        t.start()
    for t in threads:
        t.join(0.5)
    if errors:
        raise errors[0]
    if handshake is not None:
        handshake()
    root: str = collect_peer_root(driver_t, _empty, n, timeout_s=30)
    for t in threads:
        t.join(timeout=10)
    assert not errors
    tap.drain()
    kinds = {p[0] for p in tap.payloads}
    assert "events" not in kinds
    assert "profile" not in kinds
    return root


def _ipc(n: int, w: int, mon: Any) -> str:
    addrs = tuple(f"w{i}" for i in range(w))
    inboxes: dict[str, queue.Queue[object]] = {a: queue.Queue() for a in (DRIVER, *addrs)}
    bounds = make_bounds(n, w)
    items = slice_items(_partitions(n), bounds, addrs)
    driver_t = QueueTransport(DRIVER, inboxes[DRIVER], {a: inboxes[a] for a in addrs})
    return _run(
        n,
        driver_t,
        {
            a: lambda a=a: ipc_peer_actor(
                a,
                inboxes[a],
                {p: inboxes[p] for p in (DRIVER, *addrs) if p != a},
                n,
                bounds,
                addrs,
                _leaf_value,
                _cat,
                items[a],
                **_kw(mon),
            )
            for a in addrs
        },
    )


def _http(n: int, w: int, mon: Any) -> str:
    addrs = tuple(f"w{i}" for i in range(w))
    bounds = make_bounds(n, w)
    items = slice_items(_partitions(n), bounds, addrs)
    driver_t = HttpTransport(DRIVER)
    try:
        return _run(
            n,
            driver_t,
            {
                a: lambda a=a: http_peer_actor(
                    a,
                    driver_t.host,
                    driver_t.port,
                    n,
                    bounds,
                    addrs,
                    _leaf_value,
                    _cat,
                    items[a],
                    **_kw(mon),
                )
                for a in addrs
            },
            handshake=lambda: http_driver_handshake(driver_t, addrs, timeout_s=30.0),
        )
    finally:
        driver_t.close()


def _pooled(n: int, w: int, mon: Any) -> str:
    addrs = tuple(f"w{i}" for i in range(w))
    registry: dict[str, queue.Queue[object]] = {a: queue.Queue() for a in (DRIVER, *addrs)}
    bounds = make_bounds(n, w)
    items = slice_items(_partitions(n), bounds, addrs)
    driver_t = QueueTransport(DRIVER, registry[DRIVER], {a: registry[a] for a in addrs})
    try:
        peer_pool_init(registry)
        return _run(
            n,
            driver_t,
            {
                a: lambda a=a: pooled_peer_actor(a, n, bounds, addrs, _leaf_value, _cat, items[a], **_kw(mon))
                for a in addrs
            },
        )
    finally:
        _peer._peer_registry = None


def _pinned(n: int, w: int, mon: Any) -> str:
    registry: dict[str, queue.Queue[object]] = {DRIVER: queue.Queue(), "w0": queue.Queue()}
    try:
        pinned_peer_init("w0", registry["w0"], {DRIVER: registry[DRIVER]}, ())
        driver_t = QueueTransport(DRIVER, registry[DRIVER], {"w0": registry["w0"]})
        items = [(i, p) for i, p in enumerate(_partitions(n))]
        return _run(
            n,
            driver_t,
            {
                "w0": lambda: pinned_peer_actor(
                    n,
                    make_bounds(n, 1),
                    ("w0",),
                    _leaf_value,
                    _cat,
                    items,
                    steal=False,
                    **_kw(mon),
                )
            },
        )
    finally:
        _peer._pinned = None


ACTORS: dict[str, tuple[Callable[[int, int, Any], str], int]] = {
    "ipc": (_ipc, 4),
    "http": (_http, 4),
    "pooled": (_pooled, 4),
    "pinned": (_pinned, 1),
}


@pytest.mark.parametrize("actor", ACTORS)
def test_inprocess_peer_actors_push_and_lean(actor: str, tmp_path: Any) -> None:
    n = 8
    run, w = ACTORS[actor]
    mon = bp.worker_recorder(str(tmp_path), True)
    assert run(n, w, mon) == _flat(n)
    events = [e for _, _, evs in bp.connections(str(tmp_path)) for e in evs]
    finished = [e for e in events if e["phase"] == "finished"]
    assert sorted(e["key"] for e in finished) == list(range(n))
    assert all(e["partition"] == "" for e in finished)
    assert not [e for e in events if e["phase"] == "started"]
    assert mon.profiles >= 1
