"""A peer worker whose partial fails to pickle raises that error at once, not the root timeout (GW34)."""

from __future__ import annotations

import pickle
import threading
import time
from typing import Any

import awkward as ak
import pytest
from _hdna_payloads import EVENTS, Events, keep_first, nothing, unpicklable_reduce
from graphed import Session
from graphed.aggregate import aggregate_plan
from graphed.awkward import AwkwardBackend, AwkwardForm, gak
from graphed.core.execution import LocalResources, Partition

import graphed_executors.local.executors as local_executors
from graphed_executors.local import ProcessPoolExecutor
from graphed_executors.local._peer import make_bounds, process_and_reduce


@pytest.mark.parametrize("comms", ["ipc", "http"])
def test_an_unpicklable_partial_surfaces_its_pickling_error_promptly(
    comms: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(local_executors, "_PEER_ROOT_TIMEOUT_S", 30.0)
    session = Session(AwkwardBackend())
    form = AwkwardForm(ak.Array(EVENTS.layout.to_typetracer(forget_length=True)))
    ev = session.source("events", form=form, data=Events(EVENTS))
    plan = aggregate_plan(
        gak.sum(ev.Jet.pt, axis=1),
        reduce=unpicklable_reduce,
        combine=keep_first,
        empty=nothing,
        steps_per_file=4,
    )
    start = time.monotonic()
    with pytest.raises(pickle.PicklingError, match="this partial does not pickle"):
        ProcessPoolExecutor(max_workers=2, comms=comms).run(plan)
    assert time.monotonic() - start < 30.0


class _SilentBrokenTransport:
    """Every send raises and no ``done`` ever arrives, as when the partial for the root never lands."""

    address = "w0"

    def send(self, dest: str, message: Any) -> bool:
        raise pickle.PicklingError("partial does not pickle")

    def poll(self) -> list[tuple[str, Any]]:
        return []

    def recv(self, timeout: float | None = None) -> tuple[str, Any] | None:
        time.sleep(timeout or 0.0)
        return None


def _one(partition: Partition, resources: LocalResources) -> int:
    return 1


def test_the_actor_stops_on_a_failed_send_without_waiting_for_done() -> None:
    raised: list[BaseException] = []

    def actor() -> None:
        try:
            process_and_reduce(
                "w0",
                _SilentBrokenTransport(),
                1,
                make_bounds(1, 1),
                ("w0",),
                _one,
                lambda a, b: a + b,
                [(0, Partition("f.root", "Events", 0, 1))],
                LocalResources(),
                steal=False,
            )
        except BaseException as exc:
            raised.append(exc)

    thread = threading.Thread(target=actor, daemon=True)
    thread.start()
    thread.join(10.0)
    assert not thread.is_alive()
    assert [type(e) for e in raised] == [pickle.PicklingError]
