"""Cancelling parsl futures (tests/extra — NOT frozen).

``ParslBackend.cancel`` leaves HTEX futures alone: HTEX never marks a future running, so a dispatched
task's future still cancels, and its result then kills parsl's result thread with InvalidStateError,
after which no task of that executor resolves (m68a's run scope cancels its pending plan tasks, which
hung test-parsl this way). On TPE the cancel goes through. ``_ParslFuture.cancelled`` delegates, as
the frozen m68a live test's probe recorder asks it on the HTCondor backend, which shares the adapter.
"""

from __future__ import annotations

from concurrent.futures import Future
from typing import Any

import pytest

from graphed_executors.parsl_backend.backend import ParslBackend, _ParslFuture


def _backend(*, htex: bool) -> ParslBackend:
    backend = object.__new__(ParslBackend)  # no executor: cancel reads only the executor kind
    backend._is_htex = htex
    return backend


@pytest.mark.parametrize(("htex", "cancelled"), [(True, False), (False, True)], ids=["htex", "tpe"])
def test_cancel_leaves_htex_futures_and_cancels_tpe_ones(htex: bool, cancelled: bool) -> None:
    raw: Future[Any] = Future()
    _backend(htex=htex).cancel([_ParslFuture(raw, {})])
    assert raw.cancelled() is cancelled
    if not cancelled:
        raw.set_result(("done", []))  # what parsl's result thread does; it must not raise
        assert _ParslFuture(raw, {}).result() == "done"


def test_cancelled_follows_the_raw_future() -> None:
    raw: Future[Any] = Future()
    fut = _ParslFuture(raw, {})
    assert not fut.cancelled()
    seen: list[bool] = []
    fut.add_done_callback(lambda f: seen.append(f.cancelled()))
    assert fut.cancel()
    assert fut.cancelled()
    assert seen == [True]
