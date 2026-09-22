"""Coverage-policy rollout (ci/per-file-coverage-policy): closes `local/_pinned_pool.py` to the
per-file >=90% gate. Not frozen; may be extended or replaced by later coverage work."""

from __future__ import annotations

import multiprocessing
import queue
import signal
import sys
import time

import pytest

from graphed_executors.local._pinned_pool import PinnedProcessPool, _pinned_loop

_PP = None


def _pp_init(tag: str) -> None:
    global _PP
    _PP = tag


def _pp_echo(x: int) -> tuple[str | None, int]:
    return (_PP, x)


def _pp_sleep_boom() -> None:
    raise ValueError("boom")


def _pp_sleep(seconds: float) -> None:
    time.sleep(seconds)


class _FlakyResultQueue:
    """A result queue whose first `.put` raises (simulating a pickling failure on an unpicklable
    exception), so `_pinned_loop`'s fallback stand-in path is the only thing that reaches `.received`."""

    def __init__(self) -> None:
        self.received: list[tuple[int, bool, object]] = []
        self._raise_next = True

    def put(self, item: tuple[int, bool, object]) -> None:
        if self._raise_next:
            self._raise_next = False
            raise TypeError("simulated: exc is unpicklable")
        self.received.append(item)


def test_pinned_loop_falls_back_to_runtimeerror_stand_in_when_exception_is_unpicklable() -> None:
    call_q: queue.Queue[tuple[int, object, tuple[object, ...]] | None] = queue.Queue()
    call_q.put((1, _pp_sleep_boom, ()))
    call_q.put(None)  # shutdown sentinel
    result_q = _FlakyResultQueue()

    _pinned_loop(lambda: None, (), call_q, result_q)

    assert len(result_q.received) == 1  # the FIRST put (the real exc) failed and isn't here
    cid, ok, value = result_q.received[0]
    assert (cid, ok) == (1, False)
    assert isinstance(value, RuntimeError)  # the stand-in, not the original ValueError
    assert str(value) == "ValueError: boom"


def test_submit_rejects_task_kwargs() -> None:
    ctx = multiprocessing.get_context("spawn")
    pool = PinnedProcessPool(1, ctx, _pp_init, [("a",)])
    try:
        with pytest.raises(TypeError, match="no keyword arguments"):
            pool.submit(_pp_echo, 1, worker=0, extra="not allowed")
    finally:
        pool.shutdown()


def test_submit_after_shutdown_raises() -> None:
    ctx = multiprocessing.get_context("spawn")
    pool = PinnedProcessPool(1, ctx, _pp_init, [("a",)])
    pool.shutdown()
    with pytest.raises(RuntimeError, match="shut-down"):
        pool.submit(_pp_echo, 1, worker=0)


def test_shutdown_is_idempotent() -> None:
    ctx = multiprocessing.get_context("spawn")
    pool = PinnedProcessPool(1, ctx, _pp_init, [("a",)])
    pool.shutdown()
    pool.shutdown()  # second call must short-circuit, not re-join/re-close already-closed queues


def test_shutdown_without_wait_terminates_still_running_workers() -> None:
    ctx = multiprocessing.get_context("spawn")
    pool = PinnedProcessPool(1, ctx, _pp_init, [("a",)])
    try:
        assert pool.workers_alive()
        # occupy the worker inside fn(*args) so it cannot reach call_q.get() and see the sentinel
        # before shutdown checks is_alive() — without this the worker can race the sentinel and exit
        # cleanly (exitcode 0), which is indistinguishable from terminate() to a bare is_alive() check.
        pool.submit(_pp_sleep, 5.0, worker=0)
        time.sleep(0.5)  # let the worker enter time.sleep before shutdown fires
        pool.shutdown(wait=False)  # skips the join loop; the worker is still mid-call -> terminate()
    finally:
        for p in pool._procs:
            p.join(timeout=10)
            assert not p.is_alive()
            if sys.platform == "win32":
                assert p.exitcode != 0  # TerminateProcess exit code, not the clean-exit 0
            else:
                assert p.exitcode == -signal.SIGTERM  # a sentinel-driven clean exit would be 0


def test_reap_tolerates_a_result_for_an_unknown_call_id() -> None:
    """An orphan `(cid, ok, value)` on the shared result queue (its Future already popped/expired)
    must not crash the reaper thread — later results still resolve normally."""
    ctx = multiprocessing.get_context("spawn")
    pool = PinnedProcessPool(1, ctx, _pp_init, [("a",)])
    try:
        pool._result.put((999, True, "orphan"))
        time.sleep(0.2)  # let the reaper thread drain the orphan before real work follows
        assert pool.submit(_pp_echo, 3, worker=0).result(timeout=15) == ("a", 3)
    finally:
        pool.shutdown()
