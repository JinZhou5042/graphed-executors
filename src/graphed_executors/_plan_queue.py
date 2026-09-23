"""``submit(plan)``: run plans one at a time, in submit order, on a driver thread, so the caller can
record and compile the next plan while this one runs."""

from __future__ import annotations

import threading
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, TypeVar

from graphed.core import ExecResult, Plan

R = TypeVar("R")


class PlanQueue:
    """A one-thread driver in front of a host's ``run``, bounded to ``max_in_flight`` submitted and
    unfinished plans (the running one included); ``submit`` blocks at the bound."""

    def __init__(self, run: Callable[[Plan[Any]], ExecResult[Any]], max_in_flight: int) -> None:
        if max_in_flight < 1:
            raise ValueError(f"max_in_flight must be at least 1, got {max_in_flight}")
        self._run = run
        self.max_in_flight = max_in_flight
        self._slots = threading.BoundedSemaphore(max_in_flight)
        self._lock = threading.Lock()
        self._driver: ThreadPoolExecutor | None = None

    def submit(self, plan: Plan[R]) -> Future[ExecResult[R]]:
        self._slots.acquire()
        with self._lock:
            if self._driver is None:  # created lazily, and again after close()
                self._driver = ThreadPoolExecutor(1, thread_name_prefix="graphed-plan-driver")
            fut: Future[ExecResult[R]] = self._driver.submit(self._run, plan)
        # A done-callback also fires on cancel, so a cancelled queued plan frees its slot.
        fut.add_done_callback(lambda _: self._slots.release())
        return fut

    def close(self) -> None:
        """Wait for every submitted plan to finish, then release the driver thread."""
        with self._lock:
            driver, self._driver = self._driver, None
        if driver is not None:
            driver.shutdown(wait=True)
