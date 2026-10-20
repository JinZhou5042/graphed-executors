"""``VineGraphBackend``: a :class:`SubmitBackend` over a vine_graph ``Executor``.

vine_graph (``ndcctools.taskvine.vine_graph``) runs Python calls on TaskVine workers: ``submit(fn, *args)`` returns a
future with done-callbacks, futures among the arguments become dependencies whose results move between workers (or
through the manager's vault), a lost result is recomputed, and a failed task fails its dependents with a
``TaskError`` that carries the original exception and the reason. That is the submit seam the engine needs, so this
adapter only

- runs each task fn through the worker shim (``_shim._vine_task_shim``), which installs graphed's ``WorkerEnv``;
- wraps each vine_graph future in :class:`_VineFuture`, which raises the original exception of a failed task;
- returns broadcast payloads as the bytes themselves: vine_graph serializes an argument object once however many
  tasks pass it, and stages a large one as a file that workers cache, so the bytes travel once per worker.

The caller owns the vine_graph Executor. Workers need this package, graphed, and the analysis's own dependencies in
their environment. Modules that a plan refers to by import and that workers lack go to every worker through the
Executor's ``library_files``.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any, cast

from graphed_executors.submit import SubmitCapabilities, SubmitRunner

from ._shim import _Emitted, _vine_task_shim

__all__ = ["VineGraphBackend", "vine_graph_runner"]


class _VineFuture:
    """A :class:`SubmitFuture` over a vine_graph future."""

    __slots__ = ("_future", "key")

    def __init__(self, future: Any, key: str) -> None:
        self._future = future
        self.key = key

    def result(self, timeout: float | None = None) -> object:
        from ndcctools.taskvine.vine_graph import TaskError  # noqa: PLC0415  (optional dependency)

        try:
            value = self._future.result(timeout)
        except TaskError as error:
            if error.exception is not None:
                raise error.exception from None
            error.graphed_key = self.key  # for describe_failure()
            raise
        return value.value if type(value) is _Emitted else value

    def exception(self, timeout: float | None = None) -> BaseException | None:
        # vine_graph answers without loading the task's result, which the engine asks of every finished future.
        error = self._future.exception(timeout)
        if error is None:
            return None
        if error.exception is not None:
            return cast(BaseException, error.exception)
        error.graphed_key = self.key  # for describe_failure()
        return cast(BaseException, error)

    def done(self) -> bool:
        return cast(bool, self._future.done())

    def cancel(self) -> bool:
        return cast(bool, self._future.cancel())

    def add_done_callback(self, fn: Callable[[Any], None]) -> None:
        self._future.add_done_callback(lambda _: fn(self))

    def __repr__(self) -> str:
        return f"<_VineFuture {self.key} done={self.done()}>"


class VineGraphBackend:
    """A :class:`SubmitBackend` over a vine_graph ``Executor`` (``ndcctools.taskvine.vine_graph.Executor``).

    vine_graph moves results between workers (``peer_data_movement``), serializes a broadcast payload once
    (``scatter_broadcast``), cancels running tasks with their dependents (``cancel_running``), and keeps inputs
    in each worker's cache across tasks (``worker_file_cache``). It has no worker pins, and retries and resources
    are executor-wide settings, so those hints are ignored."""

    capabilities = SubmitCapabilities(
        peer_data_movement=True,
        scatter_broadcast=True,
        pin_to_worker=False,
        per_task_retries=False,
        per_task_resources=False,
        cancel_running=True,
        worker_file_cache=True,
    )

    def __init__(self, executor: Any) -> None:
        self._executor = executor
        self._handlers: dict[str, Callable[[list[dict[str, object]]], None]] = {}

    def n_workers(self) -> int:
        return cast(int, self._executor.capacity().workers)

    def task_slots(self) -> int:
        """Tasks the workers run at once: one per core, as vine_graph's library gives each core a function slot."""
        return cast(int, self._executor.capacity().cores)

    def submit(
        self,
        fn: Callable[..., object],
        /,
        *args: object,
        key: str,
        retries: int = 0,
        priority: int = 0,
        resources: Mapping[str, float] | None = None,
        workers: Sequence[str] | None = None,
    ) -> _VineFuture:
        args = tuple(arg._future if type(arg) is _VineFuture else arg for arg in args)
        future = self._executor.submit(_vine_task_shim, fn, *args)
        if self._handlers:
            # Registered first, so a task's events reach the monitor before the engine's callbacks run.
            future.add_done_callback(self._dispatch)
        return _VineFuture(future, key)

    def broadcast(self, payload: bytes, *, token: str) -> object:
        return payload

    def subscribe_events(
        self, topic: str, handler: Callable[[list[dict[str, object]]], None]
    ) -> Callable[[], None]:
        self._handlers[topic] = handler

        def unsub() -> None:
            self._handlers.pop(topic, None)

        return unsub

    def cancel(self, futures: Sequence[Any]) -> None:
        for future in futures:
            if type(future) is _VineFuture:
                future.cancel()

    def close(self) -> None:
        pass

    def describe_failure(self, exc: BaseException) -> tuple[str, str] | None:
        """A task that vine_graph failed because the workers running it kept exiting (a crash or an OOM kill):
        ``(its key, "a TaskVine worker")``, so the engine raises an attributed StageError. Anything else: None."""
        if getattr(exc, "reason", None) == "workers" and hasattr(exc, "graphed_key"):
            return (exc.graphed_key, "a TaskVine worker")
        return None

    def _dispatch(self, future: Any) -> None:
        try:
            error = future.exception()
            value = future.result() if error is None else None
        except Exception:
            # The executor closed before the task finished, or its result cannot be loaded here: no events.
            return
        if error is None:
            events = value.events if type(value) is _Emitted else None
        elif error.source == error.task:
            # Dependents that did not run carry the same exception, whose events only its source reports.
            events = getattr(error.exception, "graphed_events", None)
        else:
            events = None
        for topic, batch in events or ():
            handler = self._handlers.get(topic)
            if handler is not None:
                handler(batch)


def vine_graph_runner(
    executor: Any,
    *,
    monitor: Any = None,
    retries: int = 3,
    max_in_flight: int = 2,
    services: Mapping[str, str] | None = None,
    control: Any = None,
) -> SubmitRunner:
    """A :class:`SubmitRunner` over a :class:`VineGraphBackend` of ``executor``. ``close()`` leaves the executor open."""
    return SubmitRunner(
        VineGraphBackend(executor),
        monitor=monitor,
        retries=retries,
        max_in_flight=max_in_flight,
        services=services,
        control=control,
    )
