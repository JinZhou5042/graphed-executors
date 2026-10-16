"""The common submit-backend protocol (plan §1.1).

``SubmitBackend`` names the one mechanism every parallel-execution library in the class shares:
``submit(fn, *args) -> future`` with future-valued args resolved before the callable runs (dask:
worker-side peer fetch; parsl: DFK head-node unwrap; :class:`ThreadBackend`: in-process wait). It
joins the codebase's ``*Backend`` protocol family (``ShuffleBackend``/``JoinBackend`` in
``graphed.core.execution``). This module is **pure typing** — no third-party imports — so the
protocol, the engine, and ``ThreadBackend`` install and run with the base package (no dask).
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True)
class SubmitCapabilities:
    """What **this backend instance** can do (flags are per-instance, not per-library: a parsl
    adapter over HTEX reports different flags than one over TaskVineExecutor). The engine may *use*
    a capability only behind an ``if backend.capabilities.X`` check, and the fixed/adaptive Plan
    paths must be correct with every flag ``False`` (the parsl floor)."""

    peer_data_movement: bool  # future-dep data moves worker<->worker (dask: yes; parsl/HTEX: no)
    scatter_broadcast: bool  # a payload can be placed once and referenced by many tasks
    pin_to_worker: bool  # task -> named-worker affinity (dask workers=; parsl: none)
    per_task_retries: bool  # the library retries per submit (dask retries=; parsl: DFK only)
    per_task_resources: bool  # per-task cores/mem/gpu (dask resources=; HTEX: priority only)
    cancel_running: bool  # a RUNNING task can be cancelled (dask best-effort; parsl: none)
    worker_file_cache: bool  # worker-side file caching survives across tasks (TaskVine/WQ: yes)


@runtime_checkable
class SubmitFuture(Protocol):
    """Structural — ``distributed.Future`` is NOT a ``concurrent.futures.Future`` subclass, parsl's
    ``AppFuture`` is; both satisfy this shape, as does the stdlib future ``ThreadBackend`` returns."""

    def result(self, timeout: float | None = None) -> object: ...
    def done(self) -> bool: ...
    def exception(self, timeout: float | None = None) -> BaseException | None: ...
    def add_done_callback(self, fn: Callable[[Any], None]) -> None: ...


@runtime_checkable
class SubmitBackend(Protocol):
    """The portable intersection: ``submit`` future-graph, ``broadcast`` payload placement, a
    driver-side event tap, best-effort ``cancel``, and ``close``. ``capabilities`` is per-instance."""

    capabilities: SubmitCapabilities

    def n_workers(self) -> int: ...

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
    ) -> SubmitFuture:
        """``args`` may contain :class:`SubmitFuture`\\ s previously returned by THIS backend; the
        backend guarantees ``fn`` receives their RESOLVED VALUES. ``key`` is a caller-owned unique
        identity. ``retries``/``priority``/``resources``/``workers`` are best-effort hints a backend
        without the matching capability silently ignores — correctness must never depend on them
        (``workers`` is a strict worker pin only where ``pin_to_worker`` holds)."""
        ...

    def broadcast(self, payload: bytes, *, token: str) -> object:
        """Return an arg-embeddable handle; the task fn receives the RAW BYTES on the worker. A
        ``scatter_broadcast=False`` backend may return the bytes themselves (degenerate reship)."""
        ...

    def subscribe_events(
        self, topic: str, handler: Callable[[list[dict[str, object]]], None]
    ) -> Callable[[], None]:
        """Driver-side tap for the run's monitor topic; returns an unsubscribe callable. In-process
        backends stash the handler and have their worker shim's ``emit`` call it directly."""
        ...

    def cancel(self, futures: Sequence[SubmitFuture]) -> None:
        """Best-effort; a ``cancel_running=False`` backend only prevents not-yet-started work."""
        ...

    def close(self) -> None: ...
