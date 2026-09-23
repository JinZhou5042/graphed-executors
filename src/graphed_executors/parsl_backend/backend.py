"""``ParslBackend`` (plan §1.2): a :class:`SubmitBackend` over a **started** parsl executor used
via DIRECT executor submit — no DFK. The DFK unwraps future args on the head node anyway
(dflow.py:863-901), so it would add a global singleton + config-global retries/caching (fighting the
determinism gate) for zero capability gain; direct ``executor.submit(func, resource_specification,
*args)`` is public (executor.py:690) and measured working with three integration moves the
:mod:`~graphed_executors.parsl_backend.launch` helper encodes.

Per-INSTANCE capability vectors (plan §1.1): ``ParslBackend(HighThroughputExecutor)`` is the
all-seven-False "parsl floor" (future args resolve on the submit host, so ``peer_data_movement`` is
False even when the m47 transport plane exists — setting it True would falsely open the m43
``_require_peer`` gate to head-node routing); ``ParslBackend(ThreadPoolExecutor)`` is the
:class:`ThreadBackend` shape (``peer_data_movement=True`` alone — same-process shared memory). Any
other executor type is refused with a loud ``TypeError`` naming both verified classes.

This module imports NOTHING from parsl at load (the ``_lazy`` accessor + the parsl-free ``_shim``);
``test_parsl_no_cross_import`` pins that importing the package leaves parsl/dask/distributed out of
``sys.modules``.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any, cast

from graphed_executors.submit import SubmitCapabilities, SubmitRunner
from graphed_executors.submit.protocol import SubmitFuture

from ._lazy import _parsl_executor_types
from ._shim import EventBatch, _parsl_task_shim, dispatch_events

# The two frozen 7-field vectors (m42 pin — NO new fields, NO subclass; the m47 transport dispatch
# rides a backend-private attribute, never a capability field).
_HTEX_CAPS = SubmitCapabilities(
    peer_data_movement=False,
    scatter_broadcast=False,
    pin_to_worker=False,
    per_task_retries=False,
    per_task_resources=False,
    cancel_running=False,
    worker_file_cache=False,
)
_TPE_CAPS = SubmitCapabilities(
    peer_data_movement=True,  # same-process shared memory — the ThreadBackend precedent
    scatter_broadcast=False,
    pin_to_worker=False,
    per_task_retries=False,
    per_task_resources=False,
    cancel_running=False,
    worker_file_cache=False,
)

_N_WORKERS_WAIT_S = 30.0  # bounded wait for >=1 HTEX manager (the exec-local wait_for_workers bound)
_WORKER_DEATH_NAMES = ("WorkerLost", "ManagerLost")  # parsl death classes, matched by chain name-string


class _ParslFuture:
    """A thin :class:`SubmitFuture` over a parsl future. ``result()`` unwraps the ``(result, events)``
    shim envelope and dispatches the buffered monitor events to the backend's driver-side handler
    registry exactly once (completion-granularity piggyback); ``done``/``exception`` delegate."""

    __slots__ = ("_dispatched", "_handlers", "_raw")

    def __init__(self, raw: Any, handlers: dict[str, Callable[[list[dict[str, object]]], None]]) -> None:
        self._raw = raw
        self._handlers = handlers
        self._dispatched = False

    def result(self, timeout: float | None = None) -> object:
        result, events = cast("tuple[object, list[EventBatch]]", self._raw.result(timeout))
        if not self._dispatched:
            self._dispatched = True
            dispatch_events(events, self._handlers)
        return result

    def done(self) -> bool:
        return bool(self._raw.done())

    def exception(self, timeout: float | None = None) -> BaseException | None:
        return cast("BaseException | None", self._raw.exception(timeout))

    def add_done_callback(self, fn: Callable[[Any], None]) -> None:
        # deliver THIS adapter (not the raw future) so a backend-neutral as_completed keys on it
        self._raw.add_done_callback(lambda _raw: fn(self))

    def cancel(self) -> bool:
        return bool(self._raw.cancel())


class ParslBackend:
    """A :class:`SubmitBackend` over a direct parsl executor submit (plan §1.2)."""

    def __init__(self, executor: Any) -> None:
        htex_cls, tpe_cls = _parsl_executor_types()
        if isinstance(executor, htex_cls):
            self.capabilities = _HTEX_CAPS
            self._is_htex = True
        elif isinstance(executor, tpe_cls):
            self.capabilities = _TPE_CAPS
            self._is_htex = False
        else:
            raise TypeError(
                "ParslBackend supports a started parsl HighThroughputExecutor or ThreadPoolExecutor "
                f"(the two verified executor classes), got {type(executor).__name__!r} — TaskVine/"
                "WorkQueue are the Phase-2 extension seam (per-instance honesty means not vouching "
                "for an unverified executor's capability vector)"
            )
        self._executor = executor
        self._handlers: dict[str, Callable[[list[dict[str, object]]], None]] = {}

    @property
    def peer_transport(self) -> bool:
        """m47: True on HTEX (the §1.4 peer plane runs on distinct worker PROCESSES), False on TPE
        (driver-process threads — a 'peer exchange' there is loopback head-node routing). A
        parsl_backend-private attribute the facade gates ``shuffle_method='transport'`` on; NEVER a
        ``SubmitCapabilities`` field (the frozen 7-field vector stays unchanged)."""
        return self._is_htex

    def n_workers(self) -> int:
        if not self._is_htex:
            return int(self._executor.max_threads)
        import time  # noqa: PLC0415  (stdlib; the bounded manager-registration wait, not an assertion)

        deadline = time.monotonic() + _N_WORKERS_WAIT_S
        total = self._count_htex_workers()
        while total == 0 and time.monotonic() < deadline:
            time.sleep(0.05)
            total = self._count_htex_workers()
        if total == 0:
            raise RuntimeError(
                "no parsl managers registered after "
                f"{_N_WORKERS_WAIT_S}s — start_htex uses fixed blocks (init_blocks==min_blocks=="
                "max_blocks); check the interchange/manager launch (never a silent 0)"
            )
        return total

    def _count_htex_workers(self) -> int:
        return sum(
            int(m["worker_count"])
            for m in self._executor.connected_managers()
            if m.get("active", True) and not m.get("draining", False)
        )

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
        """Resolve any :class:`_ParslFuture` args DRIVER-side (parsl workers cannot resolve a
        driver-process future — the §1.1 head-node round-trip that makes ``peer_data_movement``
        False), then submit the concrete values wrapped in the module-level shim. ``resource_spec``
        is ``{"priority": p}`` on HTEX iff ``p != 0`` (the one key HTEX forwards, executor.py:393)
        else ``{}``; ALWAYS ``{}`` on TPE (measured: it raises ``InvalidResourceSpecification`` on
        anything else). ``key``/``retries``/``resources``/``workers`` are advisory hints this
        all-False backend silently ignores (correctness never depends on them). The shim wrap happens
        HERE, after any spy seam, so a submitted-fn-name witness records the raw ``fn`` (design-minor-3)."""
        resolved = tuple(a.result() if isinstance(a, _ParslFuture) else a for a in args)
        spec: dict[str, int] = {"priority": priority} if (self._is_htex and priority) else {}
        raw = self._executor.submit(_parsl_task_shim, spec, fn, *resolved)
        return _ParslFuture(raw, self._handlers)

    def broadcast(self, payload: bytes, *, token: str) -> object:
        # degenerate on scatter_broadcast=False (the ThreadBackend form): the task fn receives the raw
        # bytes directly; each consuming task ships its own copy through the submit host.
        return payload

    def subscribe_events(
        self, topic: str, handler: Callable[[list[dict[str, object]]], None]
    ) -> Callable[[], None]:
        self._handlers[topic] = handler

        def unsub() -> None:
            self._handlers.pop(topic, None)

        return unsub

    def cancel(self, futures: Sequence[Any]) -> None:
        for fut in futures:  # best-effort: pre-run only (cancel_running=False) — a no-op cancel([])
            cancel = getattr(fut, "cancel", None)
            if cancel is not None:
                cancel()

    def close(self) -> None:
        return None  # the caller owns the executor; the launch helper documents teardown

    def describe_failure(self, exc: BaseException) -> tuple[str, str] | None:
        """Map a parsl ``WorkerLost``/``ManagerLost`` (matched by CLASS NAME anywhere in the exception
        chain — never by parsl identity, so this module stays parsl-import-free at load) to the
        ``(failing_key, last_worker)`` string 2-tuple the engine turns into an attributed
        :class:`StageError`. Unlike a dask ``KilledWorker`` (whose ``.task`` is the graphed key that
        ``key_to_task`` resolves to a partition), a parsl worker-loss exception carries no graphed
        task key — so BOTH slots are the death message (``str(exc)``: parsl's ``WorkerLost`` /
        ``ManagerLost`` render as "…loss of worker N on host H", naming the dead worker). The engine
        then attributes to a NON-EMPTY, meaningful unit (``StageError.partition`` / ``frame.filename``
        = the death message) rather than to an empty key; an m47 worker-death driver, which knows
        which partitions the dead peer owned, layers its partition context on top. Anything unrelated
        returns ``None``."""
        seen: set[int] = set()
        node: BaseException | None = exc
        while node is not None and id(node) not in seen:
            seen.add(id(node))
            if type(node).__name__ in _WORKER_DEATH_NAMES:
                label = str(node) or type(node).__name__
                return label, label
            node = node.__cause__ if node.__cause__ is not None else node.__context__
        return None


def parsl_runner(
    executor: Any, *, monitor: Any = None, retries: int = 3, max_in_flight: int = 2
) -> SubmitRunner:
    """The user-facing one-liner: a :class:`SubmitRunner` over a :class:`ParslBackend` on a started
    parsl executor. ``close()`` does NOT shut the caller's executor down (the launch helper documents
    teardown via ``stop_htex``)."""
    return SubmitRunner(ParslBackend(executor), monitor=monitor, retries=retries, max_in_flight=max_in_flight)
