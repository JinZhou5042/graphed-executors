"""``ThreadBackend`` — the stdlib conformance backend (plan §1.2.5).

A ~80-line ``ThreadPoolExecutor`` :class:`SubmitBackend`: ``submit`` wraps ``fn`` to ``.result()``
any :class:`SubmitFuture` args before invoking (in-process dependency resolution), and the same
wrapper installs the :class:`WorkerEnv` (a per-thread :class:`LocalResources` + an ``emit`` that
calls the handler stashed by ``subscribe_events`` directly — no topic transport). Its flag set
differs from dask's on five of seven flags, so running the ONE frozen conformance suite against
BOTH backends proves the engine is correct across capability variation (the parsl floor) — and the
protocol/engine tests run in the main CI matrix with no dask installed.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

from graphed.core.execution import LocalResources, WorkerResources

from .engine import _reset_worker_env, set_worker_env
from .protocol import SubmitCapabilities, SubmitFuture


class _ThreadEnv:
    """A per-worker-thread :class:`WorkerEnv`: its own ``open_once`` resources and an ``emit`` that
    delivers event dicts straight to the driver handler (shared memory — no head-node copy)."""

    def __init__(self, backend: ThreadBackend) -> None:
        self.resources: WorkerResources = LocalResources()
        self.worker = threading.current_thread().name
        self._backend = backend

    def emit(self, topic: str, events: list[dict[str, object]]) -> None:
        handler = self._backend._handlers.get(topic)
        if handler is not None:
            handler(events)


class ThreadBackend:
    """A :class:`SubmitBackend` over a stdlib thread pool. ``peer_data_movement=True`` (shared
    memory), everything else ``False`` — no scatter, no pin, no per-task retries/resources, no
    running-cancel, no worker file cache."""

    capabilities = SubmitCapabilities(
        peer_data_movement=True,
        scatter_broadcast=False,
        pin_to_worker=False,
        per_task_retries=False,
        per_task_resources=False,
        cancel_running=False,
        worker_file_cache=False,
    )

    # a service beside the driver is reached by in-process workers on loopback (the service set's
    # duck-typed ``advertise_host``)
    advertise_host = "127.0.0.1"

    def __init__(self, max_workers: int = 2) -> None:
        self._max_workers = max_workers
        self._pool = ThreadPoolExecutor(max_workers=max_workers)
        self._local = threading.local()
        self._handlers: dict[str, Callable[[list[dict[str, object]]], None]] = {}

    def n_workers(self) -> int:
        return self._max_workers

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
        # `key`/`retries`/`priority`/`resources`/`workers` are best-effort hints this backend has no
        # capability for -- silently ignored (correctness never depends on them).
        return self._pool.submit(self._invoke, fn, args)

    def _invoke(self, fn: Callable[..., object], args: tuple[object, ...]) -> object:
        resolved = tuple(a.result() if isinstance(a, Future) else a for a in args)
        env = self._env()
        token = set_worker_env(env)
        try:
            return fn(*resolved)
        finally:
            _reset_worker_env(token)

    def _env(self) -> _ThreadEnv:
        env: _ThreadEnv | None = getattr(self._local, "env", None)
        if env is None:
            env = _ThreadEnv(self)
            self._local.env = env
        return env

    def broadcast(self, payload: bytes, *, token: str) -> object:
        return payload  # degenerate (shared memory): the task fn receives the raw bytes directly

    def subscribe_events(
        self, topic: str, handler: Callable[[list[dict[str, object]]], None]
    ) -> Callable[[], None]:
        self._handlers[topic] = handler

        def unsub() -> None:
            self._handlers.pop(topic, None)

        return unsub

    def cancel(self, futures: Sequence[Any]) -> None:
        for fut in futures:  # best-effort: only not-yet-started work is prevented (cancel_running=False)
            fut.cancel()

    def close(self) -> None:
        self._pool.shutdown(wait=True)
