"""``DaskBackend`` (plan §1.3.2): a :class:`SubmitBackend` over a READY ``distributed.Client`` used
as a dumb scheduler. Cluster construction (LocalCluster, SLURMCluster, LPCCondorCluster) is
out-of-band user code — the verified ecosystem seam is "produce a Client, consume a Client".

This module imports NOTHING from distributed at top level (only ``_lazy._distributed`` and, at
construction time, ``_shim``), so ``graphed_executors.dask_backend`` imports leave ``distributed``
out of ``sys.modules`` until a ``DaskBackend`` is actually built (``test_submit_no_dask_import``).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, cast

from graphed_executors.submit import SubmitCapabilities, SubmitRunner
from graphed_executors.submit.engine import _identity
from graphed_executors.submit.protocol import SubmitFuture

from ._lazy import _distributed


class DaskBackend:
    """A dask.distributed :class:`SubmitBackend`. Every submit is ``pure=False`` with an explicit
    namespaced+nonced key (graphed's leaves are distinct-byte-range I/O reads and opaque
    evaluations — exactly the class dask must NOT dedup by content tokenization). Payloads ship
    once via an identity future (the coffea idiom, over ``scatter`` which breaks on scale-to-zero)."""

    # The flags describe what the underlying dask LIBRARY supports, not what this MVP adapter wires:
    # per_task_resources/pin_to_worker are real dask features (resources=/workers=), but `submit`
    # does NOT auto-forward `resources` (an unsatisfiable hard constraint would stall the task in
    # no-worker state — see `submit`). Enforcement is a future deployment-time opt-in.
    capabilities = SubmitCapabilities(
        peer_data_movement=True,
        scatter_broadcast=True,
        pin_to_worker=True,
        per_task_retries=True,
        per_task_resources=True,
        cancel_running=True,
        worker_file_cache=False,  # honest: no ColumnCache-style plugin ships in the MVP
    )

    def __init__(self, client: Any, *, replicate_broadcast: bool = False) -> None:
        self._distributed = _distributed()  # raises the hinted ImportError if the extra is absent
        self._client = client
        self._replicate_broadcast = replicate_broadcast
        from ._shim import _dask_task_shim  # noqa: PLC0415  (lazy: keeps distributed out of __init__)

        self._shim = _dask_task_shim

    def n_workers(self) -> int:
        return len(self._client.scheduler_info()["workers"])

    def task_slots(self) -> int:
        """Tasks the cluster runs at once (each worker runs ``nthreads``); 0 with no worker yet."""
        return sum(self._client.nthreads().values())

    def submit(
        self,
        fn: Any,
        /,
        *args: Any,
        key: str,
        retries: int = 0,
        priority: int = 0,
        resources: Mapping[str, float] | None = None,
        workers: Sequence[str] | None = None,
    ) -> SubmitFuture:
        """``retries`` and ``priority`` ARE forwarded to ``client.submit``; ``resources`` is accepted
        but **advisory-dropped** (NOT forwarded). dask treats ``resources=`` as a HARD scheduling
        constraint, so a request for a resource no worker advertises (e.g. ``{"GPU": 1}``) pins the
        task in no-worker state forever — that would make correctness depend on a hint, which §1.1
        forbids. Enforcement on a resource-provisioned cluster is a future deployment-time opt-in
        (e.g. a ``DaskBackend(enforce_resources=True)`` knob), recorded for m43/Phase-2.

        ``workers`` (m44 §1.2) is a STRICT worker pin honoured only when ``pin_to_worker`` is true —
        forwarded as ``workers=[…], allow_other_workers=False`` so the task ERRORS on worker loss
        rather than migrating (the loud signal the epoch restart needs). Advisory-dropped otherwise,
        mirroring the ``resources`` treatment."""
        # Wrap in the worker shim so the neutral engine task fn runs under a WorkerEnv (§1.1). dask
        # resolves future args before invoking the shim, so wrapping is transparent to dependency
        # resolution; pure=False + the explicit key ARE the task identity (§1.2.3).
        pin: dict[str, Any] = {}
        if workers is not None and self.capabilities.pin_to_worker:
            pin = {"workers": list(workers), "allow_other_workers": False}
        return cast(
            "SubmitFuture",
            self._client.submit(
                self._shim, fn, *args, key=key, pure=False, retries=retries, priority=priority, **pin
            ),
        )

    def broadcast(self, payload: bytes, *, token: str) -> object:
        # An identity future placed once and referenced by many tasks (coffea pattern) — chosen over
        # client.scatter (hardcoded worker-discovery timeout breaks scale-to-zero) and over
        # closure-per-task (serializes the payload per task, ~20x slowdown). token already carries
        # the run nonce, so the key is unique per run.
        future = self._client.submit(_identity, payload, key=f"graphed-bcast-{token}", pure=False)
        if self._replicate_broadcast:  # coffea#1490 escape hatch (§1.2.2 mitigation 2)
            self._client.replicate(future)
        return future

    def subscribe_events(self, topic: str, handler: Any) -> Any:
        client = self._client

        def _wrapped(event: Any) -> None:
            _timestamp, msg = event  # dask delivers (timestamp, msg); msg is the worker's list[dict]
            handler(msg)

        client.subscribe_topic(topic, _wrapped)

        def _unsub() -> None:
            client.unsubscribe_topic(topic)

        return _unsub

    def cancel(self, futures: Sequence[Any]) -> None:
        futures = list(futures)
        if futures:  # client.cancel([]) is fine, but skip the round-trip
            self._client.cancel(futures)

    def close(self) -> None:
        return None  # the caller owns the client; closing it here would break a shared cluster

    def describe_failure(self, exc: BaseException) -> tuple[str, str] | None:
        """Recognize a ``KilledWorker`` and return ``(failing_task_key, last_worker_address)`` so the
        engine can raise an attributed :class:`StageError` without importing distributed (§1.4).
        Everything else returns ``None`` — the engine re-raises it intact."""
        if isinstance(exc, self._distributed.KilledWorker):
            last_worker = exc.last_worker
            address = getattr(last_worker, "address", None) or str(last_worker)
            return (str(exc.task), str(address))
        return None


def dask_runner(
    client: Any,
    *,
    monitor: Any = None,
    retries: int = 3,
    replicate_broadcast: bool = False,
    max_in_flight: int = 2,
) -> SubmitRunner:
    """The user-facing one-liner: register the per-worker plugin on ``client`` and return a
    :class:`SubmitRunner` over a :class:`DaskBackend`. ``close()`` does NOT close the caller's client.
    ``retries`` is forwarded to dask (per-task); a Plan's per-task ``resources`` hints, if any, are
    advisory only — this adapter does not enforce them yet (see :meth:`DaskBackend.submit`)."""
    from .plugin import GraphedWorkerPlugin  # noqa: PLC0415  (lazy: distributed is the optional extra)

    client.register_plugin(GraphedWorkerPlugin())
    backend = DaskBackend(client, replicate_broadcast=replicate_broadcast)
    return SubmitRunner(backend, monitor=monitor, retries=retries, max_in_flight=max_in_flight)
