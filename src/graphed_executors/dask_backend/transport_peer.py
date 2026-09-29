"""m44 (b)+(c): the M38 peer reduction hosted on dask workers over ``DaskWorkerTransport``
(m44-transport-plan §1.3). One long-running, strict-pinned, seceded peer task per worker runs the
UNCHANGED ``process_and_reduce`` actor loop; the driver runs the UNCHANGED ``collect_peer_root``
loop over a ``DriverTransport`` (fed by the workers' ``log_event``), always with an explicit
``timeout_s`` so a lost root can never hang it (F3: on ``TimeoutError`` the caller falls back to the
peer task's returned root).

Scheduler graph: exactly k ``_dask_peer_main`` actor tasks (leaves ride the broadcast payload —
no per-leaf futures) + O(1) control (``client.run`` broadcast/purge). Determinism is
``PeerReducer.settle``'s fixed ``(level, pos)`` ownership, independent of dask timing; the same fixed
tree makes the result bit-for-bit the ``SequentialRunner`` baseline.

This module imports the reduction kernels from ``graphed_executors.local._peer`` UNCHANGED and defers
the dask-touching ``.transport`` import into function bodies, so it imports on the dask-free matrix
(F13).
"""

from __future__ import annotations

import uuid
from typing import Any

from graphed.core.execution import Plan
from graphed.services import require_bound

from graphed_executors.local._peer import (
    collect_peer_root,
    make_bounds,
    slice_items,
    worker_outbox_addresses,
)

from ._transport_run import (
    TransportExecResult,
    TransportWitness,
    build_stage_error,
    collect_and_purge,
    is_restart_worthy,
    pick_attributable,
    require_pin,
    sorted_addresses,
)

_ROOT_TIMEOUT_S = 30.0  # generous vs a sub-second in-cluster reduction; a peer error triggers restart
_MISSING: Any = object()


def _select_root(driver_root: Any, results: dict[str, Any], missing: Any) -> Any:
    """Resolve the reduction root after a clean run. If the driver's ``collect_peer_root`` returned one
    (``driver_root is not missing``, even a genuinely-``None`` root), it is authoritative. Otherwise fall
    back to the F3 belt-and-braces: the first peer that CAPTURED a root (``has_root``). If NO peer
    captured one, RAISE a ``TransportDeliveryError`` (R2): the reduction was aborted before producing a
    root (e.g. ``collect_peer_root`` timed out and broadcast ``('done',)``, ending the actors) — returning
    ``plan.empty()`` here would silently substitute the identity value for the real answer (hard
    constraint 8). The raise is restart-worthy, so it escalates through §1.5, never a silent wrong result."""
    if driver_root is not missing:
        return driver_root
    for r in results.values():
        if r.get("has_root"):
            return r["root"]
    from .transport import TransportDeliveryError  # noqa: PLC0415  (deferred: distributed-touching)

    raise TransportDeliveryError(
        f"peer reduction completed with no captured root across {len(results)} peer(s): the driver root "
        "collection did not deliver a root and no peer future carried one (the reduction was aborted "
        "before the root — a lost/withheld root must never default to the identity value)"
    )


def _dask_peer_main(
    spec: Any,
    process: Any,
    combine: Any,
    items: Any,
    n: int,
    bounds: list[int],
    worker_addresses: tuple[str, ...],
    steal_peers: tuple[str, ...],
) -> dict[str, Any]:
    """One worker's peer actor (module-level: spawn-safe). Build the ``DaskWorkerTransport`` from the
    plugin + spec, ``secede`` (free the pool slot for the long-running loop), then run
    ``process_and_reduce`` UNCHANGED over this endpoint and this worker's owned leaf slice. Returns
    the reducer witness plus the root value the endpoint captured (the §1.1.3 belt-and-braces
    fallback: worker 0 also ships the root to the driver over ``log_event``)."""
    import distributed  # noqa: PLC0415  (worker-side only)
    from graphed.core.execution import LocalResources  # noqa: PLC0415

    from graphed_executors.local._peer import process_and_reduce  # noqa: PLC0415

    from .transport import open_endpoint  # noqa: PLC0415  (deferred: distributed-touching)

    endpoint = open_endpoint(spec)
    distributed.secede()  # research §5: free the ThreadPool slot; the worker keeps serving handlers
    counters = process_and_reduce(
        endpoint.address,
        endpoint,
        n,
        bounds,
        tuple(worker_addresses),
        process,
        combine,
        items,
        LocalResources(),
        steal=True,
        steal_peers=tuple(steal_peers),
        emit=False,
    )
    # ``has_root`` is a plain bool (survives the wire): ``endpoint.root_sent`` may be a genuinely-``None``
    # root, which the ``_NO_ROOT`` sentinel — a live object identity that does NOT survive pickling —
    # can only disambiguate here, in the endpoint's own process (R2).
    captured = endpoint.has_root
    return {
        "address": endpoint.address,
        "has_root": captured,
        "root": endpoint.root_sent if captured else None,
        "counters": counters,
    }


def transport_run_plan(
    plan: Plan[Any],
    backend: Any,
    *,
    monitor: Any = None,
    inbox_maxsize: int | None = None,
    root_timeout_s: float = _ROOT_TIMEOUT_S,
    epoch_restarts_allowed: int = 1,
) -> TransportExecResult:
    """Run ``plan`` as an M38 peer reduction hosted on ``backend`` (a ``DaskBackend``). ``monitor`` is
    accepted for signature parity but peer-mode telemetry over the transport is not wired for m44
    (emit=False) — a documented limitation. ``root_timeout_s`` bounds the driver's ``collect_peer_root``
    wait (R4; default ``_ROOT_TIMEOUT_S``) — raise it for a legitimately long reduction, since a lost
    root now RAISES rather than defaulting to the identity value (R2). Failure semantics (§1.5): an
    exhausted send / worker death / no captured root restarts the whole run under a fresh epoch up to
    ``epoch_restarts_allowed``, else surfaces as an attributed ``StageError``."""
    require_bound(plan)  # resolves no services: an unbound plan is refused before any task
    require_pin(backend)
    tasks = sorted(plan.tasks, key=lambda t: t.key)
    n = len(tasks)
    witness = TransportWitness()
    if n == 0:
        witness.epoch_nonces.append(uuid.uuid4().hex)
        return TransportExecResult(plan.empty(), 0, 0, witness)

    from .transport import (  # noqa: PLC0415  (deferred: distributed-touching)
        _ensure_run_probe,
        ensure_engine_plugins,
        make_transport_spec,
        open_driver_endpoint,
    )

    ensure_engine_plugins(backend._client)  # the shim needs graphed-worker; idempotent for transport
    addresses = sorted_addresses(backend)
    k = max(1, min(len(addresses), n))
    worker_addrs = tuple(addresses[:k])
    bounds = make_bounds(n, k)
    items = slice_items([t.partition for t in tasks], bounds, worker_addrs)
    overlay = {a: tuple(v) for a, v in worker_outbox_addresses(n, bounds, worker_addrs).items()}

    per_worker: dict[str, dict[str, int]] = {}
    last_exc: BaseException | None = None
    for attempt in range(epoch_restarts_allowed + 1):
        nonce = uuid.uuid4().hex
        witness.epoch_nonces.append(nonce)
        spec = make_transport_spec(nonce, worker_addrs, inbox_maxsize=inbox_maxsize, overlay=overlay)
        driver_ep = open_driver_endpoint(backend, spec)
        # pre-create every worker's inbox so an early boundary-node send can never be stale-rejected
        # (hard constraint 8); the peer tasks then attach to the SAME per-epoch state.
        backend._client.run(_ensure_run_probe, spec, workers=list(worker_addrs))
        futures = {
            addr: backend.submit(
                _dask_peer_main,
                spec,
                plan.process,
                plan.combine,
                items[addr],
                n,
                bounds,
                worker_addrs,
                overlay.get(addr, ()),
                key=f"graphed-transport-{nonce}-peer-{i}",
                workers=[addr],
                retries=0,
            )
            for i, addr in enumerate(worker_addrs)
        }
        root: Any = _MISSING
        try:
            root = collect_peer_root(driver_ep, plan.empty, n, timeout_s=root_timeout_s)
        except TimeoutError:
            pass  # F3: a lost root falls back to the peer future's returned root (below)
        finally:
            driver_ep.close()

        results: dict[str, Any] = {}
        errs: dict[str, BaseException] = {}
        for addr, fut in futures.items():
            try:
                results[addr] = fut.result()
            except BaseException as exc:  # harvest every peer's outcome for the restart decision
                errs[addr] = exc
        collect_and_purge(backend._client, nonce, per_worker)

        if not errs:
            try:
                root = _select_root(root, results, _MISSING)  # RAISES if no root captured (R2)
            except BaseException as exc:  # a no-root run is restart-worthy, like an exhausted send
                errs["__no_root__"] = exc
            else:
                witness.epoch_restarts = attempt
                witness.per_worker = per_worker
                return TransportExecResult(root, n, max(0, n - 1), witness)

        last_exc = pick_attributable(errs, backend)
        if not is_restart_worthy(last_exc, backend) or attempt >= epoch_restarts_allowed:
            witness.per_worker = per_worker
            raise build_stage_error(last_exc, backend) from last_exc

    witness.per_worker = per_worker  # pragma: no cover  (the loop returns or raises)
    raise build_stage_error(last_exc, backend) if last_exc is not None else RuntimeError("no epochs run")
