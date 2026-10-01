"""m47 (a): the parsl worker transport — k persistent peers over the ``EscalatingHttpTransport``
dual-route plane, driver-hosted self-rendezvous, epoch restarts (plan §1.4).

``parsl_run_plan`` hosts the UNCHANGED M38 ``process_and_reduce`` actor (review N1) on ``k`` peer
tasks. Each peer mints its ``(host, port)`` IN-TASK, announces a ``hello`` to the driver endpoint,
blocks on the barrier for the assembled registry (so no peer holds the address book before ALL k
hellos — the G5 pre-created-inbox obligation subsumed by construction), then runs its engine body.
The driver collects the root over its own endpoint and releases the peers with ``("done",)``. An
exhausted send / a lost peer restarts the WHOLE run under a fresh epoch up to
``epoch_restarts_allowed``, else an attributed ``StageError`` (never a raw parsl exception).

This module owns the shared driver primitives (barrier, registry broadcast, probe collection,
restart classification) that ``transport_shuffle`` and ``api`` reuse. It imports NOTHING from parsl
or ``dask_backend`` at load; the peer body's shuffle leg is a deferred import of
``parsl_backend.transport_shuffle`` (F13 / G9)."""

from __future__ import annotations

import contextlib
import os
import time
import uuid
from pathlib import Path
from typing import Any

from graphed.core.execution import Plan
from graphed.debug import SourceFrame, StageError
from graphed.services import require_bound

from graphed_executors.common import refuse_staged
from graphed_executors.common.http_plane import EscalatingHttpTransport
from graphed_executors.common.transport_run import (
    TransportExecResult,
    TransportWitness,
    build_stage_error,
    is_restart_worthy,
    pick_attributable,
)
from graphed_executors.local._peer import make_bounds, process_and_reduce, slice_items

DRIVER = "driver"
HOST = "127.0.0.1"  # single-machine HTEX: peers + driver share the host, so loopback is dialable
_ROOT_TIMEOUT_S = 30.0  # default bound on the driver's root wait (a peer error breaks it sooner)
_IDLE_SLACK_S = 60.0  # reduce peer lost-done backstop margin ABOVE root_timeout_s (> the 25 s send
#                       worst case + slack); the idle deadline is DERIVED, never double-hardcoded
_IDLE_DEADLINE_S = _ROOT_TIMEOUT_S + _IDLE_SLACK_S  # 90.0 at the default (parsl_run_plan re-derives)
_BARRIER_DEFAULT_S = 60.0  # production barrier bound (> the 25 s inline-send worst case)
_PROBE_TIMEOUT_S = 5.0


class ProbeUnreachable(Exception):
    """The reachability probe found an unreachable overlay pair BEFORE any data-plane traffic. Carries
    ``failed_pairs`` so the facade can name them (``on_unreachable="error"``) or fall back to relay."""

    def __init__(self, failed_pairs: list[tuple[str, str]]) -> None:
        self.failed_pairs = failed_pairs
        super().__init__(f"peer overlay unreachable: {failed_pairs}")


class _BarrierTimeout(Exception):
    """The rendezvous barrier expired before k hellos arrived (an over-ask or a dead peer)."""


def _worker_addrs(k: int) -> tuple[str, ...]:
    return tuple(f"w{i}" for i in range(k))


# ---- the peer actor (module-level: pickled BY REFERENCE, runs on an HTEX worker) -----------------
def _parsl_peer_main(spec: dict[str, Any]) -> dict[str, Any]:
    """One peer: mint the endpoint in-task, hello, barrier for the registry, then the engine body
    (reduce | shuffle) or the probe/release marker. Its RAW ``__name__`` is the SubmitBackend spy
    seam — leaves/fragments ride the spec + the pull plane, never per-leaf futures."""
    addr = spec["addr"]
    recv_failures = spec.get("recv_failures") or None
    ep = EscalatingHttpTransport(
        addr,
        epoch=spec["epoch"],
        host=HOST,
        inbox_maxsize=spec.get("inbox_maxsize"),
        idle_deadline_s=spec.get("idle_deadline_s"),
        recv_failures=recv_failures,
    )
    base: dict[str, Any] = {"endpoint_pid": os.getpid(), "endpoint_port": int(ep.port)}
    try:
        registry, prebuffered, released = _peer_rendezvous(ep, spec)
        base["registry_size_at_receipt"] = len(registry) if registry is not None else 0
        if released or registry is None:  # barrier expired / aborted: resolve without engine work
            return {
                "addr": addr,
                "has_root": False,
                "root": None,
                "counters": {**base, **_sender_counters(ep)},
            }
        kind = spec["kind"]
        if kind == "reduce":
            return _peer_reduce(ep, spec, registry, prebuffered, base)
        decision, prebuffered = _peer_probe_leg(ep, spec, registry, prebuffered)
        if kind == "probe" or decision != "proceed":
            return {
                "addr": addr,
                "has_root": False,
                "root": None,
                "counters": {**base, **_sender_counters(ep)},
            }
        from .transport_shuffle import _peer_shuffle_body  # noqa: PLC0415  (deferred; avoids a cycle)

        return _peer_shuffle_body(ep, spec, registry, prebuffered, base)
    except BaseException as exc:
        # a raising peer's counters ride the RESULT, which is now the exception — attach the
        # endpoint witnesses (send_failures/sends_retried/…) so an exhaustion in a discarded epoch
        # still reaches the driver's per_worker aggregate (BaseException.__reduce__ pickles __dict__).
        with contextlib.suppress(Exception):
            exc.peer_counters = {**base, **_sender_counters(ep)}  # type: ignore[attr-defined]
        raise
    finally:
        ep.close()


def _peer_rendezvous(
    ep: EscalatingHttpTransport, spec: dict[str, Any]
) -> tuple[dict[str, tuple[str, int]] | None, list[tuple[str, Any]], bool]:
    """Announce ``hello`` to the driver, then block (bounded) for the ``("registry", ...)`` reply,
    buffering any node/pull traffic that races ahead. Returns ``(peer_registry, prebuffered,
    released)``; ``released`` is True on an ``("abort"|"done")`` marker (the barrier expired — the
    peer resolves without doing engine work)."""
    ep.set_registry({DRIVER: (spec["driver_host"], spec["driver_port"])})
    ep.send(DRIVER, ("hello", ep.address, ep.host, ep.port, spec["epoch"]))
    prebuffered: list[tuple[str, Any]] = []
    deadline = time.monotonic() + spec.get("registry_wait_s", _BARRIER_DEFAULT_S + 30.0)
    while time.monotonic() < deadline:
        got = ep.recv(timeout=0.2)
        if got is None:
            continue
        sender, msg = got
        tag = msg[0]
        if tag == "registry":
            peer_registry: dict[str, tuple[str, int]] = msg[1]
            ep.set_registry({**peer_registry, DRIVER: (spec["driver_host"], spec["driver_port"])})
            return peer_registry, prebuffered, False
        if tag in ("abort", "done"):
            return None, prebuffered, True
        prebuffered.append((sender, msg))
    return None, prebuffered, True  # a lost registry: resolve as released (never a dangling future)


def _peer_reduce(
    ep: EscalatingHttpTransport,
    spec: dict[str, Any],
    registry: dict[str, tuple[str, int]],
    prebuffered: list[tuple[str, Any]],
    base: dict[str, Any],
) -> dict[str, Any]:
    """Run the UNCHANGED ``process_and_reduce`` over this endpoint + this peer's leaf slice (N1). The
    reducer ships boundary nodes peer→peer and the root to the driver; it drains until ``("done",)``.
    Steal is OFF (deterministic — only the boundary hand-offs cross w1→w0, which the inject seam
    targets)."""
    from graphed.core.execution import LocalResources  # noqa: PLC0415

    counters = process_and_reduce(
        ep.address,
        ep,
        spec["n"],
        spec["bounds"],
        spec["worker_addrs"],
        spec["process"],
        spec["combine"],
        spec["items"],
        LocalResources(),
        steal=False,
        emit=False,
        prebuffered=prebuffered,
    )
    # the root reaches the driver over the driver endpoint (recv_class_root); the peer returns only
    # its witness counters (the driver's captured root is authoritative for this loopback plane).
    return {"addr": ep.address, "counters": {**base, **counters, **_sender_counters(ep)}}


def _peer_probe_leg(
    ep: EscalatingHttpTransport,
    spec: dict[str, Any],
    registry: dict[str, tuple[str, int]],
    prebuffered: list[tuple[str, Any]],
) -> tuple[str, list[tuple[str, Any]]]:
    """Dial each overlay peer (a bounded connection probe), report ``("probe", addr, {peer: ok})`` to
    the driver, then block for the driver's ``("proceed"|"abort")`` verdict — buffering data traffic
    that races ahead so the engine body sees it."""
    import socket  # noqa: PLC0415

    probe_timeout = spec.get("probe_timeout_s", _PROBE_TIMEOUT_S)
    results: dict[str, bool] = {}
    for peer_addr, (host, port) in registry.items():
        if peer_addr == ep.address:
            continue
        ok = False
        try:
            with socket.create_connection((host, port), timeout=probe_timeout):
                ok = True
        except OSError:
            ok = False
        results[peer_addr] = ok
    ep.send(DRIVER, ("probe", ep.address, results))
    deadline = time.monotonic() + spec.get("registry_wait_s", _BARRIER_DEFAULT_S + 30.0)
    while time.monotonic() < deadline:
        got = ep.recv(timeout=0.2)
        if got is None:
            continue
        sender, msg = got
        if msg[0] in ("proceed", "abort", "done"):
            return ("proceed" if msg[0] == "proceed" else "abort"), prebuffered
        prebuffered.append((sender, msg))
    return "abort", prebuffered


def _sender_counters(ep: EscalatingHttpTransport) -> dict[str, int]:
    return {
        "sends_retried": int(ep.sends_retried),
        "send_failures": int(ep.send_failures),
        "sends_rejected": int(ep.sends_rejected),
        "recv_duplicate_deliveries": int(ep.recv_duplicate_deliveries),
        "stale_epoch_rejects": int(ep.stale_epoch_rejects),
    }


# ---- driver-side primitives (shared by parsl_run_plan / transport_shuffle / api.probe) -----------
def _merge_peer(per_worker: dict[str, dict[str, int]], addr: str, counters: dict[str, Any]) -> None:
    """Accumulate a peer's returned counters across epochs into ``per_worker[addr]`` — sums, ``peak_*``
    keeps the max, provenance identities keep the last non-zero."""
    acc = per_worker.setdefault(addr, {})
    for key, val in counters.items():
        if not isinstance(val, int):
            continue
        if key.startswith("peak_"):
            acc[key] = max(acc.get(key, 0), val)
        elif key in ("endpoint_pid", "endpoint_port", "registry_size_at_receipt", "serve_pid"):
            if val:
                acc[key] = val
        else:
            acc[key] = acc.get(key, 0) + val


def _bump_driver(driver_counts: dict[str, int], key: str) -> None:
    driver_counts[key] = driver_counts.get(key, 0) + 1


def _barrier_and_registry(
    driver_ep: EscalatingHttpTransport,
    futures: dict[str, Any],
    k: int,
    *,
    barrier_timeout_s: float,
    registry_rewrite: Any,
    driver_counts: dict[str, int],
) -> list[str]:
    """Collect exactly k hellos (the barrier), assemble the ``{addr: (host, port)}`` book, and
    broadcast it. The driver's OWN registry keeps the TRUE endpoints (so release/attribution reach the
    peers even under a ``registry_rewrite`` poison); the peers get the possibly-rewritten book.
    Raises ``_BarrierTimeout`` on expiry, naming ``hellos_seen/k``."""
    hellos: dict[str, tuple[str, int]] = {}
    deadline = time.monotonic() + barrier_timeout_s
    while len(hellos) < k and time.monotonic() < deadline:
        got = driver_ep.recv(timeout=0.2)
        if got is None:
            continue
        _sender, msg = got
        if msg[0] == "hello":
            _tag, addr, host, port, _epoch = msg
            hellos[addr] = (host, int(port))
            _bump_driver(driver_counts, "recv_class_hello")
    driver_ep.set_registry(dict(hellos))  # the driver's TRUE control legs
    if len(hellos) < k:
        raise _BarrierTimeout(f"rendezvous barrier expired: {len(hellos)}/{k} hellos seen")
    peer_registry = dict(hellos)
    if registry_rewrite is not None:
        peer_registry = registry_rewrite(dict(hellos))
    for addr in hellos:
        driver_ep.send(addr, ("registry", peer_registry))
    return sorted(hellos)


def _collect_probes(
    driver_ep: EscalatingHttpTransport, k: int, *, driver_counts: dict[str, int], timeout_s: float
) -> list[tuple[str, str]]:
    """Collect k probe reports and return the failed ``(src, dst)`` overlay pairs (empty iff all-ok)."""
    reports: dict[str, dict[str, bool]] = {}
    deadline = time.monotonic() + timeout_s
    while len(reports) < k and time.monotonic() < deadline:
        got = driver_ep.recv(timeout=0.2)
        if got is None:
            continue
        _sender, msg = got
        if msg[0] == "probe":
            _tag, addr, results = msg
            reports[addr] = results
            _bump_driver(driver_counts, "recv_class_probe")
    failed: list[tuple[str, str]] = []
    for src, results in reports.items():
        for dst, ok in results.items():
            if not ok:
                failed.append((src, dst))
    return sorted(failed)


def _release(driver_ep: EscalatingHttpTransport, addrs: list[str], token: tuple[Any, ...]) -> None:
    """Attempt-all release to the seated peers (one dead peer cannot strand the live ones)."""
    for addr in addrs:
        with contextlib.suppress(Exception):  # best-effort teardown; the futures resolve regardless
            driver_ep.send(addr, token)


def _cleanup_after_barrier(
    driver_ep: EscalatingHttpTransport, pbackend: Any, futures: dict[str, Any], seated: list[str]
) -> None:
    """On a barrier expiry: abort-release the seated peers so their slots free, and drain every future
    so it resolves (the §1.4 cleanup). A still-QUEUED peer is NOT cancelled — parsl's HTEX races
    cancel against dispatch (a dispatched task returning to a cancelled future raises InvalidStateError
    in parsl's result thread, which pytest escalates); instead it self-terminates via its own bounded
    rendezvous wait once a freed slot dispatches it, and the freed seated slots suffice for the caller."""
    _release(driver_ep, seated, ("abort",))
    for fut in futures.values():
        with contextlib.suppress(Exception):  # drain the seated markers (a queued peer self-terminates)
            fut.result(timeout=_ROOT_TIMEOUT_S)


def _barrier_stage_error(exc: _BarrierTimeout) -> StageError:
    return StageError(
        op="run",
        frames=(SourceFrame(filename="rendezvous-barrier", lineno=0),),
        input_forms=(),
        partition="rendezvous-barrier",
        cause_type="BarrierTimeout",
        cause_message=str(exc),
        opt_level=0,
    )


def _open_driver_endpoint(nonce: str) -> EscalatingHttpTransport:
    return EscalatingHttpTransport(DRIVER, epoch=nonce, host=HOST)


def _resolve_k(pbackend: Any, workers: int | None, *, restart: bool = False) -> int:
    """Peer count for an epoch. ``workers=None`` reads a FRESH ``n_workers()`` every attempt; an
    explicit ``workers`` is honored as-given on the first attempt but on a ``restart`` degrades to
    ``min(workers, fresh n_workers())`` (plan §1.4) — re-asking for slots a shrunken cluster can no
    longer seat would burn the restart budget on a doomed barrier."""
    if workers is None:
        return int(pbackend.n_workers())
    if restart:
        return min(int(workers), int(pbackend.n_workers()))
    return int(workers)


# ---- the reduce entry point (M38 hosted on parsl) -----------------------------------------------
def parsl_run_plan(
    plan: Plan[Any],
    pbackend: Any,
    *,
    monitor: Any = None,
    workers: int | None = None,
    inbox_maxsize: int | None = None,
    epoch_restarts_allowed: int = 1,
    barrier_timeout_s: float | None = None,
    root_timeout_s: float = _ROOT_TIMEOUT_S,
    inject_recv_failures: dict[str, dict[str, int]] | None = None,
) -> TransportExecResult:
    """Run ``plan`` as an M38 peer reduction hosted on ``pbackend`` (a ``ParslBackend`` over HTEX).
    ``monitor`` is accepted for signature parity (peer-mode telemetry is emit=False for m47). Failure
    semantics: an exhausted send / worker death / no captured root restarts the whole run under a
    fresh epoch up to ``epoch_restarts_allowed``, else an attributed ``StageError`` (§1.5).
    ``root_timeout_s`` bounds the driver's wait for the reduction root — raise it for a legitimately
    long reduction (m44 parity); the peer lost-done ``idle_deadline_s`` is DERIVED as
    ``root_timeout_s + slack`` so it always stays ≥ the root wait (§1.4)."""
    refuse_staged(plan, "parsl_run_plan", "ParslBackend(executor)")
    require_bound(plan)  # resolves no services: an unbound plan is refused before any task
    tasks = sorted(plan.tasks, key=lambda t: t.key)
    n = len(tasks)
    witness = TransportWitness()
    if n == 0:
        witness.epoch_nonces.append(uuid.uuid4().hex)
        return TransportExecResult(plan.empty(), 0, 0, witness)

    barrier_s = _BARRIER_DEFAULT_S if barrier_timeout_s is None else barrier_timeout_s
    idle_deadline_s = root_timeout_s + _IDLE_SLACK_S  # derived: always ≥ the root wait (§1.4)
    budget_dir = _write_budget_files(inject_recv_failures)
    per_worker: dict[str, dict[str, int]] = {}
    last_exc: BaseException | None = None

    for attempt in range(epoch_restarts_allowed + 1):
        nonce = uuid.uuid4().hex
        witness.epoch_nonces.append(nonce)
        k = max(1, min(_resolve_k(pbackend, workers, restart=attempt > 0), n))
        worker_addrs = _worker_addrs(k)
        bounds = make_bounds(n, k)
        items = slice_items([t.partition for t in tasks], bounds, worker_addrs)
        driver_ep = _open_driver_endpoint(nonce)
        driver_counts: dict[str, int] = {}
        futures = {
            addr: pbackend.submit(
                _parsl_peer_main,
                {
                    "kind": "reduce",
                    "addr": addr,
                    "epoch": nonce,
                    "driver_host": driver_ep.host,
                    "driver_port": driver_ep.port,
                    "worker_addrs": worker_addrs,
                    "n": n,
                    "bounds": bounds,
                    "items": items[addr],
                    "process": plan.process,
                    "combine": plan.combine,
                    "inbox_maxsize": inbox_maxsize,
                    "idle_deadline_s": idle_deadline_s,
                    "recv_failures": _peer_budget_files(budget_dir, addr, inject_recv_failures),
                },
                key=f"graphed-parsl-peer-{nonce}-{i}",
            )  # ponytail: k persistent tasks gang-schedule by slot saturation (pin_to_worker=False)
            for i, addr in enumerate(worker_addrs)
        }
        errs, root, has_captured = _drive_reduce(
            driver_ep, pbackend, futures, k, barrier_s, driver_counts, per_worker, root_timeout_s
        )
        _merge_peer(per_worker, DRIVER, driver_counts)
        driver_ep.close()

        if not errs and has_captured:
            witness.epoch_restarts = attempt
            witness.per_worker = per_worker
            return TransportExecResult(root, n, max(0, n - 1), witness)

        last_exc = _reduce_failure(errs, pbackend)
        if not is_restart_worthy(last_exc, pbackend) or attempt >= epoch_restarts_allowed:
            witness.per_worker = per_worker
            raise build_stage_error(last_exc, pbackend) from last_exc

    witness.per_worker = per_worker  # pragma: no cover  (the loop returns or raises)
    raise build_stage_error(last_exc, pbackend) if last_exc is not None else RuntimeError("no epochs run")


def _drive_reduce(
    driver_ep: EscalatingHttpTransport,
    pbackend: Any,
    futures: dict[str, Any],
    k: int,
    barrier_s: float,
    driver_counts: dict[str, int],
    per_worker: dict[str, dict[str, int]],
    root_timeout_s: float = _ROOT_TIMEOUT_S,
) -> tuple[dict[str, BaseException], Any, bool]:
    """Barrier + registry, then collect the root while watching for a fast peer error; release the
    peers and harvest their futures. Returns ``(errs, root, has_captured_root)``."""
    try:
        seated = _barrier_and_registry(
            driver_ep,
            futures,
            k,
            barrier_timeout_s=barrier_s,
            registry_rewrite=None,
            driver_counts=driver_counts,
        )
    except _BarrierTimeout as bt:
        _cleanup_after_barrier(driver_ep, pbackend, futures, sorted_addrs_seen(driver_ep))
        raise _barrier_stage_error(bt) from bt

    root: Any = None
    got_root = False
    deadline = time.monotonic() + root_timeout_s
    while not got_root and time.monotonic() < deadline:
        got = driver_ep.recv(timeout=0.1)
        if got is not None:
            _sender, msg = got
            tag = msg[0]
            if tag == "root":
                root, got_root = msg[1], True
                _bump_driver(driver_counts, "recv_class_root")
                break
            if tag in ("node", "leaf"):
                _bump_driver(driver_counts, f"recv_class_{tag}")
        if any(_future_errored(f) for f in futures.values()):
            break  # a peer errored — stop waiting for a root that will never form; restart
    _release(driver_ep, seated, ("done",))

    errs: dict[str, BaseException] = {}
    for addr, fut in futures.items():
        try:
            res = fut.result()
            _merge_peer(per_worker, addr, res["counters"])
        except BaseException as exc:  # harvest every peer's outcome for the restart decision
            errs[addr] = exc
            _merge_peer(per_worker, addr, getattr(exc, "peer_counters", {}) or {})
    return errs, root, got_root


def _future_errored(fut: Any) -> bool:
    try:
        return fut.done() and fut.exception() is not None
    except Exception:
        return False


def _reduce_failure(errs: dict[str, BaseException], pbackend: Any) -> BaseException:
    if errs:
        return pick_attributable(errs, pbackend)
    from graphed_executors.common.http_plane import TransportDeliveryError  # noqa: PLC0415

    return TransportDeliveryError(
        "peer reduction completed with no captured root (a lost/withheld root must never default to "
        "the identity value)"
    )


def sorted_addrs_seen(driver_ep: EscalatingHttpTransport) -> list[str]:
    return sorted(a for a in driver_ep._registry)


# ---- inject-seam budget files (persisted across epochs so retry+restart compose) -----------------
def _write_budget_files(inject: dict[str, dict[str, int]] | None) -> str | None:
    if not inject:
        return None
    import tempfile  # noqa: PLC0415

    d = tempfile.mkdtemp(prefix="graphed-m47-inject-")
    for dest, srcs in inject.items():
        for src, budget in srcs.items():
            Path(d, f"{dest}__{src}").write_text(str(int(budget)))
    return d


def _peer_budget_files(
    budget_dir: str | None, addr: str, inject: dict[str, dict[str, int]] | None
) -> dict[str, str] | None:
    if budget_dir is None or not inject or addr not in inject:
        return None
    return {src: str(Path(budget_dir, f"{addr}__{src}")) for src in inject[addr]}
