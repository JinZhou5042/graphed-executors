"""The generic Plan engine ``SubmitRunner`` over any :class:`SubmitBackend` (plan §1.2).

One engine, N backends — the same factorization as the generic shuffle engine over
``ShuffleBackend``. The fixed path mirrors the local reduction topology EXACTLY: leaves are
``process`` tasks, combines follow the ``plan_tree`` shape as future dependencies, and the driver
waits on the single root future (so bit-for-bit equality vs ``SequentialRunner`` is inherited, not
re-derived). The adaptive path folds completions with ``running_fold`` and cancels outstanding work
on stop. Both are reused from ``graphed_executors.local._reduce`` — no duplication.

The worker seam (plan §1.1, review r1 B1): per-run state travels as a picklable :class:`RunContext`
first argument; per-worker capability (``open_once`` resources + the event transport) arrives via a
:class:`WorkerEnv` a BACKEND installs through its own module-level shim — never via an import here,
so ``submit/`` names dask nowhere and ``test_submit_no_dask_import`` holds.
"""

from __future__ import annotations

import contextvars
import hashlib
import pickle
import queue
import time
import uuid
from collections import OrderedDict
from collections.abc import Callable
from concurrent.futures import Future
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, TypeVar, cast

from graphed.core import ExecContext, ExecResult, Plan, StopReason, Task
from graphed.core.execution import (
    LocalResources,
    Monitor,
    TaskEvent,
    TaskPhase,
    WorkerResources,
    emit_task,
    partition_label,
)
from graphed.debug import SourceFrame, StageError

from graphed_executors._plan_queue import PlanQueue
from graphed_executors.local._reduce import plan_tree, running_fold

from .protocol import SubmitBackend, SubmitFuture

if TYPE_CHECKING:
    from graphed.core import Partition

R = TypeVar("R")
_MISSING = object()
_DRAIN_TIMEOUT_S = 30.0  # bounded wait for trailing worker events before unsubscribing (off-path)


# ---- the worker seam: RunContext (picklable arg) + WorkerEnv (backend-installed contextvar) ------


@dataclass(frozen=True)
class RunContext:
    """Per-run state, shipped as the first arg to every task fn (stdlib-picklable)."""

    run_nonce: str
    monitor_topic: str | None  # f"graphed-monitor-{run_nonce}" when a monitor is attached, else None
    profiler_payload: bytes | None  # pickled worker_profiler_factory (small by contract), else None


class WorkerEnv(Protocol):
    """Per-worker capability the backend installs. ``resources`` backs ``open_once``; ``worker`` is
    the backend's identity for this worker (a dask address / a thread name); ``emit`` ships event
    dicts on the run's monitor topic (swallow-on-error — telemetry never breaks a run)."""

    resources: WorkerResources
    worker: str

    def emit(self, topic: str, events: list[dict[str, object]]) -> None: ...


class _DefaultEnv:
    """The process-global fallback env for a task run outside any backend shim (e.g. a bare
    ``process`` call): a shared :class:`LocalResources` and a no-op emit."""

    def __init__(self) -> None:
        self.resources: WorkerResources = LocalResources()
        self.worker = "local"

    def emit(self, topic: str, events: list[dict[str, object]]) -> None:
        return None


_ENV: contextvars.ContextVar[WorkerEnv | None] = contextvars.ContextVar("graphed_worker_env", default=None)
_DEFAULT_ENV = _DefaultEnv()


def set_worker_env(env: WorkerEnv) -> contextvars.Token[WorkerEnv | None]:
    """Install ``env`` as the current worker env; returns a token for :func:`_reset_worker_env`."""
    return _ENV.set(env)


def _reset_worker_env(token: contextvars.Token[WorkerEnv | None]) -> None:
    _ENV.reset(token)


def current_env() -> WorkerEnv:
    """The worker env a task fn reads for ``resources``/``emit`` — the backend-installed one, or the
    process-global default when no shim wrapped the call."""
    env = _ENV.get()
    return env if env is not None else _DEFAULT_ENV


# ---- broadcast-once worker-side token cache (mirror of local ``_shared_objects``) ----------------

_PAYLOAD_CACHE: OrderedDict[str, object] = OrderedDict()
_PAYLOAD_CACHE_CAP = 32


def _shared_payload(token: str, raw: bytes) -> object:
    """Deserialize the broadcast payload ONCE per worker process (bounded FIFO cache keyed by
    ``token``) — even though the future's raw bytes are already cached by the backend per worker,
    the ``pickle.loads`` (and any ``__setstate__`` cost) must not repeat per task (dask/dask#5503)."""
    cached = _PAYLOAD_CACHE.get(token, _MISSING)
    if cached is _MISSING:
        cached = pickle.loads(raw)
        _PAYLOAD_CACHE[token] = cached
        while len(_PAYLOAD_CACHE) > _PAYLOAD_CACHE_CAP:
            _PAYLOAD_CACHE.popitem(last=False)
    return cached


# ---- module-level spawn-safe task entry points (pickled BY REFERENCE, never by value) -----------


def _identity(x: bytes) -> bytes:
    """The broadcast identity future's body: place ``x`` once, referenced by many tasks."""
    return x


def _event_dict(
    phase: TaskPhase, key: int, worker: str, label: str, n_entries: int, error: str | None
) -> dict[str, object]:
    """A msgpack-safe TaskEvent as a plain dict (worker->driver transports require msgpack, not
    pickle; ``phase`` is stringified explicitly so a StrEnum never leaks)."""
    return {
        "phase": str(phase),
        "key": key,
        "worker": worker,
        "t": time.perf_counter(),
        "partition": label,
        "n_entries": n_entries,
        "error": error,
    }


def _emit_phase(
    ctx: RunContext, env: WorkerEnv, phase: TaskPhase, task: Task, *, error: str | None = None
) -> None:
    if ctx.monitor_topic is None:  # no monitor attached -> zero telemetry cost on the task path
        return
    ev = _event_dict(
        phase, task.key, env.worker, partition_label(task.partition), task.partition.n_entries, error
    )
    env.emit(ctx.monitor_topic, [ev])


def _render_error(exc: BaseException) -> str:
    """A concise picklable summary for the dashboard (a StageError already str()s to a user-mapped
    message, so its own text is the best summary; anything else gets ``Type: message``)."""
    if isinstance(exc, StageError):
        return str(exc)
    return f"{type(exc).__name__}: {exc}"


def _leaf_task(ctx: RunContext, payload: bytes, token: str, task: Task) -> object:
    """Run one ``process`` on a worker, emitting STARTED then FINISHED/ERRORED (worker-side timing).
    ``payload`` is the resolved broadcast bytes; the token cache deserializes it once per worker."""
    process = cast("Callable[[Partition, WorkerResources], object]", _shared_payload(token, payload))
    env = current_env()
    _emit_phase(ctx, env, TaskPhase.STARTED, task)
    try:
        result = process(task.partition, env.resources)
    except BaseException as exc:
        _emit_phase(ctx, env, TaskPhase.ERRORED, task, error=_render_error(exc))
        raise
    _emit_phase(ctx, env, TaskPhase.FINISHED, task)
    return result


def _combine_task(ctx: RunContext, payload: bytes, token: str, a: object, b: object) -> object:
    """Reduce two partials on a worker. ``a``/``b`` are resolved by the backend (dask fetches them
    peer-to-peer; ThreadBackend ``.result()``\\ s them). Combines emit no task events (leaf keys only)."""
    combine = cast("Callable[[object, object], object]", _shared_payload(token, payload))
    return combine(a, b)


def _fingerprint(obj: object) -> tuple[str, bytes]:
    """(12-hex content fingerprint, pickled payload) — the M31 broadcast-token idiom."""
    payload = pickle.dumps(obj)
    return hashlib.sha256(payload).hexdigest()[:12], payload


def _key(plan_fp: str, run_nonce: str, kind: str, idx: int) -> str:
    """A namespaced, per-run-nonced, collision-proof task key (plan §1.2.3). The ``graphed-`` +
    fingerprint prefix guarantees no key collides with a user string arg (dask/dask#9969); the nonce
    makes an explicit ``key=`` re-execute across two ``run()`` calls instead of returning a cached
    future."""
    return f"graphed-{plan_fp}-{run_nonce}-{kind}-{idx}"


def _event_from_dict(d: dict[str, object]) -> TaskEvent:
    return TaskEvent(
        phase=TaskPhase(cast("str", d["phase"])),
        key=cast("int", d["key"]),
        worker=cast("str", d["worker"]),
        t=cast("float", d["t"]),
        partition=cast("str", d["partition"]),
        n_entries=cast("int", d["n_entries"]),
        error=cast("str | None", d.get("error")),
    )


def _wait_until(predicate: Callable[[], bool], timeout_s: float) -> None:
    """Bounded off-path poll (never an assertion): drain trailing worker events before unsubscribe."""
    deadline = time.monotonic() + timeout_s
    while not predicate() and time.monotonic() < deadline:
        time.sleep(0.005)


class SubmitRunner:
    """A :class:`graphed.core.Executor` over any :class:`SubmitBackend`. ``run`` dispatches to the
    adaptive path when the plan carries ``next_tasks``, else the fixed ``plan_tree`` future graph."""

    def __init__(
        self,
        backend: SubmitBackend,
        *,
        monitor: Monitor | None = None,
        retries: int = 3,
        max_in_flight: int = 2,
    ) -> None:
        self._plans = PlanQueue(self.run, max_in_flight)
        self.backend = backend
        self.monitor = monitor  # read once at each run's start (Dashboard.attach assigns it)
        self._retries = retries

    def __enter__(self) -> SubmitRunner:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @property
    def max_in_flight(self) -> int:
        return self._plans.max_in_flight

    def submit(self, plan: Plan[R]) -> Future[ExecResult[R]]:
        """Queue ``plan`` and return a future of its :meth:`run` result without waiting for it.
        Plans run one at a time in submit order; this blocks while ``max_in_flight`` submitted plans
        are unfinished."""
        return self._plans.submit(plan)

    def close(self) -> None:
        """Finish every submitted plan, then close the backend."""
        self._plans.close()
        self.backend.close()

    def run(self, plan: Plan[R]) -> ExecResult[R]:
        run_nonce = uuid.uuid4().hex[:8]
        monitor = self.monitor
        monitor_topic = f"graphed-monitor-{run_nonce}" if monitor is not None else None
        events_seen = [0]
        unsub: Callable[[], None] | None = None
        if monitor is not None and monitor_topic is not None:

            def handler(events: list[dict[str, object]]) -> None:
                events_seen[0] += len(events)
                for d in events:
                    emit_task(monitor, _event_from_dict(d))  # swallows a raising monitor (passivity)

            unsub = self.backend.subscribe_events(monitor_topic, handler)
        try:
            if plan.next_tasks is not None:
                return self._run_adaptive(plan, monitor, run_nonce, monitor_topic, events_seen)
            return self._run_fixed(plan, monitor, run_nonce, monitor_topic, events_seen)
        finally:
            if unsub is not None:
                unsub()

    # ---- fixed path: plan_tree as the future graph (plan §1.2.1) ----

    def _run_fixed(
        self,
        plan: Plan[R],
        monitor: Monitor | None,
        run_nonce: str,
        monitor_topic: str | None,
        events_seen: list[int],
    ) -> ExecResult[R]:
        backend = self.backend
        tasks = sorted(plan.tasks, key=lambda t: t.key)  # deterministic leaf order
        n = len(tasks)
        if n == 0:
            return ExecResult(plan.empty(), 0, 0, StopReason.EXHAUSTED)
        plan_fp, ppayload = _fingerprint(plan.process)
        ctx = RunContext(run_nonce, monitor_topic, self._profiler_payload(monitor))
        ptoken = f"{plan_fp}-{run_nonce}"
        phandle = backend.broadcast(ppayload, token=ptoken)
        key_to_task: dict[str, Task] = {}
        futs: dict[int, SubmitFuture] = {}
        try:
            for i, task in enumerate(tasks):
                if monitor is not None and monitor_topic is not None:
                    emit_task(monitor, self._submitted_event(task))  # driver-side SUBMITTED (leaves only)
                key = _key(plan_fp, run_nonce, "leaf", i)
                key_to_task[key] = task
                futs[i] = backend.submit(
                    _leaf_task, ctx, phandle, ptoken, task, key=key, retries=self._retries
                )
            combines, root = plan_tree(n)
            assert root is not None  # n >= 1
            _cfp, cpayload = _fingerprint(plan.combine)
            ctoken = f"{_cfp}-{run_nonce}"
            chandle = backend.broadcast(cpayload, token=ctoken)
            for out, a, b in combines:  # a < b: deterministic left/right, the plan_tree shape
                key = _key(plan_fp, run_nonce, "combine", out)
                futs[out] = backend.submit(
                    _combine_task, ctx, chandle, ctoken, futs[a], futs[b], key=key, retries=self._retries
                )
            value = cast(R, self._result(futs[root], key_to_task))
            return ExecResult(value, n, len(combines), StopReason.EXHAUSTED)
        finally:
            if monitor_topic is not None:  # drain trailing worker events (2 per leaf) before unsubscribe
                _wait_until(lambda: events_seen[0] >= 2 * n, _DRAIN_TIMEOUT_S)

    # ---- adaptive path + stop (plan §1.2.4) ----

    def _run_adaptive(
        self,
        plan: Plan[R],
        monitor: Monitor | None,
        run_nonce: str,
        monitor_topic: str | None,
        events_seen: list[int],
    ) -> ExecResult[R]:
        assert plan.next_tasks is not None
        backend = self.backend
        next_tasks = plan.next_tasks
        plan_fp, ppayload = _fingerprint(plan.process)
        ctx = RunContext(run_nonce, monitor_topic, self._profiler_payload(monitor))
        ptoken = f"{plan_fp}-{run_nonce}"
        phandle = backend.broadcast(ppayload, token=ptoken)
        exec_ctx = ExecContext()
        done_q: queue.Queue[SubmitFuture] = queue.Queue()
        outstanding: dict[SubmitFuture, tuple[int, int]] = {}  # future -> (task.key, n_entries)
        key_to_task: dict[str, Task] = {}  # F2b: dask key -> Task, so a worker death names the partition
        results: list[tuple[int, R]] = []
        stopped: StopReason | None = None
        seq = 0

        def refill() -> None:
            nonlocal seq
            batch = next_tasks(exec_ctx)  # DONE == None
            if not batch:
                return
            for task in batch:
                if monitor is not None and monitor_topic is not None:
                    emit_task(monitor, self._submitted_event(task))
                dask_key = _key(plan_fp, run_nonce, "leaf", seq)
                key_to_task[dask_key] = task  # F2b: attribute a KilledWorker to this chunk (not the key)
                fut = backend.submit(
                    _leaf_task, ctx, phandle, ptoken, task, key=dask_key, retries=self._retries
                )
                seq += 1
                outstanding[fut] = (task.key, task.partition.n_entries)
                fut.add_done_callback(done_q.put)  # backend-neutral as_completed (queue.Queue)

        try:
            refill()
            while outstanding:
                fut = done_q.get()
                if fut not in outstanding:  # a cancelled/duplicate callback
                    continue
                key, n_entries = outstanding.pop(fut)
                results.append((key, cast(R, self._result(fut, key_to_task))))  # F2b: real map, not {}
                exec_ctx.n_done += 1
                exec_ctx.events_done += n_entries
                reason = plan.stop.reason(exec_ctx) if plan.stop else None
                if reason is not None:
                    stopped = reason
                    backend.cancel(list(outstanding))  # best-effort; cancel_running=False backend no-ops
                    break
                refill()

            value, n_combines = running_fold(iter(results), plan.combine, plan.empty)
            return ExecResult(value, exec_ctx.n_done, n_combines, stopped or StopReason.EXHAUSTED)
        finally:
            if monitor_topic is not None:  # F2a: drain trailing worker events (2 per consumed leaf)
                _wait_until(lambda: events_seen[0] >= 2 * exec_ctx.n_done, _DRAIN_TIMEOUT_S)

    # ---- helpers ----

    def _result(self, fut: SubmitFuture, key_to_task: dict[str, Task]) -> object:
        """Resolve a future, translating a backend worker-death signal (``KilledWorker``) into an
        attributed :class:`StageError` (plan §1.4). Ordinary exceptions re-raise intact."""
        try:
            return fut.result()
        except BaseException as exc:
            translated = self._translate(exc, key_to_task)
            if translated is not None:
                raise translated from exc
            raise

    def _translate(self, exc: BaseException, key_to_task: dict[str, Task]) -> StageError | None:
        describe = getattr(self.backend, "describe_failure", None)
        if describe is None:
            return None
        info = describe(exc)
        if info is None:
            return None
        failing_key, last_worker = info
        task = key_to_task.get(failing_key)
        partition = partition_label(task.partition) if task is not None else str(failing_key)
        frame = SourceFrame(filename=task.partition.uri if task is not None else str(failing_key), lineno=0)
        return StageError(
            op="run",
            frames=(frame,),
            input_forms=(),
            partition=partition,
            cause_type="KilledWorker",
            cause_message=(
                f"worker {last_worker} died (segfault/OOM/preemption suspected; note: blame can be "
                f"unfair under co-located tasks)"
            ),
            opt_level=0,
        )

    def _submitted_event(self, task: Task) -> TaskEvent:
        return TaskEvent(
            TaskPhase.SUBMITTED,
            task.key,
            "driver",
            time.perf_counter(),
            partition_label(task.partition),
            task.partition.n_entries,
        )

    def _profiler_payload(self, monitor: Monitor | None) -> bytes | None:
        # Worker-side statistical sampling is not wired for the dask backend in m42 (no frozen test
        # exercises on_profile); the RunContext field is populated per the §1.1 contract so the seam
        # is ready, but the shim does not start a profiler. ponytail: wire when a profile test lands.
        if monitor is None:
            return None
        factory = monitor.worker_profiler_factory()
        return pickle.dumps(factory) if factory is not None else None
