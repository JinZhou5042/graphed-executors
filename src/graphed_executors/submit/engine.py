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

A submission's scope is its run (plan-services §3.1): ``run`` opens one ``ExitStack`` and registers on
it everything the submission acquires — the plan's :class:`ServiceSet` (``submit/services.py``) when
``plan.services`` is non-empty, then a ``cancel`` of its plan tasks not yet done — so no service,
record or queued task of a run outlives it or reaches another. The body runs on the plan with the
run's endpoints bound (graphed's ``bind_services``, before the first plan-task submit), and the value
is resolved against the plan while its services are still up.
"""

from __future__ import annotations

import contextlib
import contextvars
import hashlib
import pickle
import queue
import threading
import time
import uuid
from collections import OrderedDict
from collections.abc import Callable, Mapping
from concurrent.futures import Future
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, cast, overload

import cloudpickle
from graphed import services as graphed_services
from graphed.core import (
    DurablePlanV2,
    ExecContext,
    ExecResult,
    Plan,
    RunControl,
    RunState,
    StopReason,
    Task,
)
from graphed.core.execution import (
    LocalResources,
    Monitor,
    TaskEvent,
    TaskPhase,
    WorkerResources,
    emit_task,
    lean_events,
    partition_label,
    worker_monitor_factory,
)
from graphed.debug import SourceFrame, StageError
from graphed.shuffle import pick, split

from graphed_executors._plan_queue import PlanQueue
from graphed_executors.local._reduce import plan_tree, running_fold
from graphed_executors.local.executors import _PAUSED_WAKE_S, _wait_until, _Window

from .protocol import SubmitBackend, SubmitFuture
from .services import ServiceSet

if TYPE_CHECKING:
    from graphed.core import Partition

R = TypeVar("R")
T = TypeVar("T")
_MISSING = object()
_DRAIN_TIMEOUT_S = 30.0  # bounded wait for trailing worker events before unsubscribing (off-path)


# ---- the worker seam: RunContext (picklable arg) + WorkerEnv (backend-installed contextvar) ------


@dataclass(frozen=True)
class RunContext:
    """Per-run state, shipped as the first arg to every task fn (stdlib-picklable)."""

    run_nonce: str
    monitor_topic: str | None  # f"graphed-monitor-{run_nonce}" when a monitor is attached, else None
    profiler_payload: bytes | None  # pickled worker_profiler_factory (small by contract), else None
    monitor_factory: bytes | None = None  # pickled worker_monitor_factory: workers push, not the topic
    lean: bool = False  # lean events: no STARTED, and an unlabelled terminal event

    @property
    def events_per_leaf(self) -> int:
        """Worker events the driver's topic receives per leaf (none when workers push)."""
        return 0 if self.monitor_factory is not None else 1 if self.lean else 2


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


# One pushed monitor per worker process and shipped factory; two task threads can reach a fresh
# process's first build together, hence the lock.
_WORKER_MONITORS: dict[bytes, Monitor | None] = {}
_WORKER_MONITORS_LOCK = threading.Lock()


def _worker_monitor(payload: bytes) -> Monitor | None:
    with _WORKER_MONITORS_LOCK:
        if payload not in _WORKER_MONITORS:
            monitor: Monitor | None = None
            with contextlib.suppress(Exception):  # a monitor that won't build just disables telemetry
                monitor = pickle.loads(payload)()
            _WORKER_MONITORS[payload] = monitor
        return _WORKER_MONITORS[payload]


def _emit_phase(
    ctx: RunContext, env: WorkerEnv, phase: TaskPhase, task: Task, *, error: str | None = None
) -> None:
    if ctx.monitor_topic is None:  # no monitor attached -> zero telemetry cost on the task path
        return
    label = "" if ctx.lean else partition_label(task.partition)
    if ctx.monitor_factory is not None:
        ev = TaskEvent(
            phase, task.key, env.worker, time.perf_counter(), label, task.partition.n_entries, error=error
        )
        emit_task(_worker_monitor(ctx.monitor_factory), ev)
        return
    env.emit(
        ctx.monitor_topic, [_event_dict(phase, task.key, env.worker, label, task.partition.n_entries, error)]
    )


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
    if not ctx.lean:
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


def _stage_task(payload: bytes, token: str, task: Task, *inputs: bytes) -> bytes:
    """Run one task of a ``DurablePlanV2`` stage: ``process(task, inputs, resources)``."""
    process = cast("Callable[..., bytes]", _shared_payload(token, payload))
    return process(task, inputs, current_env().resources)


def _map_task(payload: bytes, token: str, task: Task, *inputs: bytes) -> dict[int, bytes]:
    """A ``map_write`` task, its payload split per dest so each (map, dest) edge moves one slice."""
    return split(_stage_task(payload, token, task, *inputs))


def _fingerprint(obj: object, dumps: Callable[[object], bytes] = pickle.dumps) -> tuple[str, bytes]:
    """(12-hex content fingerprint, pickled payload) — the M31 broadcast-token idiom."""
    payload = dumps(obj)
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


class _RunTasks:
    """One run's plan-task submits (the ``_RunLeaves`` idiom): only the futures not yet done are held,
    since a held dask future pins its result in cluster memory, and :meth:`cancel` cancels those."""

    def __init__(self, backend: SubmitBackend) -> None:
        self._backend = backend
        self._lock = threading.Lock()
        self._pending: set[SubmitFuture] = set()

    def submit(self, fn: Callable[..., object], /, *args: object, key: str, retries: int) -> SubmitFuture:
        fut = self._backend.submit(fn, *args, key=key, retries=retries)
        with self._lock:
            self._pending.add(fut)
        fut.add_done_callback(self._done)  # outside the lock: a done future calls back at once
        return fut

    def _done(self, fut: SubmitFuture) -> None:
        with self._lock:
            self._pending.discard(fut)

    def cancel(self) -> None:
        """Cancel the run's plan tasks not yet done (best-effort, as ``backend.cancel`` is)."""
        with self._lock:
            pending = [fut for fut in self._pending if not fut.done()]  # a done one may not have called back
        if pending:
            self._backend.cancel(pending)


class SubmitRunner:
    """A :class:`graphed.core.Executor` over any :class:`SubmitBackend`. ``run`` dispatches to the
    adaptive path when the plan carries ``next_tasks``, else the fixed ``plan_tree`` future graph.

    ``services`` (service name -> ``scheme://host:port``) are the leg-1 endpoints of every run, read
    once at each run's start like ``monitor``; a user-held :class:`ServiceSet`'s endpoints keep its
    services warm across runs."""

    def __init__(
        self,
        backend: SubmitBackend,
        *,
        monitor: Monitor | None = None,
        retries: int = 3,
        max_in_flight: int = 2,
        control: RunControl | None = None,
        services: Mapping[str, str] | None = None,
    ) -> None:
        self._plans = PlanQueue(self.run, max_in_flight)
        self.backend = backend
        self.monitor = monitor  # read once at each run's start (Dashboard.attach assigns it)
        self.control = control  # likewise
        self.services = services  # likewise
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

    @overload
    def run(self, plan: Plan[R]) -> ExecResult[R]: ...
    @overload
    def run(self, plan: DurablePlanV2) -> ExecResult[Any]: ...
    def run(self, plan: Plan[R] | DurablePlanV2) -> ExecResult[R] | ExecResult[Any]:
        """Run ``plan``. Its services are resolved, checked, probed and bound before the first plan task
        is submitted, and released, with any of its tasks not yet done, when ``run`` returns or raises.
        A :class:`~graphed.core.DurablePlanV2` (a join or repartition) runs stage by stage to
        ``plan.value`` of its last stage; a cancel returns ``value=None``."""
        monitor = self.monitor
        control = self.control
        given = self.services
        ctx = self._context(uuid.uuid4().hex[:8], monitor)
        with contextlib.ExitStack() as scope:  # the submission's scope: released in reverse on exit
            return self._run_scoped(plan, monitor, control, given, ctx, scope)

    def _run_scoped(
        self,
        plan: Plan[R] | DurablePlanV2,
        monitor: Monitor | None,
        control: RunControl | None,
        given: Mapping[str, str] | None,
        ctx: RunContext,
        scope: contextlib.ExitStack,
    ) -> ExecResult[R] | ExecResult[Any]:
        events_seen = [0]
        seen_lock = threading.Lock()  # handlers run on worker threads; += is not atomic without the GIL
        unsub: Callable[[], None] | None = None
        try:
            if control is not None and control.state is RunState.CANCELLED:
                empty = None if isinstance(plan, DurablePlanV2) else plan.empty()
                return ExecResult(empty, 0, 0, StopReason.CANCELLED)
            bound = plan
            if plan.services:
                services = ServiceSet(plan.services, self.backend, endpoints=given, scope=ctx.run_nonce)
                bound = graphed_services.bind_services(plan, scope.enter_context(services))
            submits = _RunTasks(self.backend)
            scope.callback(submits.cancel)  # registered after the services, so it runs before their release
            if ctx.monitor_topic is not None and ctx.events_per_leaf:

                def handler(events: list[dict[str, object]]) -> None:
                    with seen_lock:
                        events_seen[0] += len(events)
                    for d in events:
                        emit_task(monitor, _event_from_dict(d))  # swallows a raising monitor (passivity)

                unsub = self.backend.subscribe_events(ctx.monitor_topic, handler)
            result: ExecResult[R] | ExecResult[Any]
            if isinstance(bound, DurablePlanV2):
                result = self._run_stages(bound, control, ctx, submits)
            elif control is not None:  # the default path never meets a window
                if bound.next_tasks is not None:
                    result = self._run_adaptive_windowed(bound, monitor, control, ctx, events_seen, submits)
                else:
                    result = self._run_fixed_windowed(bound, monitor, control, ctx, events_seen, submits)
            elif bound.next_tasks is not None:
                result = self._run_adaptive(bound, monitor, ctx, events_seen, submits)
            else:
                result = self._run_fixed(bound, monitor, ctx, events_seen, submits)
            if not plan.services:
                return result
            # graphed's walk, through collate and aggregate_plan, while the services are still up
            return replace(result, value=graphed_services.resolve_services(bound, result.value))
        finally:
            if unsub is not None:
                unsub()
            if control is not None and control.state is RunState.CANCELLED:
                control.reset()  # a cancel ends this run only, whether or not a check saw it

    # ---- fixed path: plan_tree as the future graph (plan §1.2.1) ----

    def _run_fixed(
        self,
        plan: Plan[R],
        monitor: Monitor | None,
        ctx: RunContext,
        events_seen: list[int],
        submits: _RunTasks,
    ) -> ExecResult[R]:
        backend = self.backend
        tasks = sorted(plan.tasks, key=lambda t: t.key)  # deterministic leaf order
        n = len(tasks)
        if n == 0:
            return ExecResult(plan.empty(), 0, 0, StopReason.EXHAUSTED)
        plan_fp, ppayload = _fingerprint(plan.process)
        ptoken = f"{plan_fp}-{ctx.run_nonce}"
        phandle = backend.broadcast(ppayload, token=ptoken)
        key_to_task: dict[str, Task] = {}
        futs: dict[int, SubmitFuture] = {}
        try:
            for i, task in enumerate(tasks):
                if monitor is not None:
                    emit_task(monitor, self._submitted_event(task))  # driver-side SUBMITTED (leaves only)
                key = _key(plan_fp, ctx.run_nonce, "leaf", i)
                key_to_task[key] = task
                futs[i] = submits.submit(
                    _leaf_task, ctx, phandle, ptoken, task, key=key, retries=self._retries
                )
            combines, root = plan_tree(n)
            assert root is not None  # n >= 1
            _cfp, cpayload = _fingerprint(plan.combine)
            ctoken = f"{_cfp}-{ctx.run_nonce}"
            chandle = backend.broadcast(cpayload, token=ctoken)
            for out, a, b in combines:  # a < b: deterministic left/right, the plan_tree shape
                key = _key(plan_fp, ctx.run_nonce, "combine", out)
                futs[out] = submits.submit(
                    _combine_task, ctx, chandle, ctoken, futs[a], futs[b], key=key, retries=self._retries
                )
            value = cast(R, self._result(futs[root], key_to_task))
            return ExecResult(value, n, len(combines), StopReason.EXHAUSTED)
        finally:
            if ctx.monitor_topic is not None:  # drain trailing worker events before unsubscribe
                _wait_until(lambda: events_seen[0] >= ctx.events_per_leaf * n, _DRAIN_TIMEOUT_S)

    def _run_fixed_windowed(
        self,
        plan: Plan[R],
        monitor: Monitor | None,
        control: RunControl,
        ctx: RunContext,
        events_seen: list[int],
        submits: _RunTasks,
    ) -> ExecResult[R]:
        """``_run_fixed`` through a :class:`_Window`: the same ``plan_tree``, with each combine submitted
        once both inputs completed, so a cancel leaves completed subtrees to fold by first leaf."""
        backend = self.backend
        tasks = sorted(plan.tasks, key=lambda t: t.key)  # deterministic leaf order
        n = len(tasks)
        combines, _root = plan_tree(n)
        waiting: dict[int, list[int]] = {}  # input node -> combine indices needing it
        remaining: dict[int, set[int]] = {}  # combine index -> still-incomplete inputs
        first = list(range(n))  # node -> its first leaf (a combine's left input comes first)
        for ci, (_out, a, b) in enumerate(combines):
            remaining[ci] = {a, b}
            waiting.setdefault(a, []).append(ci)
            waiting.setdefault(b, []).append(ci)
            first.append(first[a])
        plan_fp, ppayload = _fingerprint(plan.process)
        ptoken = f"{plan_fp}-{ctx.run_nonce}"
        phandle = backend.broadcast(ppayload, token=ptoken)
        _cfp, cpayload = _fingerprint(plan.combine)
        ctoken = f"{_cfp}-{ctx.run_nonce}"
        chandle = backend.broadcast(cpayload, token=ctoken)
        if monitor is not None:
            for task in tasks:
                emit_task(monitor, self._submitted_event(task))
        window = _Window(control, self._task_slots(), enumerate(tasks))
        done_q: queue.Queue[SubmitFuture] = queue.Queue()
        node_of: dict[SubmitFuture, int] = {}  # outstanding future -> its plan_tree node
        ready: dict[int, SubmitFuture] = {}  # completed node -> its future, until a combine takes it
        key_to_task: dict[str, Task] = {}
        sent = done = n_combines = 0  # leaves submitted, leaves completed, combines submitted

        def start(item: tuple[int, Task]) -> None:
            nonlocal sent
            i, task = item
            key = _key(plan_fp, ctx.run_nonce, "leaf", i)
            key_to_task[key] = task
            fut = submits.submit(_leaf_task, ctx, phandle, ptoken, task, key=key, retries=self._retries)
            sent += 1
            node_of[fut] = i
            fut.add_done_callback(done_q.put)

        try:
            while True:
                if window.held:
                    self._fill(window, sent - done, start)
                if not node_of and not window.held:
                    break
                if not node_of:  # paused with nothing running
                    control.wait()
                    continue
                try:
                    fut = done_q.get(timeout=_PAUSED_WAKE_S if window.held else None)
                except queue.Empty:
                    continue
                node = node_of.pop(fut)
                if fut.exception() is not None:  # exception() transfers no result on dask
                    self._result(fut, key_to_task)  # raises it, translated
                done += node < n
                ready[node] = fut
                for ci in waiting.get(node, ()):
                    remaining[ci].discard(node)
                    if not remaining[ci]:
                        out, a, b = combines[ci]
                        fa, fb = ready.pop(a), ready.pop(b)
                        key = _key(plan_fp, ctx.run_nonce, "combine", out)
                        f2 = submits.submit(
                            _combine_task, ctx, chandle, ctoken, fa, fb, key=key, retries=self._retries
                        )
                        n_combines += 1
                        node_of[f2] = out
                        f2.add_done_callback(done_q.put)
            pieces = sorted(
                ((first[m], self._result(f, key_to_task)) for m, f in ready.items()), key=lambda p: p[0]
            )
            value, k = running_fold(iter(cast("list[tuple[int, R]]", pieces)), plan.combine, plan.empty)
            stopped = StopReason.CANCELLED if window.stopped else StopReason.EXHAUSTED
            return ExecResult(value, done, n_combines + k, stopped)
        finally:
            if ctx.monitor_topic is not None:  # drain trailing worker events of every submitted leaf
                _wait_until(lambda: events_seen[0] >= ctx.events_per_leaf * sent, _DRAIN_TIMEOUT_S)

    # ---- adaptive path + stop (plan §1.2.4) ----

    def _run_adaptive(
        self,
        plan: Plan[R],
        monitor: Monitor | None,
        ctx: RunContext,
        events_seen: list[int],
        submits: _RunTasks,
    ) -> ExecResult[R]:
        assert plan.next_tasks is not None
        backend = self.backend
        next_tasks = plan.next_tasks
        plan_fp, ppayload = _fingerprint(plan.process)
        ptoken = f"{plan_fp}-{ctx.run_nonce}"
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
                if monitor is not None:
                    emit_task(monitor, self._submitted_event(task))
                dask_key = _key(plan_fp, ctx.run_nonce, "leaf", seq)
                key_to_task[dask_key] = task  # F2b: attribute a KilledWorker to this chunk (not the key)
                fut = submits.submit(
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
            if ctx.monitor_topic is not None:  # F2a: drain trailing worker events of every consumed leaf
                _wait_until(lambda: events_seen[0] >= ctx.events_per_leaf * exec_ctx.n_done, _DRAIN_TIMEOUT_S)

    def _run_adaptive_windowed(
        self,
        plan: Plan[R],
        monitor: Monitor | None,
        control: RunControl,
        ctx: RunContext,
        events_seen: list[int],
        submits: _RunTasks,
    ) -> ExecResult[R]:
        """``_run_adaptive`` with each ``next_tasks`` batch held in a :class:`_Window`."""
        assert plan.next_tasks is not None
        backend = self.backend
        next_tasks = plan.next_tasks
        plan_fp, ppayload = _fingerprint(plan.process)
        ptoken = f"{plan_fp}-{ctx.run_nonce}"
        phandle = backend.broadcast(ppayload, token=ptoken)
        exec_ctx = ExecContext()
        done_q: queue.Queue[SubmitFuture] = queue.Queue()
        outstanding: dict[SubmitFuture, tuple[int, int]] = {}  # future -> (task.key, n_entries)
        key_to_task: dict[str, Task] = {}  # F2b: dask key -> Task, so a worker death names the partition
        results: list[tuple[int, R]] = []
        stopped: StopReason | None = None
        seq = 0
        window: _Window[Task] = _Window(control, self._task_slots())

        def refill() -> None:
            batch = next_tasks(exec_ctx)  # DONE == None
            if not batch:
                return
            for task in batch:
                if monitor is not None:
                    emit_task(monitor, self._submitted_event(task))
            window.held.extend(batch)

        def start(task: Task) -> None:
            nonlocal seq
            dask_key = _key(plan_fp, ctx.run_nonce, "leaf", seq)
            key_to_task[dask_key] = task  # F2b: attribute a KilledWorker to this chunk (not the key)
            fut = submits.submit(_leaf_task, ctx, phandle, ptoken, task, key=dask_key, retries=self._retries)
            seq += 1
            outstanding[fut] = (task.key, task.partition.n_entries)
            fut.add_done_callback(done_q.put)  # backend-neutral as_completed (queue.Queue)

        try:
            refill()
            while True:
                if window.held:  # an empty window has nothing to start or drop
                    self._fill(window, len(outstanding), start)
                if not outstanding and not window.held:
                    break
                if not outstanding:  # paused with nothing running
                    control.wait()
                    continue
                try:
                    fut = done_q.get(timeout=_PAUSED_WAKE_S if window.held else None)
                except queue.Empty:  # a timed wake only re-reads the slots and re-runs take
                    continue
                if fut not in outstanding:  # a cancelled/duplicate callback
                    continue
                key, n_entries = outstanding.pop(fut)
                results.append((key, cast(R, self._result(fut, key_to_task))))  # F2b: real map, not {}
                exec_ctx.n_done += 1
                exec_ctx.events_done += n_entries
                if window.cancelled():  # then no next_tasks and no stop exit
                    continue
                reason = plan.stop.reason(exec_ctx) if plan.stop else None
                if reason is not None:
                    stopped = reason
                    backend.cancel(list(outstanding))  # best-effort; cancel_running=False backend no-ops
                    break
                refill()

            value, n_combines = running_fold(iter(results), plan.combine, plan.empty)
            stopped = stopped or (StopReason.CANCELLED if window.stopped else StopReason.EXHAUSTED)
            return ExecResult(value, exec_ctx.n_done, n_combines, stopped)
        finally:
            if ctx.monitor_topic is not None:  # F2a: drain trailing worker events of every consumed leaf
                _wait_until(lambda: events_seen[0] >= ctx.events_per_leaf * exec_ctx.n_done, _DRAIN_TIMEOUT_S)

    # ---- staged path: a DurablePlanV2's stages as barriers ----

    def _run_stages(
        self, plan: DurablePlanV2, control: RunControl | None, ctx: RunContext, submits: _RunTasks
    ) -> ExecResult[Any]:
        """Submit each stage once the stages it reads have finished. A ``map_write`` task's payload
        reaches each gather as that gather's slice alone: picked in a task beside it where the backend
        moves data between workers, else picked here from its one fetch."""
        backend = self.backend
        peer = backend.capabilities.peer_data_movement
        plan_fp = plan.ir_fingerprint()[:12]
        done_q: queue.Queue[SubmitFuture] = queue.Queue()
        outstanding: dict[SubmitFuture, bool] = {}  # stage task not yet seen done -> its stage has inputs
        key_to_task: dict[str, Task] = {}
        stage_futs: list[list[SubmitFuture]] = []
        ran = [0, 0]  # tasks of stages with no inputs, tasks of the others

        def settle(needed: list[SubmitFuture]) -> bool:
            """Wait until ``needed`` is done; False when a completion finds the run cancelled."""
            remaining = {f for f in needed if f in outstanding}
            while remaining:
                fut = done_q.get()  # each stage task completes into the queue once
                ran[outstanding.pop(fut)] += 1
                remaining.discard(fut)
                if fut.exception() is not None:  # exception() transfers no result on dask
                    self._result(fut, key_to_task)  # raises it, translated
                if control is not None and control.state is RunState.CANCELLED:
                    return False
            return True

        for s, stage in enumerate(plan.stages):
            upstream = [f for i in stage.inputs for f in stage_futs[i]]
            if not settle(upstream) or (control is not None and control.wait() is RunState.CANCELLED):
                return ExecResult(None, ran[0], ran[1], StopReason.CANCELLED)
            # by value: a resolved opaque process is a copy stdlib pickle cannot name by reference
            fp, payload = _fingerprint(stage.process.resolve(), cloudpickle.dumps)
            token = f"{fp}-{ctx.run_nonce}"
            handle = backend.broadcast(payload, token=token)
            fn = _map_task if stage.kind == "map_write" else _stage_task
            # driver edge: each map result is fetched once, then sliced per dest here
            fetched = {
                i: [cast("dict[int, bytes]", self._result(f, key_to_task)) for f in stage_futs[i]]
                for i in stage.inputs
                if plan.stages[i].kind == "map_write" and not peer
            }
            futs: list[SubmitFuture] = []
            for t, task in enumerate(stage.tasks):
                args: list[object] = []
                for i in stage.inputs:
                    if i in fetched:
                        args.extend(pick(m, task.key) for m in fetched[i])
                    elif plan.stages[i].kind == "map_write":
                        args.extend(
                            submits.submit(
                                pick,
                                f,
                                task.key,
                                key=_key(plan_fp, ctx.run_nonce, f"pick.{i}.{m}", task.key),
                                retries=self._retries,
                            )
                            for m, f in enumerate(stage_futs[i])
                        )
                    else:
                        args.extend(stage_futs[i])
                key = _key(plan_fp, ctx.run_nonce, f"{stage.kind}.{s}", t)
                key_to_task[key] = task
                fut = submits.submit(fn, handle, token, task, *args, key=key, retries=self._retries)
                outstanding[fut] = bool(stage.inputs)
                fut.add_done_callback(done_q.put)
                futs.append(fut)
            stage_futs.append(futs)
        if not settle(list(outstanding)):
            return ExecResult(None, ran[0], ran[1], StopReason.CANCELLED)
        last = [cast("bytes", self._result(f, key_to_task)) for f in stage_futs[-1]]
        return ExecResult(plan.value(last), ran[0], ran[1], StopReason.EXHAUSTED)

    # ---- helpers ----

    def _task_slots(self) -> int:
        """The controlled window's size, floored at one: ``task_slots()`` where the backend has it
        (off the pinned Protocol; it never waits), else ``n_workers()``."""
        task_slots = getattr(self.backend, "task_slots", None)
        slots = task_slots() if task_slots is not None else self.backend.n_workers()
        return max(1, int(slots))

    def _fill(self, window: _Window[T], in_flight: int, start: Callable[[T], None]) -> None:
        """Start the held work the window releases; a full window holding work re-reads the slots and
        resizes, so workers that joined since the last read are used."""
        while True:
            items = window.take(in_flight)
            for item in items:
                start(item)
            in_flight += len(items)
            if not window.held or in_flight < window.size:
                return
            size = self._task_slots()
            grew = size > window.size
            window.size = size  # follows each re-read, so a shrunk pool narrows the window too
            if not grew:
                return

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

    def _context(self, run_nonce: str, monitor: Monitor | None) -> RunContext:
        if monitor is None:
            return RunContext(run_nonce, None, None)
        push = worker_monitor_factory(monitor)
        return RunContext(
            run_nonce,
            f"graphed-monitor-{run_nonce}",
            self._profiler_payload(monitor),
            pickle.dumps(push) if push is not None else None,
            lean_events(monitor),
        )

    def _profiler_payload(self, monitor: Monitor | None) -> bytes | None:
        # Worker-side statistical sampling is not wired for the dask backend in m42 (no frozen test
        # exercises on_profile); the RunContext field is populated per the §1.1 contract so the seam
        # is ready, but the shim does not start a profiler. ponytail: wire when a profile test lands.
        if monitor is None:
            return None
        factory = monitor.worker_profiler_factory()
        return pickle.dumps(factory) if factory is not None else None
