"""Reference single-machine executors (plan M7): a thread pool AND a process pool, both running the
same `graphed.core.Plan` to one reduced result.

Both share the driver below; they differ only in the worker pool and how per-worker `open_once`
resources are held (thread-local vs a per-process global set by an initializer). Fixed partition sets
use the deterministic straggler-tolerant tree reduction; an adaptive `next_tasks` plan uses a running
fold over a partition set discovered from observed timings. A worker failure (thread or process)
propagates to the driver intact — in particular a picklable `graphed.debug.StageError` is re-raised
as-is, never degraded to an opaque string (plan A.3 #8).
"""

from __future__ import annotations

import atexit
import contextlib
import hashlib
import multiprocessing
import os
import pickle
import queue
import sys
import threading
import time
import warnings
from collections import OrderedDict, deque
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import (
    FIRST_COMPLETED,
    Future,
    ThreadPoolExecutor,
    as_completed,
    wait,
)
from concurrent.futures import (
    Executor as _PoolExecutor,
)
from concurrent.futures import (
    ProcessPoolExecutor as _StdProcessPool,  # the stdlib pool; our public ProcessPoolExecutor wraps it
)
from multiprocessing.managers import SyncManager
from typing import Any, Generic, TypeVar, cast

from graphed.core import ExecContext, ExecResult, Partition, Plan, RunControl, RunState, StopReason, Task
from graphed.core.execution import (
    LocalResources,
    Monitor,
    TaskEvent,
    TaskPhase,
    WorkerProfiler,
    emit_task,
    partition_label,
)
from graphed.debug import StageError

from .._plan_queue import PlanQueue
from ._peer import (
    OUTBOX_EXIT_WAIT_S,
    PeerControl,
    http_driver_handshake,
    http_peer_actor,
    lifeline_neighbors,
    make_bounds,
    peer_pool_init,
    pinned_peer_actor,
    pinned_peer_init,
    pooled_peer_actor,
    process_and_reduce,
    release_workers,
    slice_items,
    worker_outbox_addresses,
)
from ._pinned_pool import PinnedProcessPool
from ._reduce import LazyReducer, plan_tree, running_fold, tree_reduce
from ._transport import HttpTransport, PipeInbox, QueueTransport, build_transports

R = TypeVar("R")
T = TypeVar("T")
# How long a peer driver waits for the root, counting only time the run is not paused.
_PEER_ROOT_TIMEOUT_S = 300.0


def _drain_queue(q: Any) -> None:
    """Discard any straggler messages left in a reused peer inbox (a clean run leaves none)."""
    with contextlib.suppress(Exception):
        while True:
            q.get_nowait()


def _close_registry(registry: dict[str, Any]) -> None:
    """Tear down a peer inbox registry (PipeInbox/SimpleQueue — no feeder thread to join)."""
    for q in registry.values():
        with contextlib.suppress(Exception):
            q.close()


def _exceeds_fd_budget(w: int) -> bool:
    """Would a full-registry IPC pool of ``w`` workers strain the per-process fd limit? Each worker in
    :class:`ProcessPoolExecutor` inherits every inbox (~``2*(w+1)`` fds), so the registry is O(N²) in
    fds; on large many-core machines (>~128 cores, or any low ``RLIMIT_NOFILE``) that approaches the
    limit. This is advisory only — it drives a warning that recommends :class:`PinnedPoolExecutor`
    (whose bounded O(log N) overlay inherits ~``log w`` fds per worker); it never switches pools
    silently. The user picks the executor explicitly."""
    if sys.platform == "win32":  # no POSIX RLIMIT; Windows fd/handle limits are high (CRT default 512)
        soft = 512
    else:
        import resource  # noqa: PLC0415 — POSIX only (sys.platform narrows this off Windows for mypy)

        soft = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
    return 2 * (w + 1) + 64 > soft // 2  # full-registry fds would exceed half the limit


# ---- per-worker resources --------------------------------------------------
_thread_local = threading.local()
_proc_resources: LocalResources | None = None
# M37: a worker->driver side channel for dashboard events (a bounded queue proxy, set by the
# process pool initializer) + a per-worker statistical profiler. Both are None unless a Monitor is
# attached. Emission is best-effort and MUST NOT block a worker (drop-on-full).
_proc_event_q: object | None = None
_proc_profiler: WorkerProfiler | None = None
_MISSING = object()

# M37 (telemetry OFF the data path): a worker emits events into a local in-process buffer (a deque
# append, ~0.1us) instead of doing a Manager().Queue() put per task (~21us IPC round-trip, ~35x
# slower). A per-worker daemon thread drains the buffer on a cadence and ships the whole batch with
# ONE Manager put, so IPC latency + batching never touch the task's critical path. The buffer is
# bounded (drops oldest if the drain can't keep up — best-effort, never blocks the worker).
_proc_buffer: deque[tuple[str, object]] | None = None
_proc_drain_stop: threading.Event | None = None
_proc_drain_thread: threading.Thread | None = None
_proc_last_flush = 0.0
_PROC_DRAIN_INTERVAL = 0.05  # seconds between worker->driver batch flushes
_PROC_BUFFER_CAP = 50000  # bounded local buffer (drop-oldest on overflow)
_PROFILE_FLUSH_INTERVAL = 1.0  # serialize the profiler at most ~1/s, never per task
# Monitored peer runs drain trailing worker events while waiting for the futures to finish; a coarse
# poll cadence here is a fixed per-run tail (the no-monitor path blocks on f.result() to avoid exactly
# that). 2 ms keeps the tail negligible (~1 % on a sub-second pass) without busy-spinning.
_PEER_MONITOR_DRAIN_POLL_S = 0.002
# A paused hub route with tasks running wakes this often, so a resume refills the free slots without
# waiting for a running task to end.
_PAUSED_WAKE_S = 0.05

# M37: every statistical profiler we start (thread-local or per-process) is registered here so a
# single atexit handler can stop it — its background sampler thread must be joined before the worker's
# interpreter tears down module globals, or a late sample touches half-collected state.
_live_profilers: list[WorkerProfiler] = []
_profiler_lock = threading.Lock()
_atexit_registered = False


def _register_profiler(profiler: WorkerProfiler) -> None:
    global _atexit_registered
    with _profiler_lock:
        _live_profilers.append(profiler)
        if not _atexit_registered:
            atexit.register(_stop_all_profilers)
            _atexit_registered = True


def _stop_all_profilers() -> None:
    with _profiler_lock:
        for profiler in _live_profilers:
            with contextlib.suppress(Exception):
                profiler.stop()
        _live_profilers.clear()


# M31: a process callable embedding a large compiled IR would otherwise be re-pickled and
# re-shipped on EVERY submit (concurrent.futures does not dedupe callables). Instead it is
# broadcast to each worker ONCE, cached here by content hash, and tasks ship only (token,
# partition). The cache is keyed by hash so re-running the same plan reuses the cached process.
# capacity of the per-worker shared-process cache AND the driver's broadcast-token set --
# the SAME value on both sides, FIFO-evicted in broadcast order (identical across workers
# because every worker receives the same broadcast sequence), so "the driver thinks token T
# is primed" stays equivalent to "every worker has T".
_SHARED_CACHE_CAP = 32
_shared_objects: OrderedDict[str, object] = OrderedDict()


def _thread_resources() -> LocalResources:
    res = getattr(_thread_local, "res", None)
    if res is None:
        res = LocalResources()
        _thread_local.res = res
    return res


def _proc_init(
    profiler_factory: Callable[[], WorkerProfiler] | None = None,
    event_q: object | None = None,
) -> None:
    global _proc_resources, _proc_event_q, _proc_profiler, _proc_buffer
    global _proc_drain_stop, _proc_drain_thread
    _proc_resources = LocalResources()
    _proc_event_q = event_q
    if profiler_factory is not None:
        with contextlib.suppress(Exception):  # a profiler that won't start just disables sampling
            prof = profiler_factory()
            prof.start()
            _proc_profiler = prof
            _register_profiler(prof)  # stopped at worker-process exit (silences sampler teardown)
    if event_q is not None:  # a monitor is attached -> run the off-path drain thread for this worker
        _proc_buffer = deque(maxlen=_PROC_BUFFER_CAP)
        _proc_drain_stop = threading.Event()
        _proc_drain_thread = threading.Thread(
            target=_proc_drain_loop, name="graphed-dash-worker-drain", daemon=True
        )
        _proc_drain_thread.start()
        atexit.register(_proc_drain_final)


def _proc_emit(item: tuple[str, object]) -> None:
    """Append an event to this worker's local buffer (in-process, ~0.1us). The drain thread ships it;
    the data path never blocks on IPC. The bounded deque drops the oldest on overflow (best-effort)."""
    buf = _proc_buffer
    if buf is not None:
        buf.append(item)


def _proc_drain_batch() -> None:
    """Move everything currently buffered to the driver in ONE Manager-queue put (amortizes IPC)."""
    buf, q = _proc_buffer, _proc_event_q
    if buf is None or q is None:
        return
    batch: list[tuple[str, object]] = []
    while True:
        try:
            batch.append(buf.popleft())
        except IndexError:
            break
    if batch:
        with contextlib.suppress(Exception):  # a full driver queue drops the batch; never blocks
            q.put_nowait(("batch", batch))  # type: ignore[attr-defined]


def _proc_drain_loop() -> None:
    stop = _proc_drain_stop
    assert stop is not None
    while not stop.is_set():
        stop.wait(_PROC_DRAIN_INTERVAL)
        _proc_drain_batch()


def _proc_drain_final() -> None:
    """At worker exit: stop sampling (same thread that started it), buffer the final session, and
    flush whatever remains so the last events/profile are not lost."""
    if _proc_drain_stop is not None:
        _proc_drain_stop.set()
    if _proc_profiler is not None:
        with contextlib.suppress(Exception):
            payload = _proc_profiler.stop()
            if payload and _proc_buffer is not None:
                _proc_buffer.append(("profile", (str(os.getpid()), payload)))
    _proc_drain_batch()


def _proc_profile_due() -> bool:
    """True at most once per ``_PROFILE_FLUSH_INTERVAL`` — keeps the profiler serialize (JSON-encoding
    the accumulated stack tree) off the per-task path. Called on the worker thread."""
    global _proc_last_flush
    now = time.monotonic()
    if now - _proc_last_flush >= _PROFILE_FLUSH_INTERVAL:
        _proc_last_flush = now
        return True
    return False


def _run_with_emit(
    process: Callable[[Partition, LocalResources], object],
    task: Task,
    resources: LocalResources,
    *,
    worker: str,
    emit_event: Callable[[TaskEvent], None],
    emit_profile: Callable[[str, bytes], None],
    profiler: WorkerProfiler | None,
    profile_due: Callable[[], bool],
) -> object:
    """Run one task, emitting STARTED before and FINISHED/ERRORED after (worker-side timing).
    ``emit_event`` is cheap (a local buffer append for processes, an in-process enqueue for threads);
    the (expensive) profiler serialize runs only when ``profile_due()`` says so (time-throttled, off
    the per-task path). SUBMITTED is emitted driver-side. Emission is best-effort."""
    label = partition_label(task.partition)
    n = task.partition.n_entries
    emit_event(TaskEvent(TaskPhase.STARTED, task.key, worker, time.perf_counter(), label, n))
    try:
        result = process(task.partition, resources)
    except BaseException as exc:
        emit_event(
            TaskEvent(
                TaskPhase.ERRORED, task.key, worker, time.perf_counter(), label, n, error=_render_error(exc)
            )
        )
        raise
    emit_event(TaskEvent(TaskPhase.FINISHED, task.key, worker, time.perf_counter(), label, n))
    if profiler is not None and profile_due():
        with contextlib.suppress(Exception):
            payload = profiler.flush()
            if payload:
                emit_profile(worker, payload)
    return result


def _render_error(exc: BaseException) -> str:
    """A concise, picklable error summary for the dashboard. A graphed-debug ``StageError`` already
    str()s to a user-source-mapped message (op + analysis line), so its own text is the best summary;
    anything else gets a plain ``Type: message``."""
    if isinstance(exc, StageError):
        return str(exc)
    return f"{type(exc).__name__}: {exc}"


def _thread_profiler(monitor: Monitor | None) -> WorkerProfiler | None:
    """A per-thread profiler, built once from the monitor's factory and reused for this worker
    thread's later tasks. ``None`` when no monitor or no factory (MVP tier)."""
    if monitor is None:
        return None
    cached = getattr(_thread_local, "prof", _MISSING)
    if cached is not _MISSING:
        return cast("WorkerProfiler | None", cached)
    prof: WorkerProfiler | None = None
    with contextlib.suppress(Exception):
        factory = monitor.worker_profiler_factory()
        if factory is not None:
            prof = factory()
            prof.start()
            _register_profiler(prof)  # stopped at process exit (silences sampler teardown)
    _thread_local.prof = prof
    return prof


def _thread_profile_due() -> bool:
    """Per-thread time throttle for the profiler serialize (see :func:`_proc_profile_due`)."""
    now = time.monotonic()
    last = cast("float", getattr(_thread_local, "last_flush", 0.0))
    if now - last >= _PROFILE_FLUSH_INTERVAL:
        _thread_local.last_flush = now
        return True
    return False


def _thread_task(
    process: Callable[[Partition, LocalResources], object], task: Task, monitor: Monitor | None
) -> object:
    def emit_event(ev: TaskEvent) -> None:
        emit_task(monitor, ev)  # in-process enqueue; the NetworkMonitor's sender thread does the I/O

    def emit_profile(worker: str, payload: bytes) -> None:
        if monitor is not None:
            with contextlib.suppress(Exception):
                monitor.on_profile(worker, payload)

    return _run_with_emit(
        process,
        task,
        _thread_resources(),
        worker=threading.current_thread().name,
        emit_event=emit_event,
        emit_profile=emit_profile,
        profiler=_thread_profiler(monitor),
        profile_due=_thread_profile_due,
    )


def _prime_shared(token: str, payload: bytes) -> int:
    """Cache the broadcast process under ``token`` and return this worker's pid so the driver
    can confirm coverage. Bounded FIFO (cap ``_SHARED_CACHE_CAP``), evicting the oldest
    broadcast -- identical across workers because every worker sees the same broadcast order,
    so it stays in lockstep with the driver's token set. The brief hold makes a worker keep
    this task long enough for its siblings to each claim one (single-round coverage)."""
    if token not in _shared_objects:  # idempotent: re-priming an existing token does not reorder
        _shared_objects[token] = pickle.loads(payload)
        while len(_shared_objects) > _SHARED_CACHE_CAP:
            _shared_objects.popitem(last=False)
    time.sleep(0.002)
    return os.getpid()


def _proc_task_shared(token: str, task: Task) -> object:
    assert _proc_resources is not None  # set by the pool initializer
    process = cast("Callable[[Partition, LocalResources], object]", _shared_objects[token])

    def emit_event(ev: TaskEvent) -> None:
        _proc_emit(("task", ev))  # local buffer append; the drain thread ships it off-path

    def emit_profile(worker: str, payload: bytes) -> None:
        _proc_emit(("profile", (worker, payload)))

    return _run_with_emit(
        process,
        task,
        _proc_resources,
        worker=str(os.getpid()),
        emit_event=emit_event,
        emit_profile=emit_profile,
        profiler=_proc_profiler,
        profile_due=_proc_profile_due,
    )


def _combine_task(combine: Callable[[object, object], object], a: object, b: object) -> object:
    """Pool entry for a pooled combine (module-level so a spawned process can import it)."""
    return combine(a, b)


class _Window(Generic[T]):
    """The dispatch point of a hub route (plan-A2 A2-1): held items go out through at most ``size``
    slots, and only while the control is RUNNING; without a control every held item goes at once."""

    def __init__(self, control: RunControl | None, size: int, items: Iterable[T] = ()) -> None:
        self.control = control
        self.size = size if control is not None else sys.maxsize
        self.held: deque[T] = deque(items)
        self.stopped = False  # a check found the control CANCELLED with work left to start
        self._paused = False

    def cancelled(self) -> bool:
        """A check that drops the held work once the control is CANCELLED."""
        if self.control is None or self.control.state is not RunState.CANCELLED:
            return False
        self.stopped = True
        self.held.clear()
        return True

    def take(self, in_flight: int) -> list[T]:
        """The check before starting tasks: the held items to submit now, given ``in_flight`` running."""
        state = self.control.state if self.control is not None else RunState.RUNNING
        self._paused = state is RunState.PAUSED
        if state is RunState.CANCELLED:
            self.stopped |= bool(self.held)
            self.held.clear()
        if state is not RunState.RUNNING:
            return []
        return [self.held.popleft() for _ in range(min(len(self.held), self.size - in_flight))]

    def wait(self, futures: list[Future[Any]]) -> set[Future[Any]]:
        """The next completions; empty on a paused wake or a resume, which only re-run :meth:`take`."""
        if not futures:  # paused with work held and nothing running
            assert self.control is not None
            self.control.wait()
            return set()
        timeout = _PAUSED_WAKE_S if self._paused else None  # the state take() saw, so a resume still wakes
        return wait(futures, timeout=timeout, return_when=FIRST_COMPLETED)[0]


class _BaseExecutor:
    """Shared driver. Subclasses supply the worker pool + the (picklable) worker entry point.

    ``pooled_combines=True`` (M10) schedules the tree-reduce combines onto the SAME worker pool as
    the leaves, instead of running them serially on the driver thread — for heavy partials (large
    histograms, many partitions) the driver otherwise becomes a serial combine bottleneck. The
    combine pairing is the same fixed `plan_tree`, so results stay bit-identical; with a process
    pool, `plan.combine` must be picklable (module-level), exactly like `plan.process`."""

    def __init__(
        self,
        max_workers: int | None = None,
        *,
        on_combine: Callable[[int], None] | None = None,
        pooled_combines: bool = False,
        persistent: bool = False,
        monitor: Monitor | None = None,
        comms: str | None = "ipc",
        steal: bool = True,
        control: RunControl | None = None,
        max_in_flight: int = 2,
    ):
        self._plans = PlanQueue(self.run, max_in_flight)
        self.max_workers = max_workers if max_workers is not None else (os.cpu_count() or 1)
        self._on_combine = on_combine  # test hook: called per tree-reduce combine with #leaves so far
        self._pooled_combines = pooled_combines
        # persistent=True keeps ONE pool across run() calls (amortizing the import-heavy spawn
        # over many plans — notebooks, sweeps); the default stays a fresh pool per run.
        self._persistent = persistent
        self._kept_pool: _PoolExecutor | None = None
        self._broadcast_tokens: OrderedDict[str, None] = (
            OrderedDict()
        )  # M31/M34: primed tokens, FIFO-bounded in lockstep with each worker cache
        self.monitor = monitor  # M37: a passive dashboard observer (None => no instrumentation)
        self.control = control  # m65: pause/resume/cancel, read at each run (plan-A1 A-2)
        # the running plan's snapshot: a reassignment mid-run reaches the next plan, not this one
        self._run_monitor: Monitor | None = None
        # M38: comms=None -> the hub reduction (driver combines); "ipc"/"http" -> PEER reduction
        # (combines run across the workers over that transport, off the driver). Result is identical
        # (same fixed plan_tree grouping); peer just relocates the combines. steal=True (peer only)
        # lets an idle worker take leaves from a busy peer (process work moves; the leaf's owner still
        # reduces it -> result unchanged). The last run's per-worker witness stats are kept for tests.
        self._comms = comms
        self._steal = steal
        self._last_peer_witness: list[dict[str, int]] = []
        # Per-run state lives on the instance (kept pool, peer registry, broadcast tokens).
        self._run_lock = threading.Lock()
        # STRICT: peer reduction (comms set) emits monitor events + fires on_combine, but it CANNOT run
        # pooled combines (its whole model is off-driver combines — `pooled_combines` is a hub-only
        # mechanism). Rather than SILENTLY falling back to the hub path (hub-mode sneaking into a run
        # the caller asked to be peer), refuse loudly, so a big calculation can never quietly run hub
        # when peer was requested. (comms=None => hub, which supports pooled_combines.)
        if comms is not None and pooled_combines:
            raise ValueError(
                f"peer reduction (comms={comms!r}) does not support pooled_combines (a hub-only "
                f"mechanism); use comms=None for the hub path, or drop pooled_combines"
            )

    def _pool(self) -> _PoolExecutor:
        raise NotImplementedError

    def _raw_submit(
        self, pool: _PoolExecutor, process: Callable[[Partition, LocalResources], object]
    ) -> Callable[[Task], Future[object]]:
        """Deliver ``process`` to the pool's workers as needed and return ``submit(task)``.
        Threads share memory (no delivery); processes broadcast the process once (M31)."""
        raise NotImplementedError

    def _prepare(
        self, pool: _PoolExecutor, process: Callable[[Partition, LocalResources], object]
    ) -> Callable[[Task], Future[object]]:
        """Wrap the subclass submit with a driver-side SUBMITTED emission (M37). Worker-side STARTED/
        FINISHED/ERRORED come from the worker entry; here we record the moment of submission."""
        raw = self._raw_submit(pool, process)
        monitor = self._run_monitor
        if monitor is None:
            return raw

        def submit(task: Task) -> Future[object]:
            emit_task(
                monitor,
                TaskEvent(
                    TaskPhase.SUBMITTED,
                    task.key,
                    "driver",
                    time.perf_counter(),
                    partition_label(task.partition),
                    task.partition.n_entries,
                ),
            )
            return raw(task)

        return submit

    def _emit_submitted(self, tasks: Iterable[Task]) -> None:
        """Driver-side SUBMITTED for every task, when it becomes known (M37)."""
        if self._run_monitor is None:
            return
        for t in tasks:
            emit_task(
                self._run_monitor,
                TaskEvent(
                    TaskPhase.SUBMITTED,
                    t.key,
                    "driver",
                    time.perf_counter(),
                    partition_label(t.partition),
                    t.partition.n_entries,
                ),
            )

    def _folded(
        self, plan: Plan[R], pieces: list[tuple[int, R]], done: int, n_combines: int, stopped: bool
    ) -> ExecResult[R]:
        """The result from what the combine tree left: the root, or on a cancel the completed subtrees
        folded by first leaf (plan-A2 A2-1)."""
        value, k = running_fold(iter(pieces), plan.combine, plan.empty)
        for _ in range(k):
            self._combine_cb(done)
        return ExecResult(
            value, done, n_combines + k, StopReason.CANCELLED if stopped else StopReason.EXHAUSTED
        )

    def _combine_cb(self, leaves_done: int) -> None:
        """The tree-reduce combine callback: the legacy test hook AND the dashboard monitor (M37)."""
        if self._on_combine is not None:
            self._on_combine(leaves_done)
        if self._run_monitor is not None:
            with contextlib.suppress(Exception):
                self._run_monitor.on_combine(leaves_done)

    @contextlib.contextmanager
    def _acquired_pool(self) -> Iterator[_PoolExecutor]:
        if not self._persistent:
            self._broadcast_tokens = OrderedDict()  # a fresh pool: nothing is primed yet
            with self._pool() as pool:
                yield pool
            return
        if self._kept_pool is None:
            self._broadcast_tokens = OrderedDict()  # newly (re)spawned workers hold no cache
            self._kept_pool = self._pool()
        yield self._kept_pool  # kept alive for the next run()

    @property
    def max_in_flight(self) -> int:
        return self._plans.max_in_flight

    def submit(self, plan: Plan[R]) -> Future[ExecResult[R]]:
        """Queue ``plan`` and return a future of its :meth:`run` result without waiting for it.
        Plans run one at a time in submit order; this blocks while ``max_in_flight`` submitted plans
        are unfinished."""
        return self._plans.submit(plan)

    def close(self) -> None:
        """Finish every submitted plan, then release a persistent pool (idempotent); a later run()
        or submit() lazily respawns."""
        self._plans.close()
        if self._kept_pool is not None:
            self._kept_pool.shutdown(wait=True)
            self._kept_pool = None
            self._broadcast_tokens = OrderedDict()  # respawned workers will need re-priming

    def __enter__(self) -> _BaseExecutor:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def run(self, plan: Plan[R]) -> ExecResult[R]:
        """Run ``plan`` to its reduced result. One plan runs at a time per executor; a concurrent
        caller waits for the running plan to finish."""
        with self._run_lock:
            self._run_monitor = self.monitor
            control = self.control
            try:
                if control is not None and control.state is RunState.CANCELLED:
                    return ExecResult(plan.empty(), 0, 0, StopReason.CANCELLED)
                if plan.next_tasks is not None:
                    return self._run_adaptive(plan, control)
                if self._comms is not None:
                    return self._run_peer(plan, control)
                if self._pooled_combines:
                    return self._run_fixed_pooled(plan, control)
                if control is not None:
                    return self._run_fixed_windowed(plan, control)
                return self._run_fixed(plan)
            finally:
                if control is not None and control.state is RunState.CANCELLED:
                    control.reset()  # a cancel ends this run only, whether or not a check saw it

    def _run_peer(self, plan: Plan[R], control: RunControl | None = None) -> ExecResult[R]:
        """M38 peer reduction: partition the leaves into contiguous per-worker ranges and reduce them
        across the workers over ``self._comms``, off the driver. The subclass spawns the W actors
        (threads or processes); the result is bit-for-bit the hub path's (same fixed tree).

        Monitor parity (so peer can be the default): the driver emits SUBMITTED per task here, the
        workers emit STARTED/FINISHED/ERRORED over the transport (forwarded to the monitor by the
        collect loop), and the driver fires the n-1 combine callbacks below (completed-1 on a cancel)."""
        tasks = sorted(plan.tasks, key=lambda t: t.key)  # deterministic leaf order
        n = len(tasks)
        if n == 0:
            return ExecResult(plan.empty(), 0, 0, StopReason.EXHAUSTED)
        w = max(1, min(self.max_workers, n))
        bounds = make_bounds(n, w)
        worker_addrs = tuple(f"w{i}" for i in range(w))
        items = slice_items([t.partition for t in tasks], bounds, worker_addrs)
        self._emit_submitted(tasks)  # worker-side STARTED/FINISHED/ERRORED stream in
        # A thread actor starts its first leaf before the driver's first poll, so no tag could hold a
        # run entered paused: it waits here, before any actor exists.
        if control is not None and control.wait() is RunState.CANCELLED:
            return ExecResult(plan.empty(), 0, 0, StopReason.CANCELLED)
        value, done = self._peer_execute(plan, n, w, bounds, worker_addrs, items, control)
        for _ in range(done - 1):  # peer ran done-1 combines across the workers; report the count
            self._combine_cb(done)
        stopped = StopReason.CANCELLED if done < n else StopReason.EXHAUSTED
        return ExecResult(value, done, max(done - 1, 0), stopped)

    def _peer_profiler_factory(self) -> Callable[[], WorkerProfiler] | None:
        """The picklable per-worker profiler factory (M37), or None — so peer workers profile exactly
        like the hub path and ``Dashboard(profile=True)`` works under peer (no silent loss)."""
        return self._run_monitor.worker_profiler_factory() if self._run_monitor is not None else None

    def _forward_peer_events(self, payload: tuple[Any, ...]) -> bool:
        """If ``payload`` is a worker monitor-event batch (task lifecycle) or a profile sample-tree,
        forward it to the monitor and return True; else return False (the caller handles root/other
        messages). Best-effort (M37 passivity) — a raising monitor never breaks the run."""
        if payload[0] == "events":
            for ev in payload[1]:
                emit_task(self._run_monitor, ev)
            return True
        if payload[0] == "profile" and self._run_monitor is not None:
            with contextlib.suppress(Exception):
                self._run_monitor.on_profile(payload[1], payload[2])
            return True
        return False

    def _peer_execute(
        self,
        plan: Plan[R],
        n: int,
        w: int,
        bounds: list[int],
        worker_addrs: tuple[str, ...],
        items: dict[str, list[tuple[int, Partition]]],
        control: RunControl | None = None,
    ) -> tuple[R, int]:
        """The reduced value and the number of leaves it holds (fewer than ``n`` only on a cancel)."""
        raise NotImplementedError

    def _run_fixed_pooled(self, plan: Plan[R], control: RunControl | None = None) -> ExecResult[R]:
        tasks = sorted(plan.tasks, key=lambda t: t.key)  # deterministic leaf order
        n = len(tasks)
        if n == 0:
            return ExecResult(plan.empty(), 0, 0, StopReason.EXHAUSTED)
        combines, root = plan_tree(n)
        assert root is not None  # n >= 1
        waiting: dict[int, list[int]] = {}  # input node -> combine indices needing it
        remaining: dict[int, set[int]] = {}  # combine index -> still-unready inputs
        first = list(range(n))  # node -> its first leaf (a combine's left input comes first)
        for ci, (_out, a, b) in enumerate(combines):
            remaining[ci] = {a, b}
            waiting.setdefault(a, []).append(ci)
            waiting.setdefault(b, []).append(ci)
            first.append(first[a])

        window = _Window(control, self.max_workers, enumerate(tasks))
        with self._acquired_pool() as pool:
            submit = self._raw_submit(pool, plan.process)
            self._emit_submitted(tasks)
            node_of: dict[Future[object], int] = {}
            ready: dict[int, R] = {}
            n_combines = 0
            leaves_done = 0
            sent = 0
            while True:
                for i, t in window.take(sent - leaves_done):
                    node_of[submit(t)] = i
                    sent += 1
                if not node_of and not window.held:
                    break
                for fut in window.wait(list(node_of)):
                    node = node_of.pop(fut)
                    ready[node] = cast(R, fut.result())  # re-raises a worker error intact
                    if node < n:
                        leaves_done += 1
                    for ci in waiting.get(node, ()):
                        remaining[ci].discard(node)
                        if not remaining[ci]:
                            out, a, b = combines[ci]
                            # a<b -> deterministic left/right grouping, same tree as the driver path
                            combine = cast("Callable[[object, object], object]", plan.combine)
                            f2 = pool.submit(_combine_task, combine, ready.pop(a), ready.pop(b))
                            node_of[f2] = out
                            n_combines += 1
                            self._combine_cb(leaves_done)
        pieces = [(first[node], v) for node, v in ready.items()]
        return self._folded(plan, sorted(pieces, key=lambda p: p[0]), leaves_done, n_combines, window.stopped)

    def _run_fixed(self, plan: Plan[R]) -> ExecResult[R]:
        tasks = sorted(plan.tasks, key=lambda t: t.key)  # deterministic leaf order
        n = len(tasks)
        if n == 0:
            return ExecResult(plan.empty(), 0, 0, StopReason.EXHAUSTED)
        with self._acquired_pool() as pool:
            submit = self._prepare(pool, plan.process)
            leaf_of: dict[Future[object], int] = {submit(t): i for i, t in enumerate(tasks)}

            def completed() -> Iterator[tuple[int, R]]:
                for fut in as_completed(leaf_of):
                    yield leaf_of[fut], cast(R, fut.result())  # fut.result() re-raises a worker error

            value, n_combines = tree_reduce(
                n, completed(), plan.combine, plan.empty, on_combine=self._combine_cb
            )
        return ExecResult(value, n, n_combines, StopReason.EXHAUSTED)

    def _run_fixed_windowed(self, plan: Plan[R], control: RunControl) -> ExecResult[R]:
        """``_run_fixed`` through a :class:`_Window`: the same fixed tree, fed as leaves complete."""
        tasks = sorted(plan.tasks, key=lambda t: t.key)  # deterministic leaf order
        reducer: LazyReducer[R] = LazyReducer(
            len(tasks), plan.combine, plan.empty, on_combine=self._combine_cb
        )
        window = _Window(control, self.max_workers, enumerate(tasks))
        with self._acquired_pool() as pool:
            submit = self._raw_submit(pool, plan.process)
            self._emit_submitted(tasks)
            leaf_of: dict[Future[object], int] = {}
            while True:
                for leaf, t in window.take(len(leaf_of)):
                    leaf_of[submit(t)] = leaf
                if not leaf_of and not window.held:
                    break
                for fut in window.wait(list(leaf_of)):
                    reducer.feed(leaf_of.pop(fut), cast(R, fut.result()))  # re-raises a worker error
        return self._folded(plan, reducer.frontier(), reducer.delivered, reducer.n_combines, window.stopped)

    def _run_adaptive(self, plan: Plan[R], control: RunControl | None = None) -> ExecResult[R]:
        assert plan.next_tasks is not None
        ctx = ExecContext()
        start = time.perf_counter()
        results: list[tuple[int, R]] = []
        submitted: dict[Future[object], tuple[int, int, float]] = {}  # future -> (key, n_entries, t0)
        stopped: StopReason | None = None
        next_tasks = plan.next_tasks
        window: _Window[Task] = _Window(control, self.max_workers)

        with self._acquired_pool() as pool:
            submit = self._raw_submit(pool, plan.process)

            def refill() -> None:
                batch = next_tasks(ctx)  # DONE == None
                if batch:
                    self._emit_submitted(batch)
                    window.held.extend(batch)

            refill()
            while True:
                for task in window.take(len(submitted)):
                    # t0 at submission, so time held in the window never counts as run time
                    submitted[submit(task)] = (task.key, task.partition.n_entries, time.perf_counter())
                if not submitted and not window.held:
                    break
                done = window.wait(list(submitted))
                if not done:
                    continue
                for fut in done:
                    key, n_entries, t0 = submitted.pop(fut)
                    results.append((key, cast(R, fut.result())))  # re-raises a worker error intact
                    ctx.n_done += 1
                    ctx.events_done += n_entries
                    ctx.last_durations[key] = time.perf_counter() - t0
                ctx.elapsed_s = time.perf_counter() - start
                if window.cancelled():  # the next_tasks call is skipped; no stop exit ends the drain
                    continue
                reason = plan.stop.reason(ctx) if plan.stop else None
                if reason is not None:
                    stopped = reason
                    for fut in submitted:
                        fut.cancel()
                    break
                refill()

        value, n_combines = running_fold(iter(results), plan.combine, plan.empty)
        stopped = stopped or (StopReason.CANCELLED if window.stopped else StopReason.EXHAUSTED)
        return ExecResult(value, ctx.n_done, n_combines, stopped)


class ThreadExecutor(_BaseExecutor):
    """Thread-pool executor: a thread-safe worker pool with thread-local `open_once` resources."""

    def _pool(self) -> _PoolExecutor:
        return ThreadPoolExecutor(max_workers=self.max_workers)

    def _raw_submit(
        self, pool: _PoolExecutor, process: Callable[[Partition, LocalResources], object]
    ) -> Callable[[Task], Future[object]]:
        return lambda task: pool.submit(_thread_task, process, task, self._run_monitor)

    def _peer_execute(
        self,
        plan: Plan[R],
        n: int,
        w: int,
        bounds: list[int],
        worker_addrs: tuple[str, ...],
        items: dict[str, list[tuple[int, Partition]]],
        control: RunControl | None = None,
    ) -> tuple[R, int]:
        # threads share the process, so the in-process transports (queue.Queue or loopback HTTP) are
        # built once by the driver and handed to each worker thread directly.
        transports = build_transports(self._comms or "ipc", ("driver", *worker_addrs))
        witness: dict[str, dict[str, int]] = {}
        errors: dict[str, BaseException] = {}  # a worker thread's exception (captured, then re-raised)

        def actor(addr: str) -> None:
            try:
                witness[addr] = process_and_reduce(
                    addr,
                    transports[addr],
                    n,
                    bounds,
                    worker_addrs,
                    plan.process,
                    plan.combine,
                    items[addr],
                    LocalResources(),  # fresh per worker thread (closed when the actor returns)
                    steal=self._steal,
                    emit=self._run_monitor is not None,
                    profiler_factory=self._peer_profiler_factory(),
                )
            except BaseException as exc:  # a thread exception is otherwise lost -> capture + propagate
                errors[addr] = exc

        threads = [
            threading.Thread(target=actor, args=(a,), name=f"graphed-peer-{a}", daemon=True)
            for a in worker_addrs
        ]
        driver_t = transports["driver"]
        ctl = PeerControl(control, driver_t, n, plan.combine, plan.empty) if control is not None else None
        for t in threads:
            t.start()
        try:
            deadline = time.monotonic() + _PEER_ROOT_TIMEOUT_S
            out: tuple[R, int] | None = None
            while out is None:
                got = driver_t.recv(timeout=0.05)
                if got is not None and got[1][0] == "root":
                    out = got[1][1], n
                    break
                if got is not None and not self._forward_peer_events(got[1]) and ctl is not None:
                    out = ctl.take(got[1])  # a cancel hand-in; the fold once all are in
                if errors:  # a worker failed -> the root will never form; stop waiting and re-raise
                    break
                if ctl is not None and ctl.relay():
                    deadline = time.monotonic() + _PEER_ROOT_TIMEOUT_S  # paused time does not count
                if time.monotonic() >= deadline:
                    driver_t.broadcast(("done",))  # release the actors the finally joins
                    raise TimeoutError(
                        f"peer reduction did not produce a root within {_PEER_ROOT_TIMEOUT_S}s"
                    )
            if ctl is not None:  # a queued tag lands before `done`
                ctl.close(0.0 if errors else OUTBOX_EXIT_WAIT_S)
            driver_t.broadcast(("done",))
            for t in threads:
                t.join(timeout=30.0)
            for _sender, payload in driver_t.poll():  # drain trailing monitor events the workers shipped
                self._forward_peer_events(payload)
            if errors:
                raise next(iter(errors.values()))  # re-raise a worker exception intact (M6)
            assert out is not None
            return out
        finally:
            if ctl is not None:
                ctl.close(0.0)
            for t in threads:
                t.join(timeout=30.0)
            for tr in transports.values():
                tr.close()
            self._last_peer_witness = [witness[a] for a in worker_addrs if a in witness]


class _ProcessExecutorBase(_BaseExecutor):
    """Shared process-pool machinery: spawn-based worker processes (cross-platform + free-threaded-safe),
    each with its own ``open_once`` resources. The Plan ``process`` callable and its partials must be
    picklable; a remote ``StageError`` round-trips to the driver intact.

    The two public process executors differ ONLY in which pool peer-reduction IPC uses, selected by the
    class attribute ``_peer_pool_is_pinned`` (no silent runtime switch): :class:`ProcessPoolExecutor`
    (full-registry stdlib pool) vs :class:`PinnedPoolExecutor` (identity-pinned, bounded O(log N)
    overlay). Everything else — the main task pool, tree reduction, monitoring — is identical.

    M37: when a :class:`Monitor` is attached, workers cannot reach the driver's monitor object, so
    they push events onto a bounded ``Manager().Queue()`` that a driver-side **collector daemon
    thread** drains and replays into the monitor. The queue is best-effort (drop-on-full) so
    instrumentation never back-pressures a worker."""

    #: Which pool the peer-reduction IPC path builds. Subclasses set this; the base is never used directly.
    _peer_pool_is_pinned: bool = False

    def __init__(
        self,
        max_workers: int | None = None,
        *,
        on_combine: Callable[[int], None] | None = None,
        pooled_combines: bool = False,
        persistent: bool = False,
        monitor: Monitor | None = None,
        comms: str | None = "ipc",
        steal: bool = True,
        control: RunControl | None = None,
        max_in_flight: int = 2,
    ):
        super().__init__(
            max_workers,
            on_combine=on_combine,
            pooled_combines=pooled_combines,
            persistent=persistent,
            monitor=monitor,
            comms=comms,
            steal=steal,
            control=control,
            max_in_flight=max_in_flight,
        )
        self._mgr: SyncManager | None = None
        self._event_q: object | None = None
        self._collector: threading.Thread | None = None
        self._collector_stop: threading.Event | None = None
        # M38 persistent peer state: reused across run()s when persistent=True so the worker spawn is
        # paid once, like the hub pool. The full-registry path reuses on w; the identity-pinned path's
        # topology depends on (n, w) so it also keys on n (a repeated plan never respawns either way).
        self._peer_pool: _StdProcessPool | PinnedProcessPool | None = None
        self._peer_nworkers = 0
        self._peer_n = 0
        self._peer_pinned = False  # which IPC path the live pool is (identity-pinned vs full-registry)
        self._warned_fd_budget = False  # fire the full-registry fd-budget warning at most once
        self._peer_registry: dict[str, Any] | None = None  # SimpleQueue inbox per address
        self._peer_driver: QueueTransport | None = None

    def _close_peer(self) -> None:
        if self._peer_pool is not None:
            self._peer_pool.shutdown(wait=True)
            self._peer_pool = None
        if self._peer_registry is not None:
            _close_registry(self._peer_registry)
            self._peer_registry = None
        self._peer_driver = None
        self._peer_nworkers = 0
        self._peer_n = 0
        self._peer_pinned = False

    # ---- M38 peer reduction across worker PROCESSES (cross-process transport) ----

    def _peer_execute(
        self,
        plan: Plan[R],
        n: int,
        w: int,
        bounds: list[int],
        worker_addrs: tuple[str, ...],
        items: dict[str, list[tuple[int, Partition]]],
        control: RunControl | None = None,
    ) -> tuple[R, int]:
        ctx = multiprocessing.get_context("spawn")
        try:
            if (self._comms or "ipc") == "http":
                return self._peer_http(plan, n, w, bounds, worker_addrs, items, ctx, control)
            return self._peer_ipc(plan, n, w, bounds, worker_addrs, items, ctx, control)
        except BaseException:
            # A failed run leaves messages in flight that a bounded exit deliberately abandoned: a
            # peer's >64 KB partial parked in the crashed worker's pipe, the driver's own `done`.
            # They land AFTER run() returns, so a reused pool cannot be cleaned by draining at the
            # start of the next run — that run would take the stale `done` as its first message and
            # its worker would leave the actor before reducing anything. Drop the persistent peer
            # state instead; the next run respawns. Bounded because ``_collect_peer`` releases the
            # workers on every exit: the surviving actors return within ``OUTBOX_EXIT_WAIT_S``.
            self._close_peer()
            raise

    def _peer_ipc(
        self,
        plan: Plan[R],
        n: int,
        w: int,
        bounds: list[int],
        worker_addrs: tuple[str, ...],
        items: dict[str, list[tuple[int, Partition]]],
        ctx: Any,
        control: RunControl | None = None,
    ) -> tuple[R, int]:
        # IPC across processes. The pool is the executor's fixed choice (no silent switch):
        #  * ProcessPoolExecutor (full-registry, the default): every worker inherits every SimpleQueue
        #    inbox (O(N²) fds — fine while N << the fd limit).
        #  * PinnedPoolExecutor (identity-pinned, large many-core machines): each worker inherits ONLY
        #    its inbox + its O(log N) overlay peers (reduction targets + hypercube lifelines + driver),
        #    so the registry is O(N log N) and stays under the per-process fd limit. Both bound stealing
        #    to the lifelines. (A *dynamic* cluster — workers joining/dying — needs a lazy-connect
        #    transport + multi-hop routing over this same overlay: the Phase-2 distributed runtime,
        #    which reuses worker_outbox_addresses.)
        pinned = self._peer_pool_is_pinned
        if not pinned and not self._warned_fd_budget and _exceeds_fd_budget(w):
            self._warned_fd_budget = True
            warnings.warn(
                f"ProcessPoolExecutor peer reduction inherits ~2*(w+1) queue fds per worker; with "
                f"w={w} this likely exceeds the per-process file-descriptor limit on this machine. "
                f"Use PinnedPoolExecutor for a bounded O(log N) overlay on large many-core machines.",
                stacklevel=2,
            )
        addrs = ("driver", *worker_addrs)
        reuse = (
            self._persistent
            and self._peer_pool is not None
            and self._peer_pinned == pinned
            and self._peer_nworkers == w
            and (self._peer_n == n or not pinned)  # only the pinned topology depends on n
            and self._peer_registry is not None
            and self._peer_driver is not None
        )
        if reuse:
            pool, registry, driver_t = self._peer_pool, self._peer_registry, self._peer_driver
            assert pool is not None and registry is not None and driver_t is not None
            for q in registry.values():  # clear any straggler before reuse (a clean run leaves none)
                _drain_queue(q)
        else:
            self._close_peer()
            registry = {a: PipeInbox(ctx) for a in addrs}  # SimpleQueue inboxes: no per-queue feeder thread
            driver_t = QueueTransport(
                "driver", registry["driver"], {a: registry[a] for a in addrs if a != "driver"}
            )
            if pinned:
                outbox = worker_outbox_addresses(n, bounds, worker_addrs)  # address -> its O(log N) peers
                wi = {a: i for i, a in enumerate(worker_addrs)}
                init_args = [
                    (
                        a,
                        registry[a],  # this worker's inbox
                        {t: registry[t] for t in outbox[a]},  # only its O(log N) outboxes (+ driver)
                        tuple(worker_addrs[j] for j in lifeline_neighbors(wi[a], w)),  # steal lifelines
                    )
                    for a in worker_addrs
                ]
                pool = PinnedProcessPool(w, ctx, pinned_peer_init, init_args)
            else:
                pool = _StdProcessPool(
                    max_workers=w, mp_context=ctx, initializer=peer_pool_init, initargs=(registry,)
                )
            if self._persistent:
                self._peer_pool, self._peer_registry, self._peer_driver = pool, registry, driver_t
                self._peer_nworkers, self._peer_n, self._peer_pinned = w, n, pinned
        factory = self._peer_profiler_factory()
        try:
            if pinned:
                ppool = cast(PinnedProcessPool, pool)
                futs = [
                    ppool.submit(
                        pinned_peer_actor,
                        n,
                        bounds,
                        worker_addrs,
                        plan.process,
                        plan.combine,
                        items[a],
                        self._steal,
                        self._run_monitor is not None,
                        factory,
                        worker=i,
                    )
                    for i, a in enumerate(worker_addrs)
                ]
            else:
                fpool = cast("_StdProcessPool", pool)
                futs = [
                    fpool.submit(
                        pooled_peer_actor,
                        a,
                        n,
                        bounds,
                        worker_addrs,
                        plan.process,
                        plan.combine,
                        items[a],
                        self._steal,
                        self._run_monitor is not None,
                        factory,
                    )
                    for a in worker_addrs
                ]
            return self._collect_peer(driver_t, plan, n, futs, pool, control)
        finally:
            if not self._persistent:
                pool.shutdown()
                _close_registry(registry)

    def _peer_http(
        self,
        plan: Plan[R],
        n: int,
        w: int,
        bounds: list[int],
        worker_addrs: tuple[str, ...],
        items: dict[str, list[tuple[int, Partition]]],
        ctx: Any,
        control: RunControl | None = None,
    ) -> tuple[R, int]:
        # HTTP across processes: each worker binds its own loopback server and announces its port to
        # the driver (known up front), which assembles + broadcasts the registry — real sockets, the
        # path a distributed scheduler takes. persistent=True reuses the worker POOL (spawn paid once);
        # the loopback servers + discovery handshake are re-done per run (workers are per-submit actors).
        reuse = (
            self._persistent
            and self._peer_pool is not None
            and not self._peer_pinned  # HTTP always uses a plain stdlib pool, never the pinned pool
            and self._peer_nworkers == w
        )
        if reuse:
            pool = cast("_StdProcessPool", self._peer_pool)
        else:
            self._close_peer()
            pool = _StdProcessPool(max_workers=w, mp_context=ctx)
            if self._persistent:
                self._peer_pool, self._peer_nworkers = pool, w
        assert pool is not None
        driver_t = HttpTransport("driver")
        factory = self._peer_profiler_factory()
        try:
            futs = [
                pool.submit(
                    http_peer_actor,
                    a,
                    driver_t.host,
                    driver_t.port,
                    n,
                    bounds,
                    worker_addrs,
                    plan.process,
                    plan.combine,
                    items[a],
                    self._steal,
                    self._run_monitor is not None,
                    factory,
                )
                for a in worker_addrs
            ]
            http_driver_handshake(driver_t, worker_addrs, timeout_s=60.0)
            return self._collect_peer(driver_t, plan, n, futs, pool, control)
        finally:
            driver_t.close()
            if not self._persistent:
                pool.shutdown()

    def _collect_peer(
        self,
        driver_t: Any,
        plan: Plan[R],
        n: int,
        futs: list[Future[Any]],
        pool: Any = None,
        control: RunControl | None = None,
    ) -> tuple[R, int]:
        """Wait for the root while watching the worker futures: a worker exception means the root will
        never form, so we must detect it PROMPTLY (not after the 300s safety timeout) and re-raise it
        intact (a picklable ``StageError``, M6 obligation). Also records each worker's witness stats."""
        ctl = PeerControl(control, driver_t, n, plan.combine, plan.empty) if control is not None else None
        deadline = time.monotonic() + _PEER_ROOT_TIMEOUT_S
        out: tuple[R, int] | None = None
        try:
            while out is None:
                got = driver_t.recv(timeout=0.05)
                if got is not None and got[1][0] == "root":
                    out = got[1][1], n
                    break
                # worker STARTED/FINISHED/ERRORED -> monitor; a cancel hand-in -> the fold once all are in
                if got is not None and not self._forward_peer_events(got[1]) and ctl is not None:
                    out = ctl.take(got[1])
                for f in futs:  # a dead worker -> re-raise the real cause now, not 300s from now
                    if f.done() and f.exception() is not None:
                        f.result()
                # PinnedProcessPool backstop: a HARD worker crash ships no error + leaves its Future
                # unresolved, so the future check above can't see it — detect the dead process directly.
                if pool is not None and not getattr(pool, "workers_alive", lambda: True)():
                    raise RuntimeError("a peer worker process died before producing the root")
                if ctl is not None and ctl.relay():
                    deadline = time.monotonic() + _PEER_ROOT_TIMEOUT_S  # paused time does not count
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        f"peer reduction did not produce a root within {_PEER_ROOT_TIMEOUT_S}s"
                    )
        except BaseException:
            # EVERY exit releases the workers — including a KeyboardInterrupt delivered while parked
            # in ``recv``. An unreleased actor waits for ``done`` forever, and the pool shutdown that
            # follows (the non-persistent ``finally``, the persistent discard) then never returns.
            if ctl is not None:
                ctl.close(0.0)
            release_workers(driver_t)
            raise
        if ctl is not None:  # a queued tag lands in this run's inboxes before `done`
            ctl.close(OUTBOX_EXIT_WAIT_S)
        release_workers(driver_t)
        if self._run_monitor is None:
            # Fast path (no monitor): workers see ``done`` immediately (the transport wakes on the
            # message) and return, so BLOCK on the futures' completion instead of polling ``f.done()``
            # on a 20 ms cadence. That poll granularity was a fixed ~20-30 ms tail on EVERY run — under
            # 1 % of a long job but ~15 % of a sub-second interactive pass (the measured peer-vs-hub
            # regression on the ADL benchmark was almost entirely this join). ``f.result()`` is woken
            # the instant a worker finishes and re-raises a worker error intact (M6).
            self._last_peer_witness = [f.result() for f in futs]
            return out
        # Monitored: drain trailing events until every worker has finished (a late FINISHED/ERRORED,
        # shipped after the root formed, must not be lost) — we cannot just block on f.result() here
        # because a worker could still be shipping events, so we drain concurrently. The IPC transport
        # write is synchronous (a worker's trailing telemetry is readable the instant it returns), so a
        # FINE cadence has no tail: a coarse cadence was a ~20 ms over-wait per run = ~13 % of a
        # sub-second interactive pass, the bulk of the measured monitor-attached overhead. HTTP delivers
        # asynchronously (a worker's end-of-run profile POST can still be in flight when its future
        # completes), so it keeps the coarse grace to let that final POST land before we stop draining.
        drain_poll = 0.02 if isinstance(driver_t, HttpTransport) else _PEER_MONITOR_DRAIN_POLL_S
        while not all(f.done() for f in futs):
            got = driver_t.recv(timeout=drain_poll)
            if got is not None:
                self._forward_peer_events(got[1])
        for _sender, payload in driver_t.poll():
            self._forward_peer_events(payload)
        self._last_peer_witness = [f.result() for f in futs]  # propagate any error even on success
        return out

    def _pool(self) -> _PoolExecutor:
        factory = self._run_monitor.worker_profiler_factory() if self._run_monitor is not None else None
        return _StdProcessPool(
            max_workers=self.max_workers,
            mp_context=multiprocessing.get_context("spawn"),
            initializer=_proc_init,
            initargs=(factory, self._event_q),
        )

    def _raw_submit(
        self, pool: _PoolExecutor, process: Callable[[Partition, LocalResources], object]
    ) -> Callable[[Task], Future[object]]:
        payload = pickle.dumps(process)  # pickled ONCE; the bytes are reused for the broadcast
        token = hashlib.sha256(payload).hexdigest()
        self._broadcast(pool, token, payload)
        return lambda task: pool.submit(_proc_task_shared, token, task)

    # ---- M37 dashboard collector (worker side-channel -> driver monitor) ----

    @contextlib.contextmanager
    def _acquired_pool(self) -> Iterator[_PoolExecutor]:
        self._ensure_collector()
        try:
            with super()._acquired_pool() as pool:
                yield pool
        finally:
            # The collector's lifecycle follows the POOL, not the run. Non-persistent: the pool is
            # shut down above, so drain + stop now. Persistent: workers stay alive and keep draining
            # their buffers *after* run() returns (emission is off-path/async), so the collector must
            # stay alive across runs or those trailing events are lost — close() stops it.
            if not self._persistent:
                self._stop_collector()

    def _ensure_collector(self) -> None:
        if self._run_monitor is None:
            return
        if self._mgr is None:  # one manager + queue for this executor's lifetime
            self._mgr = multiprocessing.get_context("spawn").Manager()
            self._event_q = self._mgr.Queue(maxsize=10000)
        if self._collector is None or not self._collector.is_alive():  # idempotent across runs
            self._collector_stop = threading.Event()
            self._collector = threading.Thread(
                target=self._collect_loop, name="graphed-dash-collector", daemon=True
            )
            self._collector.start()

    def _collect_loop(self) -> None:
        q = self._event_q
        stop = self._collector_stop
        assert q is not None and stop is not None
        while not stop.is_set():
            try:
                item = q.get(timeout=0.05)  # type: ignore[attr-defined]
            except queue.Empty:
                continue
            self._dispatch(item)
        while True:  # final drain after the pool's work has settled
            try:
                item = q.get_nowait()  # type: ignore[attr-defined]
            except queue.Empty:
                break
            self._dispatch(item)

    def _dispatch(self, item: tuple[str, object]) -> None:
        monitor = self._run_monitor
        if monitor is None:
            return
        kind, payload = item
        if kind == "batch":  # a worker drain thread coalesces many events into one Manager put
            for sub in cast("list[tuple[str, object]]", payload):
                self._dispatch(sub)
            return
        with contextlib.suppress(Exception):
            if kind == "task":
                monitor.on_task(cast("TaskEvent", payload))
            elif kind == "profile":
                worker, data = cast("tuple[str, bytes]", payload)
                monitor.on_profile(worker, data)

    def _stop_collector(self) -> None:
        if self._collector is not None and self._collector_stop is not None:
            self._collector_stop.set()
            self._collector.join(timeout=5.0)
            self._collector = None

    def close(self) -> None:
        super().close()
        self._stop_collector()
        self._close_peer()
        if self._mgr is not None:
            self._mgr.shutdown()
            self._mgr = None
            self._event_q = None

    def _broadcast(self, pool: _PoolExecutor, token: str, payload: bytes) -> None:
        """Prime every worker with ``payload`` exactly once. concurrent.futures exposes no worker
        identity, so we submit priming tasks (each holds briefly, then returns its pid) until the
        set of pids covers the whole pool — the prime is idempotent, so extra hits are harmless."""
        if token in self._broadcast_tokens:
            self._broadcast_tokens.move_to_end(token)  # idempotent; keep recency
            return
        target = self.max_workers  # concrete (resolved in __init__) -- no private pool attribute
        seen: set[int] = set()
        rounds = 0
        while len(seen) < target and rounds < 1000:
            batch = [pool.submit(_prime_shared, token, payload) for _ in range(target - len(seen))]
            seen.update(f.result() for f in batch)  # a dead worker surfaces as BrokenProcessPool here
            rounds += 1
        if len(seen) < target:  # never silently mark a token primed without full coverage (P1-3)
            raise RuntimeError(
                f"broadcast reached only {len(seen)}/{target} workers after {rounds} rounds; "
                "refusing to cache an under-primed process (would KeyError on an unprimed worker)"
            )
        self._broadcast_tokens[token] = None
        while len(self._broadcast_tokens) > _SHARED_CACHE_CAP:  # FIFO, lockstep with the workers
            self._broadcast_tokens.popitem(last=False)


class ProcessPoolExecutor(_ProcessExecutorBase):
    """Process executor whose peer-reduction IPC uses a **full-registry** worker pool (the stdlib
    :class:`concurrent.futures.ProcessPoolExecutor`): every worker inherits every peer's queue via the
    pool initializer. Simple and fast — the right default up to roughly the per-process file-descriptor
    limit. Because inheritance is O(N²) in fds, on large many-core machines (>~128 cores, or any low
    ``RLIMIT_NOFILE``) it warns and you should switch to :class:`PinnedPoolExecutor` instead. This is
    the original M7 behaviour, now named explicitly so the pool choice is never hidden."""

    _peer_pool_is_pinned = False


class PinnedPoolExecutor(_ProcessExecutorBase):
    """Process executor whose peer-reduction IPC uses an **identity-pinned** worker pool
    (:class:`~graphed_executors.local._pinned_pool.PinnedProcessPool`): each worker is spawned once and
    inherits ONLY its own inbox plus its O(log N) overlay peers (segment-tree reduction targets +
    hypercube steal lifelines + the driver). The registry is therefore O(N log N), not O(N²), so it
    stays under the per-process fd limit — the executor to pick for large many-core machines. Identical
    results to :class:`ProcessPoolExecutor` (bit-for-bit), only the communication footprint differs."""

    _peer_pool_is_pinned = True


class ProcessExecutor(ProcessPoolExecutor):
    """Deprecated alias for :class:`ProcessPoolExecutor` (the full-registry pool — its original M7
    meaning). Kept for back-compat; pick :class:`ProcessPoolExecutor` or :class:`PinnedPoolExecutor`
    explicitly so the IPC pool is visible at the call site rather than chosen silently."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        warnings.warn(
            "ProcessExecutor is deprecated; use ProcessPoolExecutor (full-registry, the same behaviour) "
            "or PinnedPoolExecutor (identity-pinned, bounded O(log N) overlay) explicitly.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)
