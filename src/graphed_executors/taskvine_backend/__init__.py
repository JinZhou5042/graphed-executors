"""A graphed ``Executor`` that runs plans on TaskVine through VineGraph.

``TaskVineExecutor().run(plan)`` lowers a ``graphed.core.Plan`` into a VineGraph ``Workflow``:

- one leaf task per ``Task`` (``plan.process(partition, resources)``), leaves ordered by key;
- combine tasks laid out with ``graphed_executors.local.plan_tree`` — the SAME fixed binary tree the
  reference executors use, so float results are bit-for-bit identical to a local run;
- a worker failure is captured, short-circuits up the tree, and is re-raised on the driver as the
  original exception (with the remote traceback attached as a note);
- ``resources.open_once`` is available inside each call. The current TaskVine task-runner forks
  each call, so opened handles do not persist across tasks yet.

An adaptive plan (``plan.next_tasks``) runs in rounds: each batch from ``next_tasks`` is one
VineGraph run reduced to a single partial on the workers; the driver folds round results in order,
updates ``ExecContext`` (task counts, events, per-task durations) and checks ``plan.stop`` between
rounds.
"""

from __future__ import annotations

import dataclasses
import os
import time
from dataclasses import dataclass, field
from pathlib import Path

import cloudpickle
from graphed.core.execution import ExecContext, ExecResult, StopReason

from graphed_executors.local import plan_tree

from . import _task_runtime, _vinegraph_context

try:
    from ndcctools.taskvine.vine_graph import VineGraph, Workflow
    from ndcctools.taskvine.vine_graph.task_runner import TaskRunnerRegistration
except ModuleNotFoundError as exc:
    _TASKVINE_IMPORT_ERROR = exc
    VineGraph = object
    Workflow = None
    TaskRunnerRegistration = None
else:
    _TASKVINE_IMPORT_ERROR = None

__all__ = ["RunStats", "TaskVineExecutor", "TaskVineWorkerError"]

cloudpickle.register_pickle_by_value(_vinegraph_context)


def _require_taskvine():
    if _TASKVINE_IMPORT_ERROR is not None:
        raise ImportError(
            "graphed-executors TaskVine backend requires a build with ndcctools.taskvine.vine_graph"
        ) from _TASKVINE_IMPORT_ERROR


class TaskVineWorkerError(RuntimeError):
    """A worker failure whose exception could not be unpickled on the driver."""


class _GraphedVineGraph(VineGraph):
    """VineGraph whose task-runner library puts its sandbox on ``sys.path`` before loading."""

    def build_task_runner_registration(self, py_graph, bridge, hoisting_modules, env_files):
        registration = TaskRunnerRegistration(self)
        # The stock hoisting list copies _TaskOutputAttribute's source (decorated with
        # @dataclasses.dataclass) into library_code.py before anything imports `dataclasses`, so the
        # library dies with NameError on every worker. The import must come first.
        registration.hoisting_modules.insert(0, dataclasses)
        registration.add_hoisting_modules(hoisting_modules)
        registration.add_env_files(env_files)
        registration.set_context_loader(
            _vinegraph_context.context_loader, context_loader_args=[cloudpickle.dumps(py_graph)]
        )
        registration.set_cores(self.get_param("libcores"))
        registration.set_name(bridge.get_task_runner_library_name())
        return registration


@dataclass
class RunStats:
    """Plan-lowering and execution statistics for the most recent ``run``."""

    n_tasks: int = 0
    n_graph_nodes: int = 0
    n_rounds: int = 0
    lower_s: float = 0.0  # Plan -> Workflow
    vine_run_s: float = 0.0  # VineGraph.run wall time (includes library install + makespan)
    total_s: float = 0.0
    rounds: list = field(default_factory=list)


def _portable(partition):
    """A relative local uri means "relative to the driver's cwd", but every VineGraph task runs in
    its own sandbox. Resolve it on the driver so the partition means the same file everywhere."""
    uri = partition.uri
    if uri and "://" not in uri and not os.path.isabs(uri) and os.path.exists(uri):
        return dataclasses.replace(partition, uri=os.path.abspath(uri))
    return partition


def _sorted_unique_tasks(tasks, *, seen=()):
    """Return tasks in deterministic key order and reject ambiguous task identities."""
    ordered = sorted(tasks, key=lambda task: task.key)
    keys = [task.key for task in ordered]
    duplicates = {key for i, key in enumerate(keys[1:], start=1) if key == keys[i - 1]}
    duplicates.update(set(keys).intersection(seen))
    if duplicates:
        raise ValueError(f"task keys must be unique within a run: {sorted(duplicates)[:5]}")
    return ordered


def _raise_worker_error(partial):
    pickled, text, key = partial.error
    exc = None
    if pickled is not None:
        try:
            exc = cloudpickle.loads(pickled)
        except Exception:
            exc = None
    if isinstance(exc, BaseException):
        exc.add_note(
            f"[graphed-taskvine] raised on a TaskVine worker (task {key}); remote traceback:\n{text}"
        )
        raise exc
    raise TaskVineWorkerError(f"task {key} failed on a TaskVine worker:\n{text}")


class TaskVineExecutor:
    """Run graphed plans on TaskVine workers via VineGraph.

    Parameters
    ----------
    manager_name, port, run_info_path, run_info_template:
        Passed to the VineGraph manager (created lazily on first ``run``), unless ``manager`` is
        given. Workers/factories connect by ``manager_name``.
    local:
        ``True`` runs the lowered Workflow in-process (VineGraph ``local-execute``): no workers,
        same lowering, same reduction tree. For tests and graph-construction benchmarks.
    ship:
        Extra files/directories workers must import (analysis modules referenced by import ref,
        e.g. a ``"dv5_graphed:make_backend"`` backend). Install this package in the worker environment.
    params:
        Extra VineGraph parameters (``libcores``, ``wait-for-workers``, ``task-priority-mode``, ...).
    """

    def __init__(
        self,
        *,
        manager=None,
        manager_name="graphed-taskvine",
        port=(9100, 9199),
        run_info_path=None,
        run_info_template=None,
        local=False,
        libcores=16,
        wait_for_workers=0,
        work_dir=None,
        ship=(),
        params=None,
    ):
        # TaskVine's TCP catalog updates fork() from a threaded process; UDP avoids that path.
        os.environ.setdefault("CATALOG_UPDATE_PROTOCOL", "udp")
        self._manager = manager
        self._owns_manager = manager is None
        self._manager_args = {
            "port": list(port) if isinstance(port, (tuple, list)) else port,
            "name": manager_name,
            "run_info_path": run_info_path,
            "run_info_template": run_info_template,
        }
        self.local = local
        self.work_dir = Path(work_dir or Path.cwd() / "vine-graph-work").resolve()
        self.env_files = {}
        destinations = set()
        for path in ship:
            path = Path(path).resolve()
            if not path.exists():
                raise FileNotFoundError(path)
            if path.name in destinations:
                raise ValueError(f"duplicate worker sandbox destination: {path.name!r}")
            destinations.add(path.name)
            self.env_files[str(path)] = path.name
        self.params = {
            "libcores": libcores,
            "wait-for-workers": wait_for_workers,
            "local-execute": 1 if local else 0,
            "output-dir": str(self.work_dir / "outputs"),
            "checkpoint-dir": str(self.work_dir / "checkpoints"),
            "progress-bar-update-interval-sec": 1.0,
        }
        self.params.update(params or {})
        self.last_stats = RunStats()

    # ---- manager lifetime -------------------------------------------------------------------
    @property
    def manager(self):
        if self._manager is None:
            _require_taskvine()
            port = self._manager_args["port"]
            args = [port] if port is not None else []
            kwargs = {k: v for k, v in self._manager_args.items() if k != "port" and v is not None}
            self._manager = _GraphedVineGraph(*args, **kwargs)
        return self._manager

    def close(self):
        if self._owns_manager and self._manager is not None:
            manager, self._manager = self._manager, None
            del manager

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ---- graphed Executor protocol ------------------------------------------------------------
    def run(self, plan):
        start = time.perf_counter()
        self.last_stats = RunStats()
        try:
            if plan.next_tasks is not None:
                return self._run_adaptive(plan)
            tasks = _sorted_unique_tasks(plan.tasks)
            if not tasks:
                return ExecResult(plan.empty(), 0, 0, StopReason.EXHAUSTED)
            partial = self._run_tree(plan, tasks, collect_durations=False)
            if partial.error is not None:
                _raise_worker_error(partial)
            return ExecResult(partial.value, len(tasks), len(tasks) - 1, StopReason.EXHAUSTED)
        finally:
            self.last_stats.total_s = time.perf_counter() - start

    def lower(self, plan, tasks=None, *, collect_durations=False):
        """Plan -> (Workflow, root TaskHandle).

        This is an advanced inspection and benchmarking hook, not part of the stable executor
        interface.
        """
        tasks = _sorted_unique_tasks(plan.tasks if tasks is None else tasks)
        if not tasks:
            raise ValueError("cannot lower an empty task set; run(plan) returns plan.empty() directly")
        _require_taskvine()
        blob = cloudpickle.dumps((plan.process, plan.combine, plan.empty))
        workflow = Workflow()
        nodes = {
            i: workflow.add_task(
                _task_runtime.run_leaf, blob, t.key, _portable(t.partition), collect_durations
            )
            for i, t in enumerate(tasks)
        }
        combines, root = plan_tree(len(tasks))
        for out, a, b in combines:
            nodes[out] = workflow.add_task(
                _task_runtime.run_combine, blob, nodes[a].output(), nodes[b].output()
            )
        workflow.finalize()
        return workflow, nodes[root]

    # ---- internals ---------------------------------------------------------------------------
    def _run_tree(self, plan, tasks, *, collect_durations):
        t0 = time.perf_counter()
        workflow, root = self.lower(plan, tasks, collect_durations=collect_durations)
        t1 = time.perf_counter()
        for key in ("output-dir", "checkpoint-dir"):  # the C executor rejects a dir whose parent is missing
            Path(self.params[key]).mkdir(parents=True, exist_ok=True)
        results = self.manager.run(
            workflow,
            targets=[root],
            params=dict(self.params),
            env_files=dict(self.env_files),
        )
        t2 = time.perf_counter()
        stats = self.last_stats
        stats.n_tasks += len(tasks)
        stats.n_graph_nodes += len(workflow.task_dict)
        stats.n_rounds += 1
        stats.lower_s += t1 - t0
        stats.vine_run_s += t2 - t1
        stats.rounds.append({"tasks": len(tasks), "lower_s": t1 - t0, "vine_run_s": t2 - t1})
        if root not in results:
            raise TaskVineWorkerError("VineGraph returned no result for the reduction root")
        return results[root]

    def _run_adaptive(self, plan):
        ctx = ExecContext()
        start = time.perf_counter()
        values = []
        stopped = None
        seen = set()
        batch = plan.next_tasks(ctx)
        while batch:
            batch = _sorted_unique_tasks(batch, seen=seen)
            seen.update(t.key for t in batch)
            partial = self._run_tree(plan, batch, collect_durations=True)
            if partial.error is not None:
                _raise_worker_error(partial)
            values.append(partial.value)
            ctx.n_done += partial.n_leaves
            ctx.events_done += partial.n_entries
            ctx.last_durations.update(partial.durations or {})
            ctx.elapsed_s = time.perf_counter() - start
            reason = plan.stop.reason(ctx) if plan.stop else None
            if reason is not None:
                stopped = reason
                break
            batch = plan.next_tasks(ctx)
        if values:
            value = values[0]
            for partial_value in values[1:]:
                value = plan.combine(value, partial_value)
        else:
            value = plan.empty()
        n_combines = max(0, len(values) - 1)
        n_combines += sum(max(0, round_stats["tasks"] - 1) for round_stats in self.last_stats.rounds)
        return ExecResult(value, ctx.n_done, n_combines, stopped or StopReason.EXHAUSTED)
