"""Private worker-side task bodies for :class:`TaskVineExecutor`.

These functions run inside the VineGraph task-runner library on a TaskVine worker (or in-process
under ``local-execute``). They are referenced by import from the shipped graph, so the
graphed-executors package must be installed in the worker environment.

Two VineGraph argument rules shape this module:

- a positional tuple whose first element is callable is evaluated as a legacy dask task, so the
  plan's callables travel as ONE cloudpickled ``bytes`` blob (a leaf type, never walked or called)
  and are unpickled once per library process;
- dataclasses and tuples inside arguments are walked and copied on every call, so partial results
  travel wrapped in :class:`Partial` (a plain ``__slots__`` object the walker leaves alone).
"""

from __future__ import annotations

import hashlib
import sys
import time
import traceback

import cloudpickle
from graphed.core.execution import LocalResources

_FN_CACHE_ATTR = "_gtv_fn_cache"
_RESOURCES_ATTR = "_gtv_resources"


class Partial:
    """One subtree's contribution: the reduced value, or the first failure beneath it."""

    __slots__ = ("durations", "error", "n_entries", "n_leaves", "value")

    def __init__(self, value=None, error=None, n_leaves=0, n_entries=0, durations=None):
        self.value = value
        self.error = error  # None, or (pickled exception | None, traceback text, task key)
        self.n_leaves = n_leaves
        self.n_entries = n_entries
        self.durations = durations  # {task key: seconds} when the driver asked for timings

    def __getstate__(self):
        return {name: getattr(self, name) for name in self.__slots__}

    def __setstate__(self, state):
        for name in self.__slots__:
            setattr(self, name, state.get(name))


def plan_functions(blob):
    """(process, combine, empty) from the plan blob, unpickled once per library process."""
    cache = getattr(sys, _FN_CACHE_ATTR, None)
    if cache is None:
        cache = {}
        setattr(sys, _FN_CACHE_ATTR, cache)
    digest = hashlib.blake2b(blob, digest_size=16).digest()
    fns = cache.get(digest)
    if fns is None:
        fns = cloudpickle.loads(blob)
        cache[digest] = fns
    return fns


def prime(workflow):
    """Unpickle every distinct plan blob in ``workflow`` into the cache (library parent process).

    Task-runner calls are forked from the library process, so primed functions — and the graphed /
    awkward / uproot imports their unpickling pulls in — are inherited by every call."""
    seen = set()
    for _func_id, args, _kwargs in workflow.task_dict.values():
        blob = args[0] if args and isinstance(args[0], bytes) else None
        if blob is not None and id(blob) not in seen:
            seen.add(id(blob))
            plan_functions(blob)
    worker_resources()


def worker_resources():
    """Per-process ``open_once`` cache.

    Calls are forked from the library process, so a handle opened inside one call is not seen by the
    next: under TaskVine this is per-call, not per-worker, file locality (same as the old adaptor)."""
    resources = getattr(sys, _RESOURCES_ATTR, None)
    if resources is None:
        resources = LocalResources(max_open=128)
        setattr(sys, _RESOURCES_ATTR, resources)
    return resources


def _capture(exc, key):
    text = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    try:
        pickled = cloudpickle.dumps(exc)
    except Exception:
        pickled = None
    return (pickled, text, key)


def run_leaf(blob, key, partition, collect_durations):
    """Run ``plan.process`` on one partition."""
    t0 = time.perf_counter()
    try:
        process, _combine, _empty = plan_functions(blob)
        value = process(partition, worker_resources())
    except Exception as exc:
        return Partial(error=_capture(exc, key), n_leaves=1)
    durations = {key: time.perf_counter() - t0} if collect_durations else None
    return Partial(value=value, n_leaves=1, n_entries=partition.n_entries, durations=durations)


def run_combine(blob, left, right):
    """Run ``plan.combine`` on two subtrees; a failure below short-circuits (left first)."""
    if left.error is not None:
        return left
    if right.error is not None:
        return right
    try:
        _process, combine, _empty = plan_functions(blob)
        value = combine(left.value, right.value)
    except Exception as exc:
        return Partial(error=_capture(exc, "combine"), n_leaves=left.n_leaves + right.n_leaves)
    durations = None
    if left.durations is not None or right.durations is not None:
        durations = {**(left.durations or {}), **(right.durations or {})}
    return Partial(
        value=value,
        n_leaves=left.n_leaves + right.n_leaves,
        n_entries=left.n_entries + right.n_entries,
        durations=durations,
    )
