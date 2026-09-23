"""Task-runner library context loader, pickled by value."""

import os
import sys

import cloudpickle

from . import _task_runtime


def context_loader(graph_pkl):
    # User-shipped analysis modules land in the library sandbox. The task runtime
    # is supplied by the installed graphed-executors package on each worker.
    sandbox = os.getcwd()
    if sandbox not in sys.path:
        sys.path.insert(0, sandbox)
    graph = cloudpickle.loads(graph_pkl)

    # Every function call forks from this library process, so anything done here is inherited by
    # all calls: unpickle each plan's (process, combine, empty) once and import graphed/awkward now,
    # instead of once per call.
    try:
        _task_runtime.prime(graph)
    except Exception as exc:  # priming is an optimization; calls still unpickle on demand
        print(f"graphed-taskvine: priming skipped: {exc!r}", file=sys.stderr)
    return {"graph": graph}
