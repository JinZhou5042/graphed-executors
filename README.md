# graphed-executors

Runners for `graphed` plans: the same analysis runs on your laptop's threads or processes, on
a dask cluster, or on a parsl HTEX pool — you change the runner, not the analysis.

- **The answer doesn't depend on where you ran it.** The order partial results are combined in
  is fixed up front, so your totals — even float histograms — come out bit-for-bit identical on
  1 worker or 100, threads or a cluster.
- **One slow file can't stall the run.** Every other part of the result keeps combining while a
  straggler finishes.
- **A failure on a worker comes back on your machine** as the exception it was, pointing at the
  analysis line you wrote — not an opaque string from another process.

## Install

```bash
pip install graphed-executors            # laptop runners; pulls graphed
pip install "graphed-executors[dask]"    # + the dask.distributed backend
pip install "graphed-executors[parsl]"   # + the parsl backend
pip install "graphed-executors[taskvine]" # + TaskVine adapter dependencies
```

Installing from source builds `graphed`'s Rust core, so you need a Rust toolchain; a plain
`pip install` from PyPI does not.

## Your first run

A plan is your analysis packaged for a runner: `process` does one chunk's work, `combine` merges
two partial results, `empty` is the starting value, and `tasks` lists the chunks. Here is a
minimal hand-made plan so you can see the whole loop; the next block gets one from a real
analysis instead:

```python
import numpy as np
from graphed.core import Partition, Plan, Task
from graphed_executors.local import ProcessPoolExecutor

def count(partition, resources):          # module-level, so workers can import it
    return np.asarray([partition.entry_stop - partition.entry_start])

def add(a, b): return a + b
def zero():    return np.zeros(1, dtype=int)

parts = tuple(Partition("data", "", i * 100, (i + 1) * 100) for i in range(7))
plan  = Plan(process=count, combine=add, empty=zero,
             tasks=tuple(Task(i, p) for i, p in enumerate(parts)))

if __name__ == "__main__":
    result = ProcessPoolExecutor(max_workers=4).run(plan)
    print(result.value)                   # [700]
```

### The same run, from a real analysis

You do not hand-build plans in practice. Fill a histogram with `graphed-histogram` and it exports
one; the runner is the same call. Needs `graphed-histogram` and `graphed[parquet]`:

```python
import awkward as ak
import boost_histogram as bh
import graphed_histogram as gh
from graphed import Session
from graphed.awkward import AwkwardBackend, from_parquet
from graphed_executors.local import ProcessPoolExecutor

if __name__ == "__main__":
    ak.to_parquet(ak.Array({"pt": [[40.0, 25.0], [55.0], [30.0, 60.0, 20.0],
                                   [80.0], [15.0, 45.0], [70.0, 10.0]]}), "events.parquet")

    session = Session(AwkwardBackend())
    events = from_parquet(session, "events", "events.parquet", steps_per_file=3)

    h = gh.boost.Histogram(bh.axis.Regular(4, 0.0, 100.0), storage=bh.storage.Int64())
    h.fill(events.pt)                     # records the fill; nothing is read yet

    plan = gh.plan({"jet_pt": h})         # your analysis, packaged for a runner
    result = ProcessPoolExecutor(max_workers=4).run(plan)
    print(gh.unpack(result.value)["jet_pt"].values())   # [3 4 3 1]
```

Swap `ProcessPoolExecutor` for `dask_runner(client)` or `parsl_runner(executor)` and the rest of
the program is untouched. If your output is not a histogram, `graphed.aggregate_plan(*outputs,
reduce=, combine=, empty=)` exports a plan the same way from any deferred `graphed` arrays.

## The one thing that's different

There is no `.compute()`. You build a plan (the `Plan`, `Task`, and `Partition` types live in
`graphed.core`, not in this package) and hand it to a runner; `run(plan)` returns a result whose
`.value` is the reduced answer. On process pools and clusters, `process`/`combine`/`empty` must
be module-level functions the workers can import — a lambda or a notebook-cell closure works on
`ThreadExecutor` only.

## Which runner do I want?

| You are running on | Use | Import from |
|---|---|---|
| One process, quick check, nothing picklable needed | `ThreadExecutor()` | `graphed_executors.local` |
| Your laptop, all cores | `ProcessPoolExecutor(max_workers=N)` | `graphed_executors.local` |
| A many-core machine where the worker count strains the open-file limit | `PinnedPoolExecutor` | `graphed_executors.local` |
| A dask cluster (local, dask-jobqueue, Kubernetes, …) | `dask_runner(client)` | `graphed_executors.dask_backend` |
| A parsl HTEX pool | `parsl_runner(executor)` | `graphed_executors.parsl_backend` |
| A TaskVine cluster | `TaskVineExecutor()` | `graphed_executors.taskvine_backend` |

(`import graphed_exec_local` still works as a deprecated alias for `graphed_executors.local`;
use the namespaced form in new code.)

The TaskVine backend requires a CCTools build that provides
`ndcctools.taskvine.vine_graph` and the `vine_worker` executable. Install this
`graphed-executors` package in the worker environment as well as on the driver.
The `[taskvine]` extra installs the Python adapter dependency, but not CCTools itself.
Direct HTCondor/Slurm submission isn't supported; use a TaskVine factory,
dask-jobqueue, or a parsl provider to reach those batch systems.

### Build VineGraph in a Conda environment

VineGraph currently lives on the CCTools `task-graph` development branch. The
released `ndcctools` package may not provide `ndcctools.taskvine.vine_graph`.
Build CCTools in its own Conda environment, then install this executor in that
same environment on the driver and workers:

```bash
git clone --branch task-graph --single-branch https://github.com/JinZhou5042/cctools.git cctools-src
cd cctools-src
unset PYTHONPATH
conda env create -y -f environment.yml
conda install -y -n cctools-dev --override-channels -c conda-forge --strict-channel-priority python=3.13
conda activate cctools-dev
./configure --with-base-dir "$CONDA_PREFIX" --prefix "$CONDA_PREFIX"
make -j4
make install
cd ../graphed-executors
python -m pip install -e ".[taskvine]"
python -c "from ndcctools.taskvine.vine_graph import VineGraph, Workflow"
vine_worker --version
```

Run this from a checkout of `graphed-executors`; the last `cd` assumes that
checkout sits beside `cctools-src`. After a release containing this backend,
`python -m pip install "graphed-executors[taskvine]"` can replace the editable
install. The [VineGraph guide](https://github.com/JinZhou5042/cctools/blob/task-graph/doc/manuals/taskvine/vine-graph.md)
covers workers and factories beyond the local example below.

### On TaskVine

[`examples/taskvine_basic.py`](examples/taskvine_basic.py) builds a small `Plan` and passes it to
`TaskVineExecutor.run(plan)`. Run it without a worker first:

```bash
python examples/taskvine_basic.py                 # prints 8
```

For a real worker on the same host, start the manager in one terminal and the worker in another:

```bash
python examples/taskvine_basic.py --cluster --port 9123
vine_worker --cores 1 localhost 9123
```

Activate the same Python environment in both terminals. It needs a CCTools build with VineGraph
and this `graphed-executors` package. To run your own analysis, replace the example's `Plan`
with one produced by graphed and keep the same `TaskVineExecutor.run(plan)` call.

This repository owns the TaskVine backend and its public interface. New HEP workflows, comparison
pipelines, and measured results are maintained in
[`cooperative-computing-lab/graphed-taskvine/baselines`](https://github.com/cooperative-computing-lab/graphed-taskvine/tree/master/baselines),
which imports this backend rather than copying its implementation.

### On a dask cluster

Needs `graphed-executors[dask]`. Point it at any `distributed.Client` you already have and hand
it the same `plan` from your first run:

```python
from distributed import Client
from graphed_executors.dask_backend import dask_runner

if __name__ == "__main__":                    # needed when Client() spawns a local cluster
    client = Client(n_workers=2, dashboard_address=":0")  # or Client("tcp://scheduler:8786")
    runner = dask_runner(client)              # registers graphed's worker plugin on your client
    print(runner.run(plan).value)             # [700] — same plan, same answer
    runner.close()                            # your client stays open; close it yourself
    client.close()
```

A failed task is retried on another worker three times before the error reaches you — that is
dask's own per-task retry, and `retries=3` is already the default. Pass `retries=0` while you are
debugging, so a deterministic bug in your `process` surfaces on the first attempt rather than the
fourth. A worker that dies mid-task surfaces as an error naming the task and the worker address,
not a hang.

### On a parsl pool

Needs `graphed-executors[parsl]`. HTEX workers are separate processes that do **not** inherit
your `sys.path`, so the plan's functions must live in a module they can import — not in your
launch script. Put them in a file of their own:

```python
# my_tasks.py — importable by the workers
import numpy as np

def count(partition, resources):
    return np.asarray([partition.entry_stop - partition.entry_start])

def add(a, b):
    return a + b

def zero():
    return np.zeros(1, dtype=int)
```

then hand `parsl_runner` any **started** parsl `HighThroughputExecutor` (or
`ThreadPoolExecutor`); `start_htex` spins one up locally if you don't have a config of your own:

```python
from graphed.core import Partition, Plan, Task
import my_tasks

parts = tuple(Partition("data", "", i * 100, (i + 1) * 100) for i in range(7))
plan = Plan(process=my_tasks.count, combine=my_tasks.add, empty=my_tasks.zero,
            tasks=tuple(Task(i, p) for i, p in enumerate(parts)))

if __name__ == "__main__":
    from graphed_executors.parsl_backend import parsl_runner, start_htex, stop_htex

    htex = start_htex(workers=8, run_dir="runinfo", heartbeat_period=2)
    try:
        with parsl_runner(htex) as runner:    # closing the runner does not stop your executor
            print(runner.run(plan).value)     # [700]
    finally:
        stop_htex(htex)                       # reaps the interchange, manager and worker
                                              # processes; skip it and you leak them, and
                                              # their ports, on any exception
```

Two things bite people on HTEX:

- **Workers don't inherit your `sys.path`.** Export `PYTHONPATH` (or install your package)
  before starting the pool, so the workers can import `my_tasks`. A plan whose functions are
  defined in the launch script itself doesn't fail fast — the run hangs.
- **A killed worker takes ~30 s to notice at parsl's default heartbeat.** `heartbeat_period=2`
  brings that to ~1.65 s, so a crash is reported (and the worker respawned) promptly.

## Useful knobs on the laptop runners

- `persistent=True` keeps the process pool alive across `run()` calls — worth it in notebooks
  and parameter sweeps, where the spawn cost would otherwise repeat per plan. Use it as a
  context manager: `with ProcessPoolExecutor(max_workers=4, persistent=True) as ex:` and call
  `ex.run(plan)` once per plan inside.
- Call `resources.open_once(uri, opener)` inside your `process` and the worker keeps that handle
  for its lifetime, so ten partitions of one file on one worker open it once instead of ten
  times. The dask backend gives you the same per-worker handle through its worker plugin.
- Every runner accepts `monitor=` — an observer that receives one event per task submitted,
  started, and finished, without changing the run. Pass `graphed.debug.Dashboard`'s monitor for
  a live web view.

The dashboard needs `pip install "graphed[dashboard]"`. Using the `plan` from your first run:

```python
from graphed.debug import Dashboard
from graphed_executors.local import ProcessPoolExecutor

if __name__ == "__main__":                    # a spawn pool re-imports this file
    with Dashboard(profile=True) as dash:
        result = ProcessPoolExecutor(max_workers=4, monitor=dash.monitor).run(plan)
    print(result.value)                       # [700]
```

## Next

- [How the executors work](docs/design.rst) — why the answer is identical everywhere, what
  happens when a worker dies, and where your combines actually run on each runner.
- [The dask backend](docs/dask.rst) and [the parsl backend](docs/parsl.rst) in depth,
  including repartitioning and joins on a cluster.
- [`graphed`](https://github.com/graphed-org/graphed) — build the analysis that produces a plan,
  and export it with `graphed.aggregate_plan`.
- [`graphed-histogram`](https://github.com/graphed-org/graphed-histogram) — deferred histogram
  filling, and `graphed_histogram.plan` for the block above.
- [API reference](docs/api.rst).
