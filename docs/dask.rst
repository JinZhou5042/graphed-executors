Running on a dask cluster
=========================

You already have a ``dask.distributed`` cluster — a ``LocalCluster`` on your laptop, a
``SLURMCluster``, an LPC HTCondor pool. Hand graphed a connected ``Client`` and it runs your plan
there: same result, same combine order, same numbers as a sequential run.

This page is the how-to. For *why* the answer doesn't move when the worker count does, read
:doc:`design`.


Installing
----------

::

    pip install "graphed-executors[dask]"    # pulls dask[distributed]>=2026.6

Only the dask paths need the extra. The laptop executors and the code that runs a plan over any
backend install and work without it, and importing :mod:`graphed_executors.dask_backend` does not
pull ``distributed`` into your process — that happens when you build a backend, so a missing
extra tells you at construction with a message naming the install.

Two things worth checking before a long run:

* Client and workers need the same ``dask``, ``distributed`` and ``graphed`` versions —
  ``client.get_versions(check=True)`` says so in one line.
* **Don't run the dask backend on free-threaded CPython (3.14t).** ``distributed`` declares
  support up to 3.14 and says nothing about free-threading, so the dask paths are untested there.
  The laptop executors do run on 3.14t.


Your first cluster run
----------------------

You build the cluster; graphed consumes the ``Client``.
:func:`~graphed_executors.dask_backend.backend.dask_runner` installs graphed's per-worker plugin
and hands you back a runner:

.. code-block:: python

    import numpy as np
    from distributed import Client, LocalCluster

    from graphed.core import Partition, Plan, Task
    from graphed_executors.dask_backend import dask_runner

    def count(partition, resources):
        return np.asarray([partition.entry_stop - partition.entry_start])

    def add(a, b):
        return a + b

    def zero():
        return np.zeros(1, dtype=int)

    if __name__ == "__main__":
        parts = tuple(Partition("data", "", i * 100, (i + 1) * 100) for i in range(7))
        plan = Plan(process=count, combine=add, empty=zero,
                    tasks=tuple(Task(i, p) for i, p in enumerate(parts)))
        with (
            LocalCluster(n_workers=2, threads_per_worker=1, processes=True,
                         dashboard_address=":0") as cluster,
            Client(cluster) as client,
        ):
            with dask_runner(client) as runner:
                result = runner.run(plan)

        print(result.value)
        print(result.n_partitions, result.n_combines)

Prints::

    [700]
    7 6

``Plan``, ``Task``, ``Partition`` and the ``ExecResult`` you get back all live in ``graphed.core``,
not in this package. ``result.value`` is the reduced result — bit-for-bit equal to a sequential
run, whatever the worker count and whatever order the tasks finished in — alongside
``.n_partitions`` and ``.n_combines``.

Four things that save an afternoon:

* ``process`` / ``combine`` / ``empty`` are pickled to the workers, so define them at module level
  (a ``functools.partial`` of a module-level function, or a frozen dataclass, is fine too), and
  guard your driver code with ``if __name__ == "__main__":``.
* On a real cluster prefer process workers with ``threads_per_worker=1`` — HEP stages hold the
  GIL inside compiled kernels — and ``dashboard_address=":0"`` so a second run doesn't collide on
  the dashboard port.
* ``dask_runner`` forwards ``retries`` to dask's own per-task retry, which resubmits on another
  worker, and there is no second retry loop on top of it. **It defaults to 3**, so a
  deterministic exception in your ``process`` runs four times before it reaches you — pass
  ``retries=0`` while debugging and it surfaces on the first attempt.
* Leaving the ``with`` block closes the runner, not your client. You built the cluster; you own it.
* ``runner.submit(plan)`` returns a future and runs the plan while you record the next one;
  ``dask_runner(client, max_in_flight=2)`` bounds how many submitted plans may be unfinished.

A plan with ``next_tasks`` runs on the adaptive path here exactly as it does on your laptop. A
worker exception comes back intact — see `When things fail`_.

Run the same plan with no extra dependencies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The runner is one engine over any backend, and
:class:`~graphed_executors.submit.threadpool.ThreadBackend` is a backend built on the standard
library alone. Same plan, same tree, same value, nothing installed:

.. code-block:: python

    import numpy as np

    from graphed.core import Partition, Plan, Task
    from graphed_executors.submit import SubmitRunner, ThreadBackend

    def count(partition, resources):
        return np.asarray([partition.entry_stop - partition.entry_start])

    def add(a, b):
        return a + b

    def zero():
        return np.zeros(1, dtype=int)

    parts = tuple(Partition("data", "", i * 100, (i + 1) * 100) for i in range(7))
    plan = Plan(process=count, combine=add, empty=zero,
                tasks=tuple(Task(i, p) for i, p in enumerate(parts)))

    with SubmitRunner(ThreadBackend(max_workers=4)) as runner:
        result = runner.run(plan)

    print(result.value, result.n_partitions, result.n_combines)

Prints::

    [700] 7 6

Useful when you want to reproduce a cluster result on a machine with nothing installed, or to
check whether a failure is yours or the cluster's.


Repartitioning and joining
--------------------------

Redistributing blocks by key — for a repartition or a join — is the one operation that moves bulk
data between workers. Two engines can do it, and you pick between them with one word:
``shuffle_method``.

Before any run that moves blocks worker-to-worker, register the transport plugin once per client
with :func:`~graphed_executors.dask_backend.transport.dask_transport_setup`. It is idempotent, and
it is also what the default (``"auto"``) needs on a plain ``DaskBackend``:

.. code-block:: python

    import numpy as np
    from distributed import Client, LocalCluster

    from graphed.numpy import NumpyBackend
    from graphed_executors.dask_backend import DaskBackend, run_repartition
    from graphed_executors.dask_backend.transport import dask_transport_setup

    def make_block(keys, field="v", values=None):
        dt = np.dtype([("__joinkey__", np.uint64), (field, np.int64)])
        block = np.zeros(len(keys), dtype=dt)
        block["__joinkey__"] = np.asarray(keys, dtype=np.uint64)
        block[field] = np.arange(len(keys)) if values is None else np.asarray(values)
        return block

    if __name__ == "__main__":
        src = [make_block([0, 1, 2, 3, 4, 5, 6, 7]),
               make_block([7, 6, 5, 4, 3, 2, 1, 0]),
               make_block([100, 200, 300, 400])]
        with (
            LocalCluster(n_workers=2, threads_per_worker=1, processes=True,
                         dashboard_address=":0") as cluster,
            Client(cluster) as client,
        ):
            dask_transport_setup(client)
            dbackend = DaskBackend(client)

            auto = run_repartition(NumpyBackend(), src, 4, dbackend=dbackend)
            tasks = run_repartition(NumpyBackend(), src, 4, dbackend=dbackend,
                                    shuffle_method="tasks")

        print(hasattr(auto, "transport"), hasattr(tasks, "partitions"))
        print(auto.dest_block_hashes == tasks.dest_block_hashes)
        print({d: len(b) for d, b in sorted(auto.value.items())})

Prints::

    True True
    True
    {0: 5, 1: 7, 2: 6, 3: 2}

That middle line is the point of the whole section. Both engines — and the single-machine engine
on your laptop — produce **byte-identical output blocks** on identical inputs, so switching
engines can never change an analysis result. Only the cost changes.

Joins go through the same door, and ``mem_budget_bytes`` bounds the working set on either engine:
duplicated output partitions spill instead of piling up.

.. code-block:: python

    import numpy as np
    from distributed import Client, LocalCluster

    from graphed.numpy import NumpyBackend
    from graphed_executors.dask_backend import DaskBackend, run_join, run_repartition
    from graphed_executors.dask_backend.transport import dask_transport_setup

    def make_block(keys, field="v", values=None):
        dt = np.dtype([("__joinkey__", np.uint64), (field, np.int64)])
        block = np.zeros(len(keys), dtype=dt)
        block["__joinkey__"] = np.asarray(keys, dtype=np.uint64)
        block[field] = np.arange(len(keys)) if values is None else np.asarray(values)
        return block

    if __name__ == "__main__":
        left = [make_block([0, 1, 2, 3, 4, 5], "lval", [10, 11, 12, 13, 14, 15]),
                make_block([1, 1, 2, 2, 13, 13], "lval", [22, 23, 24, 25, 32, 33])]
        right = [make_block([0, 0, 1, 2, 3, 5], "rval", [100, 101, 102, 103, 104, 105]),
                 make_block([3, 3, 2, 17, 17, 4], "rval", [106, 107, 108, 116, 117, 111])]
        with (
            LocalCluster(n_workers=2, threads_per_worker=1, processes=True,
                         dashboard_address=":0") as cluster,
            Client(cluster) as client,
        ):
            dask_transport_setup(client)
            dbackend = DaskBackend(client)
            joined = run_join(NumpyBackend(), left, right, 2,
                              on=("__joinkey__",), how="inner", dbackend=dbackend,
                              mem_budget_bytes=1 << 20)
            print(sum(len(block) for block in joined.value.values()))

            try:
                run_repartition(NumpyBackend(), left, 4, dbackend=dbackend,
                                shuffle_method="tasks", holder_budget_bytes=1 << 20)
            except ValueError as exc:
                print(f"ValueError: {exc}")

Prints::

    16
    ValueError: holder_budget_bytes applies only to shuffle_method='transport' (resolved: 'tasks')

That second half matters as much as the first: a knob only one engine honours is a loud error when
the other engine is the one that ran, never a silent no-op. The check runs *after* the engine is
chosen, so ``"auto"`` landing on the other engine raises too.

``broadcast=None`` (the default) lets graphed decide broadcast-vs-shuffle from the plan — from
``parts``, never from the live worker count — so the same logical join makes the same choice on a
4-worker cluster and a 400-worker one. Pass ``True`` or ``False`` to record the choice yourself.

Reading the result
~~~~~~~~~~~~~~~~~~

Whichever engine ran, code against these three attributes:

* ``.value`` — ``{dest_pid: block}``, the output blocks.
* ``.dest_block_hashes`` — ``{dest_pid: sha256}`` of each output block's serialized bytes. This is
  what is equal across engines and across runs.
* ``.witness`` — counters describing what the run actually did: spills, buffer peaks, whether the
  join broadcast.

Each engine adds one attribute of its own, which is how you tell after the fact which one ran:
``.transport`` means blocks moved worker-to-worker, ``.partitions`` means they moved as ordinary
tasks. That is the first thing to check when ``"auto"`` was slower than you expected.

If you need ``monitor=`` or ``retries=`` on a shuffle, call the engines directly —
:func:`~graphed_executors.dask_backend.shuffle.dask_run_repartition` /
:func:`~graphed_executors.dask_backend.shuffle.dask_run_join` take a ``runner=``, and
:func:`~graphed_executors.dask_backend.transport_shuffle.transport_run_repartition` /
:func:`~graphed_executors.dask_backend.transport_shuffle.transport_run_join` take a ``dbackend=``.
The one-knob front door always builds its own runner.


Choosing a shuffle engine
-------------------------

Since both engines give the same bytes, the choice is entirely about the shape of your cluster.

.. list-table::
   :header-rows: 1
   :widths: 30 22 48

   * - Your situation
     - ``shuffle_method``
     - Why
   * - Fixed-size cluster, large shuffles
     - ``"auto"`` (→ worker-to-worker)
     - Bulk bytes go straight from the worker that produced them to the worker that needs them.
       The scheduler sees a graph that grows with the number of tasks plus the number of outputs,
       not their product, so it is not the bottleneck at scale.
   * - Adaptive or elastic cluster (workers join and leave)
     - ``"tasks"`` explicitly
     - Blocks move as ordinary dask tasks, so when a worker leaves the scheduler simply recomputes
       its blocks from their inputs. The worker-to-worker engine pins tasks to specific workers,
       so a departing worker costs a whole-run restart.
   * - A backend that can't pin tasks, but can move data peer-to-peer
     - ``"auto"`` (→ tasks)
     - ``"auto"`` picks the engine that can actually run there. Forcing ``"transport"`` raises
       ``NotImplementedError`` before anything is submitted.
   * - A backend that can't move data peer-to-peer at all
     - neither engine here
     - ``"auto"`` sends it to ``"tasks"``, and that engine refuses it up front with
       ``NotImplementedError`` — it moves blocks between workers too. That shape needs an
       exchange relayed through the submit node, which is what the parsl backend provides; see
       :doc:`parsl`.
   * - Small shuffles, no preference
     - ``"auto"``
     - Either finishes quickly; let the default pick.

``"auto"`` reads the backend's declared capabilities and nothing else. It does **not** notice that
your cluster is adaptive — that is the one case where you should say ``"tasks"`` yourself.


Budgets and knobs
-----------------

``salt`` (for skew mitigation) works on both operations and both engines. On joins, so do ``on``,
``how``, ``broadcast`` and ``mem_budget_bytes``. These are the knobs the worker-to-worker engine
adds, and what each one actually bounds:

.. list-table::
   :header-rows: 1
   :widths: 26 74

   * - Knob
     - What it bounds
   * - ``n_tasks``
     - How many producer tasks write blocks (default: ``min(n_workers, n_src)``). Each producer
       writes one block per output partition, so this is one side of the transfer count.
   * - ``holder_budget_bytes``
     - How many bytes of produced blocks a worker keeps in memory before spilling them to its
       local disk. Overflow really does hit disk; the witness reports it as
       ``holder_spill_count`` and ``peak_holder_bytes``.
   * - ``fetch_budget_bytes`` / ``disk_budget_bytes``
     - The read side's accounting — see the note below for what these do and don't do.
   * - ``pull_timeout_s``
     - How long one worker waits for a block from another. If your blocks are legitimately large,
       widen this rather than letting spurious timeouts eat your restart budget.
   * - ``epoch_restarts_allowed``
     - How many times a failure may restart the whole shuffle before it gives up and raises
       (default: 1). See `When things fail`_.

.. note::

   **What the read budgets actually limit.** ``fetch_budget_bytes`` and ``disk_budget_bytes``
   bound a driver-side accounting pass over block sizes — which is how this engine's counters stay
   exactly equal to the single-machine engine's — and they are reported in the witness. They do
   **not** throttle a worker while it is pulling: the real gather fetches its fragments, holds one
   output partition resident, concatenates and returns. ``holder_budget_bytes`` on the write side
   *is* a live runtime bound with real spilling. If you are trying to cap memory on a shuffle,
   ``holder_budget_bytes`` is the knob.


When things fail
----------------

**An exception in your code** comes back whole. A ``graphed.debug.StageError`` pickles across the
worker boundary and re-raises in your driver still pointing at the line in your analysis that
raised it — not an opaque scheduler string.

**A worker that dies hard** — segfault, OOM, preemption — reaches dask as
``distributed.KilledWorker`` once it has died ``distributed.scheduler.allowed-failures`` times.
graphed turns that into a ``StageError`` naming the partition and the last worker that held it.
Under co-located tasks the blame can land on an innocent partition, so treat the attribution as a
strong hint rather than a verdict.

The two shuffle engines recover differently, and this is the same fixed-versus-elastic fork as
above:

* **Blocks as tasks** — recovery is dask's own. A dying worker loses its blocks, the scheduler
  recomputes them from their inputs, and the result is unchanged bit for bit. Per-task ``retries``
  map onto dask's native resubmit.
* **Worker-to-worker** — the whole shuffle restarts on the surviving workers. A delivery that ran
  out of attempts (``TransportDeliveryError``), a block pull that timed out
  (``PullTimeoutError``), or a ``KilledWorker`` each trigger one restart, up to
  ``epoch_restarts_allowed``. What to do about each: a ``PullTimeoutError`` on large blocks means
  raise ``pull_timeout_s``; a ``TransportDeliveryError`` or repeated ``KilledWorker`` means the
  cluster is shedding workers, so switch to ``shuffle_method="tasks"``. Restarts are tagged with a
  fresh run generation, so leftover blocks from the failed attempt can never be mistaken for good
  ones, and exhausting the budget raises an attributed ``StageError`` naming the victim — never a
  raw ``KilledWorker`` and never a hang.


Watching a run
--------------

Pass any object with the ``graphed.core.execution.Monitor`` shape to ``dask_runner`` and you get
one event per task as it is submitted, starts, finishes or errors, delivered over dask's
structured-event channel on a topic private to that run:

.. code-block:: python

    import numpy as np
    from distributed import Client, LocalCluster

    from graphed.core import Partition, Plan, Task
    from graphed_executors.dask_backend import dask_runner

    def count(partition, resources):
        return np.asarray([partition.entry_stop - partition.entry_start])

    def add(a, b):
        return a + b

    def zero():
        return np.zeros(1, dtype=int)

    class ListMonitor:
        """Collects task events. A monitor must never raise or block the run."""

        def __init__(self):
            self.events = []

        def on_task(self, event):
            self.events.append((str(event.phase), event.key, event.worker))

        def on_profile(self, worker, payload):
            pass

        def on_combine(self, leaves_done):
            pass

        def worker_profiler_factory(self):
            return None

    if __name__ == "__main__":
        parts = tuple(Partition("data", "", i * 100, (i + 1) * 100) for i in range(7))
        plan = Plan(process=count, combine=add, empty=zero,
                    tasks=tuple(Task(i, p) for i, p in enumerate(parts)))
        monitor = ListMonitor()
        with (
            LocalCluster(n_workers=2, threads_per_worker=1, processes=True,
                         dashboard_address=":0") as cluster,
            Client(cluster) as client,
        ):
            with dask_runner(client, monitor=monitor) as runner:
                result = runner.run(plan)

        print(result.value)
        print(sorted({phase for phase, _key, _worker in monitor.events}))

Prints::

    [700]
    ['finished', 'started', 'submitted']

Watching costs you nothing: emission sits off the data path and swallows its own errors, so
``result.value`` is byte-identical whether a monitor is attached, absent, or actively raising on
every event. If you are writing your own backend, the same tap is
``SubmitBackend.subscribe_events(topic, handler)``.

One gap: the worker-to-worker engine's peer-reduction path accepts ``monitor=`` for signature
parity but does not emit events over the transport yet.


Deploying on batch clusters
---------------------------

graphed consumes a connected ``Client`` and nothing else, so any launcher that produces one works
unchanged. The two sketches below **need a real batch cluster — they are recipes, not runnable
examples.** The guidance around them is what matters.

SLURM via `dask-jobqueue <https://jobqueue.dask.org>`_:

.. code-block:: python

    from dask_jobqueue import SLURMCluster
    from distributed import Client
    from graphed_executors.dask_backend import dask_runner

    cluster = SLURMCluster(
        cores=8, memory="16GB", walltime="01:00:00", interface="ib0",
        local_directory="/scratch/$USER",          # node-local, never a network mount
        worker_extra_args=["--lifetime", "55m", "--lifetime-stagger", "4m"],
    )
    cluster.scale(jobs=20)                         # jobs, not workers
    with Client(cluster) as client, dask_runner(client) as runner:
        result = runner.run(plan)

Fermilab LPC HTCondor via `lpcjobqueue <https://github.com/CoffeaTeam/lpcjobqueue>`_:

.. code-block:: python

    from distributed import Client
    from lpcjobqueue import LPCCondorCluster
    from graphed_executors.dask_backend import dask_runner

    cluster = LPCCondorCluster(ship_env=True)      # ships your venv into the singularity image
    cluster.scale(50)
    with Client(cluster) as client, dask_runner(client) as runner:
        result = runner.run(plan)

``graphed-executors`` depends on neither package — these are patterns, not APIs. On queues that
preempt:

* Set ``--lifetime`` strictly below the walltime so workers drain and migrate their keys instead
  of dying hard.
* Raise ``allowed-failures`` to 5–10.
* For long shuffles, keep the lifetime comfortably above one producer task's runtime, or an
  eviction forces the same work to be recomputed over and over.

**Broadcast joins on elastic or preemption-prone clusters need one more flag:**
``dask_runner(client, replicate_broadcast=True)``. A broadcast join places the small side as a
single future every task references, and ``distributed`` drops the last replica of a key when the
worker holding it leaves — down-scale or eviction — which fails every dependent task
(`coffea#1490 <https://github.com/scikit-hep/coffea/issues/1490>`_). The flag replicates that
future so it survives a lost holder. It defaults to ``False``, which is one copy and the lowest
memory, and is correct on a fixed cluster that never loses the holder. It is a constructor knob,
not a per-shuffle one — whether a join broadcasts at all is still decided from the plan via
``broadcast=``.


Not supported yet
-----------------

* **Per-task resource requests are accepted and ignored.** ``DaskBackend.submit`` takes
  ``resources=`` but does not forward it, because dask treats resources as a hard constraint and
  an unsatisfiable request would park the task in no-worker state forever. Shape the cluster
  uniformly instead; opt-in enforcement is future work.
* **The worker-to-worker engine and adaptive down-scaling do not mix.** Its worker pins mean a
  departing owner costs a whole-run restart. Pass ``shuffle_method="tasks"`` on elastic clusters.
* **No free-threaded (3.14t) support on the dask path**, because ``distributed`` declares none.
  The laptop executors do support it.
* **No peer-mode telemetry** from the worker-to-worker reduction path, as noted under
  `Watching a run`_.
* **No checkpoint/resume on a cluster.** ``run_resumable`` and ``run_shuffle_resumable`` drive
  themselves over a content-addressed store on the local filesystem; there is no distributed store
  behind them yet.
