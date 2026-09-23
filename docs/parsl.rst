Running on a parsl pool
=======================

If your site gives you compute through `parsl <https://parsl-project.org>`_ — an HTEX pool over
SLURM, HTCondor, LSF, or a local block — graphed runs your plan on it. You start the executor, you
own it, and you hand it over; graphed submits to it directly rather than going through parsl's
DataFlowKernel.

This page is the how-to. For *why* the answer doesn't move when the worker count does, read
:doc:`design`.


Read these four before your first run
-------------------------------------

.. warning::

   **Your task functions must be importable on the workers.** HTEX workers are fresh Python
   processes launched by a console script; they do not inherit your driver's ``sys.path``, so a
   function defined in your driver's ``__main__`` fails to unpickle there with
   ``AttributeError: Can't get attribute 'count' on <module '__mp_main__'>``. Put ``process``,
   ``combine``, ``empty`` and any helper in an installed module. This is the single most common
   first-run failure.

.. warning::

   **Shuffles route through your submit node.** A parsl pool cannot address one worker from
   another, so by default every byte of a repartition or join crosses your submit host twice, and
   the whole shuffle is resident in the driver at the regroup point. Budget for that, or turn on
   the peer transport described below.

.. warning::

   **Your partial results come back through your submit node too.** HTEX resolves a task's
   arguments on the submit host, so a merge whose inputs are two workers' partials pulls both of
   them to the driver before it runs — every leaf partial and every intermediate crosses your
   submit host, not only shuffle blocks. On the laptop pools and on dask, workers merge among
   themselves and this does not happen. Keep a partial result small on a large parsl pool — a
   histogram is fine, a per-event array is not — or run the job on dask.

.. warning::

   **A worker killed outright takes about 30 seconds to be noticed**, because that is parsl's
   default ``heartbeat_period``. Until then your run just waits. Pass
   ``start_htex(..., heartbeat_period=2)`` and the same death surfaces in roughly 1.65 seconds
   (measured against ~29.7 s at the default).


Installing
----------

::

    pip install "graphed-executors[parsl]"    # pulls parsl>=2026.7.20

Only the parsl paths need the extra: the laptop executors and the code that runs a plan over any
backend install and work without it, and importing :mod:`graphed_executors.parsl_backend` does not
pull ``parsl`` into your process.

**Platform.** parsl's HTEX needs POSIX — there is no Windows support — and it launches workers via
the ``process_worker_pool`` and ``interchange.py`` console scripts, so run from an **activated
virtualenv** where those are on ``PATH``. Tested on CPython 3.12 and 3.13; the extra is not
installed on 3.14 or 3.14t, where parsl is unverified upstream.


Your first pool run
-------------------

Put your task functions in a module the workers can import. Anything ``pip install``-ed works; for
a quick trial, a file on ``PYTHONPATH`` is enough:

.. code-block:: python

    # my_tasks.py — on PYTHONPATH, or part of an installed package
    import numpy as np

    def count(partition, resources):
        return np.asarray([partition.entry_stop - partition.entry_start])

    def add(a, b):
        return a + b

    def zero():
        return np.zeros(1, dtype=int)

    def make_block(keys, field="v", values=None):
        dt = np.dtype([("__joinkey__", np.uint64), (field, np.int64)])
        block = np.zeros(len(keys), dtype=dt)
        block["__joinkey__"] = np.asarray(keys, dtype=np.uint64)
        block[field] = np.arange(len(keys)) if values is None else np.asarray(values)
        return block

:func:`~graphed_executors.parsl_backend.launch.start_htex` starts a ``HighThroughputExecutor``
with a fixed block — no scale-in, no strategy thread — and does the three setup steps parsl's
DataFlowKernel would otherwise do for you. Hand the started executor to
:func:`~graphed_executors.parsl_backend.backend.parsl_runner`:

.. code-block:: python

    import tempfile

    from graphed.core import Partition, Plan, Task
    from graphed_executors.parsl_backend import parsl_runner, start_htex, stop_htex
    from my_tasks import add, count, zero

    if __name__ == "__main__":
        parts = tuple(Partition("data", "", i * 100, (i + 1) * 100) for i in range(7))
        plan = Plan(process=count, combine=add, empty=zero,
                    tasks=tuple(Task(i, p) for i, p in enumerate(parts)))
        executor = start_htex(workers=2, run_dir=tempfile.mkdtemp())
        try:
            with parsl_runner(executor) as runner:
                result = runner.run(plan)
            print(result.value, result.n_partitions, result.n_combines)
        finally:
            stop_htex(executor)

Run it with ``PYTHONPATH`` covering ``my_tasks.py``; it prints::

    [700] 7 6

``Plan``, ``Task``, ``Partition`` and the ``ExecResult`` you get back all live in ``graphed.core``,
not in this package. ``result.value`` is the reduced result — bit-for-bit equal to a sequential
run, whatever the worker count and whatever order the tasks finished in.

Two ownership rules: leaving the ``with`` block closes the runner but **not** your executor, and
``stop_htex`` is what actually reaps the interchange, manager and worker processes. Skip it and
you leak processes and ports.

``runner.submit(plan)`` returns a future and runs the plan while you record the next one;
``parsl_runner(executor, max_in_flight=2)`` bounds how many submitted plans may be unfinished.

An exception raised inside your ``process`` comes back whole — see `When things fail`_.


Repartitioning and joining
--------------------------

Redistributing blocks by key — for a repartition or a join — goes through
``graphed_executors.parsl_backend.api``. The default engine sends every producer's blocks to the
driver, regroups them there, and sends each output partition back out:

.. code-block:: python

    import tempfile

    from graphed.numpy import NumpyBackend
    from graphed_executors.parsl_backend import ParslBackend, start_htex, stop_htex
    from graphed_executors.parsl_backend.api import run_join, run_repartition
    from my_tasks import make_block

    if __name__ == "__main__":
        src = [make_block([0, 1, 2, 3, 4, 5, 6, 7]),
               make_block([7, 6, 5, 4, 3, 2, 1, 0]),
               make_block([100, 200, 300, 400])]
        left = [make_block([0, 1, 2, 3, 4, 5], "lval", [10, 11, 12, 13, 14, 15]),
                make_block([1, 1, 2, 2, 13, 13], "lval", [22, 23, 24, 25, 32, 33])]
        right = [make_block([0, 0, 1, 2, 3, 5], "rval", [100, 101, 102, 103, 104, 105]),
                 make_block([3, 3, 2, 17, 17, 4], "rval", [106, 107, 108, 116, 117, 111])]
        executor = start_htex(workers=2, run_dir=tempfile.mkdtemp())
        try:
            pbackend = ParslBackend(executor)

            shuffled = run_repartition(NumpyBackend(), src, 4, pbackend=pbackend)
            print({d: len(b) for d, b in sorted(shuffled.value.items())})
            print(shuffled.witness.head_node_routed, shuffled.witness.driver_relay_bytes)

            joined = run_join(NumpyBackend(), left, right, 2,
                              on=("__joinkey__",), how="inner", pbackend=pbackend)
            print(sum(len(block) for block in joined.value.values()))
        finally:
            stop_htex(executor)

Prints::

    {0: 5, 1: 7, 2: 6, 3: 2}
    True 1088
    16

Alongside the blocks, every exchange returns counters for what it actually did — the result's
``witness`` — and those middle two numbers are the cost of the shuffle warning above, measured.
``head_node_routed`` is ``True`` and ``driver_relay_bytes`` is how many bytes the driver actually
held — per run, in the result, not just in the docs. The scheduler only sees one task per
producer plus one per output partition, and no intermediate pick tasks, so the submit *load* is
light; it is the *data* that crosses your submit host.

The blocks that come out are byte-identical to what the same inputs produce on your laptop and on
a dask cluster, so the engine you happen to run on can never change an analysis result.

The result carries ``.value`` (``{dest_pid: block}``), ``.dest_block_hashes`` (the sha256 of each
output block's serialized bytes — this is what is equal across engines) and ``.witness``.

``on``, ``how``, ``broadcast``, ``salt`` and ``mem_budget_bytes`` mean the same thing they do on
dask; ``mem_budget_bytes`` spills duplicated output partitions rather than accumulating them.
``broadcast=None`` (the default) decides broadcast-vs-shuffle from the plan — from ``parts``, never
from the live worker count — so the same logical join makes the same choice on any pool size.

A ``how`` other than ``"inner"`` produces masked, null-filled blocks that ``graphed.numpy`` moves
over Arrow, so install ``graphed[parquet]`` for those.


Keeping the data off your submit node
-------------------------------------

On an HTEX pool you can opt into a peer exchange instead. ``shuffle_method="transport"`` seats one
long-lived task per worker slot; each mints an HTTP endpoint in-task, registers it with the driver,
and waits at a barrier until every peer has registered. Only then does the driver hand out the
address book, so no send can race a missing inbox. Blocks then travel worker-to-worker directly —
coalesced to one request per holding worker, so a large pool does not stampede a single worker —
and the driver sees control traffic only.

Whether two workers on your site can actually dial each other is not something parsl guarantees, so
graphed checks before any data moves. You can run that check yourself first:

.. code-block:: python

    import tempfile

    from graphed.numpy import NumpyBackend
    from graphed_executors.parsl_backend import ParslBackend, start_htex, stop_htex
    from graphed_executors.parsl_backend.api import probe_peer_reachability, run_repartition
    from my_tasks import make_block

    if __name__ == "__main__":
        src = [make_block([0, 1, 2, 3, 4, 5, 6, 7]),
               make_block([7, 6, 5, 4, 3, 2, 1, 0]),
               make_block([100, 200, 300, 400])]
        executor = start_htex(workers=2, run_dir=tempfile.mkdtemp())
        try:
            pbackend = ParslBackend(executor)
            print(pbackend.peer_transport)

            report = probe_peer_reachability(pbackend, k=2)
            print(report.ok, report.failed_pairs)

            peered = run_repartition(NumpyBackend(), src, 4, pbackend=pbackend,
                                     shuffle_method="transport")
            print({d: len(b) for d, b in sorted(peered.value.items())})

            relayed = run_repartition(NumpyBackend(), src, 4, pbackend=pbackend)
            print(peered.dest_block_hashes == relayed.dest_block_hashes)
        finally:
            stop_htex(executor)

Prints::

    True
    True ()
    {0: 5, 1: 7, 2: 6, 3: 2}
    True

The probe seats the peers, tests every pair, releases them, and leaves the pool usable — run it
once when you move to a new site. ``report.failed_pairs`` names the addresses that could not
connect, which is usually a firewall answer rather than a graphed one.

If the check fails during a real run, ``on_unreachable`` decides what happens: ``"error"`` (the
default) raises a ``StageError`` naming the unreachable pair, and ``"fallback"`` re-runs the same
inputs through the submit node and records why in ``witness.fallback_reason`` — slower, but never
silent.

``shuffle_method="transport"`` is only available on an HTEX pool, which is what
``pbackend.peer_transport`` tells you. On a parsl ``ThreadPoolExecutor`` — whose "workers" are
threads in your own process — it raises ``NotImplementedError`` pointing at ``"tasks"``, rather
than pretending a loopback exchange is a peer exchange.

The transport-only knobs are ``fetch_budget_bytes``, ``pull_timeout_s``,
``epoch_restarts_allowed``, ``workers``, ``on_unreachable`` and ``registry_rewrite``. Set one while
the run resolves to the submit-node engine and you get a ``ValueError`` naming it, before anything
is submitted — never a silent no-op.

.. note::

   ``shuffle_method="auto"`` always resolves to the submit-node engine on parsl, because no parsl
   executor advertises both worker pinning and peer data movement. You have to ask for the peer
   exchange: it needs worker-to-worker reachability your site may not give you.


When things fail
----------------

**An exception in your code** comes back whole. A ``graphed.debug.StageError`` pickles across the
real HTEX process boundary and re-raises in your driver still pointing at the line in your analysis
that raised it — not an opaque parsl wrapper.

**A worker killed outright** (SIGKILL, OOM, preemption) surfaces as parsl's ``WorkerLost`` once the
watchdog notices, and parsl respawns the worker in place, so the pool stays usable. The delay
before you hear about it is ``heartbeat_period``: about 30 seconds at parsl's default, about 1.65
seconds if you pass ``start_htex(..., heartbeat_period=2)``. graphed recognises ``WorkerLost`` and
``ManagerLost`` anywhere in the exception chain and turns them into a ``StageError`` naming the
task and the worker.

Under the peer exchange the same death restarts the whole shuffle onto the surviving workers, up
to ``epoch_restarts_allowed`` (default 1), each restart tagged with a fresh run generation so
leftovers from the failed attempt can never be picked up. Exhausting that budget raises an
attributed ``StageError`` naming the death signal — never a raw parsl exception, and never a hang.


Watching a run
--------------

Pass any object with the ``graphed.core.execution.Monitor`` shape to ``parsl_runner`` and you get
task events with the same shape as everywhere else — ``submitted`` from the driver, ``started`` and
``finished`` from the worker. parsl has no worker-to-driver event channel, so a worker buffers its
events and they arrive when that task's result is unwrapped. You see the run task by task as tasks
complete, not live within a task. As on every backend, emission is off the data path: your result
is byte-identical whether a monitor is attached, absent, or raising on every event.


Not supported yet
-----------------

* **Live event delivery.** Events arrive at task completion, as above, so a dashboard fed from a
  parsl run updates in steps rather than continuously.
* **Windows**, and CPython 3.14 / 3.14t, following parsl's own support.
* **No checkpoint/resume on a pool.** ``run_resumable`` and ``run_shuffle_resumable`` drive
  themselves over a content-addressed store on the local filesystem; there is no distributed store
  behind them yet.
