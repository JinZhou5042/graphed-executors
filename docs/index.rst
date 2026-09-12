graphed-executors
=================

This is the package that **runs** a ``graphed`` analysis. You export your analysis as a plan —
``graphed_histogram.plan(histograms)`` when the output is histograms, ``graphed.aggregate_plan``
when it is anything else — hand that plan to a runner from here, and get one reduced result
back: a histogram, an array, a dictionary of both.

The same plan runs everywhere. A process pool on your laptop, a ``dask.distributed`` cluster,
a `parsl <https://parsl-project.org>`_ pool on a batch system: one line changes, and the
number you get back does not.

A plan is four things the runner uses and never looks inside: ``process(partition,
resources)`` does one chunk of work, ``combine(a, b)`` merges two partial results, ``empty()``
is what you start from, and ``tasks`` is the fixed set of chunks. Those types — ``Plan``,
``Task``, ``Partition``, and the ``ExecResult`` you get back — live in ``graphed.core``, not
here.

Install
-------

.. code-block:: bash

   pip install graphed-executors                  # laptop: thread and process pools
   pip install "graphed-executors[dask]"          # + a dask.distributed cluster
   pip install "graphed-executors[parsl]"         # + a parsl pool

``graphed`` comes along as a dependency. It ships wheels, so a plain install needs nothing extra;
installing it from source builds its Rust core, which needs a Rust toolchain.

Your first run
--------------

Count entries across seven partitions on four worker processes. Save this to a file and run
it — the ``__main__`` guard is required, because a spawned worker re-imports the file it was
launched from.

.. code-block:: python

   import numpy as np
   from graphed.core import Partition, Plan, Task
   from graphed_executors.local import ProcessPoolExecutor

   def count(partition, resources):        # module-level, so a worker can import it
       return np.asarray([partition.entry_stop - partition.entry_start])

   def add(a, b):
       return a + b

   def zero():
       return np.zeros(1, dtype=int)

   if __name__ == "__main__":              # spawned workers re-import this file
       tasks = tuple(
           Task(i, Partition("data", "Events", i * 100, (i + 1) * 100)) for i in range(7)
       )
       plan = Plan(process=count, combine=add, empty=zero, tasks=tasks)

       result = ProcessPoolExecutor(max_workers=4).run(plan)
       print(result.value, result.n_partitions, result.n_combines)

   # [700] 7 6

``result.value`` is your answer; ``n_partitions`` and ``n_combines`` tell you how much work
ran. Seven chunks take six merges — and they take those same six merges no matter how many
workers you used, which is the point of the next section.

What you get for free
---------------------

* **The same answer on 1 worker and on 100.** Which partial results get added to which is
  decided before anything runs, so a floating-point total comes out byte-identical run to run.
* **One slow file can't stall the run.** A straggling partition holds up only the merges on
  its own path; every other partial keeps reducing meanwhile.
* **A crash on a worker lands on your line.** The exception comes back as the exception it
  was, carrying the failing operation, the partition it was on, and your source frames — not
  an opaque string from another process.
* **On the laptop pools and on dask, workers merge with each other, not through you.** Partial
  results are combined among the workers by default, so your submit node is not a funnel for the
  whole dataset. A parsl pool is the exception: HTEX resolves a task's arguments on the submit
  host, so every partial passes through it on the way to its merge. Keep partials small there,
  or use dask when they are not — :doc:`parsl` has the details.

:doc:`design` explains why each of these holds.

Which runner do I want?
-----------------------

.. list-table::
   :header-rows: 1
   :widths: 24 32 44

   * - You have
     - Use
     - Notes
   * - A laptop or one big node
     - ``ProcessPoolExecutor`` from ``graphed_executors.local``
     - The default. Real parallelism; your ``process``, ``combine`` and ``empty`` must be
       importable in a worker process.
   * - A very large core count, or a low open-file limit
     - ``PinnedPoolExecutor`` from ``graphed_executors.local``
     - Identical results, but each worker talks to a bounded set of peers instead of all of
       them. ``ProcessPoolExecutor`` warns and points here when its worker count would strain
       the file-descriptor budget.
   * - No extra dependencies, or a notebook
     - ``ThreadExecutor`` from ``graphed_executors.local``, or
       ``SubmitRunner(ThreadBackend())``
     - Threads share your address space, so nothing has to be picklable.
   * - A ``dask.distributed`` cluster
     - ``dask_runner(client)`` — see :doc:`dask`
     - Also gets you distributed repartition and join.
   * - A parsl pool (HTEX on SLURM, HTCondor, LSF, …)
     - ``parsl_runner(executor)`` — see :doc:`parsl`
     - Your task functions must live in an installed module, not in ``__main__``.

Going to a cluster is one substitution. Everything above the ``plan = ...`` line stays as it
is; only the runner changes.

.. code-block:: python

   # A recipe: this needs a running cluster, so it is not runnable as written.
   from dask.distributed import Client
   from graphed_executors.dask_backend import dask_runner

   with Client("tcp://scheduler.example:8786") as client:
       result = dask_runner(client).run(plan)          # same plan, same answer

Running the same plan through a runner with no cluster and no extra dependencies takes the
same shape, and this one you *can* run as written:

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

   tasks = tuple(
       Task(i, Partition("data", "Events", i * 100, (i + 1) * 100)) for i in range(7)
   )
   plan = Plan(process=count, combine=add, empty=zero, tasks=tasks)

   with SubmitRunner(ThreadBackend(max_workers=4)) as runner:
       print(runner.run(plan).value)

   # [700]

Reshaping data between steps
----------------------------

Some analyses need the data laid out differently partway through: grouped by a key before a
per-group operation, or joined against another dataset. That is a separate entry point rather
than something ``run()`` does for you.

On one machine, ``run_repartition`` and ``run_repartition_by_size`` in
``graphed_executors.local`` compute every kernel in your driver process, so they are the
correctness and development path rather than a way to use a whole node. For the real thing, go
to ``run_repartition`` and ``run_join`` in ``graphed_executors.dask_backend`` — call
``dask_transport_setup(client)`` once per client first, which is idempotent and is what the
default route needs.

Each returns a counters object next to the result, so you can see how much data moved and which
route it took. :doc:`dask` covers choosing a route, and the budgets that stop a large exchange
from opening a million files at once.

Where to go next
----------------

.. toctree::
   :maxdepth: 2
   :caption: Contents

   design
   dask
   parsl
   api
   improvements
   changelog

* :doc:`design` — why your result is reproducible, where your merges run, and what happens
  when a worker dies.
* :doc:`dask` and :doc:`parsl` — install, a worked run, the knobs, and the failures you will
  actually hit.
* :doc:`api` — the reference, grouped by what you are doing.
* :doc:`changelog` — what each release gives you that the last one did not.

.. note::

   ``import graphed_exec_local`` still works, as a deprecated alias for
   ``graphed_executors.local`` — so an old ``graphed_exec_local.ThreadExecutor`` keeps running.
   It covers the laptop pools only; anything else moved to its own subpackage, so
   ``graphed_exec_local.dask_backend`` raises ``ModuleNotFoundError`` and the import you want is
   ``graphed_executors.dask_backend``.

Indices
-------

* :ref:`genindex`
* :ref:`modindex`
