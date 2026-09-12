What changed
============

0.0.2
-----

Run the same plan on a cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* **Your plan runs on a** ``dask.distributed`` **cluster.** Hand ``dask_runner(client)`` any
  connected ``Client`` — a ``LocalCluster``, dask-jobqueue on SLURM, an HTCondor pool — and you
  get the answer your laptop gave you, bit for bit, because the merge tree is laid out before
  anything is submitted. Files still open once per worker, an exception still comes back pointing
  at the line in your analysis, and ``monitor=`` still gets one event per task.
  ``pip install "graphed-executors[dask]"`` (#2).
* **And on a** `parsl <https://parsl-project.org>`_ **pool.** If your site hands you compute
  through parsl's SLURM, HTCondor or LSF providers, start the executor and pass it to
  ``parsl_runner(executor)``; nothing above the runner line changes.
  ``pip install "graphed-executors[parsl]"`` (#3).
* **Run the cluster code path with nothing installed.** ``SubmitRunner(ThreadBackend())`` takes
  the same plan through the same engine over a standard-library thread pool — the quickest way to
  tell a cluster problem from an analysis problem (#2).
* **Adding a runner for another scheduler is now a small protocol**, not a new executor:
  implement ``SubmitBackend``, declare what your scheduler can do, and the engine does the rest.
  Both execution paths are correct with every declared capability absent, so a bare scheduler is
  enough to start (#2, #3).

Reshape data across a cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* **Repartitioning and relational joins run on a dask cluster** — ``run_repartition`` and
  ``run_join`` in ``graphed_executors.dask_backend``. One word, ``shuffle_method``, decides
  whether blocks travel worker-to-worker or through the scheduler as ordinary tasks. Both produce
  byte-identical output blocks, so the route changes what a run costs and never what it computes,
  and every exchange hands back counters saying what actually moved (#2).
* **The same two entry points work on a parsl pool.** The default route relays blocks through
  your submit node and reports how many bytes it held, so that cost is visible rather than
  guessed at. Ask for ``shuffle_method="transport"`` and workers exchange blocks directly, after
  a reachability probe that tells you whether your site allows it before any data moves (#3).
* **Joins on one machine too** — ``run_join`` in ``graphed_executors.local.shuffle``, with a
  memory budget that spills duplicated output partitions instead of accumulating them, and a
  broadcast-versus-shuffle choice taken from the plan, so a 4-worker run and a 400-worker run
  decide it the same way.
* **Broadcast joins survive a worker leaving.** On an elastic or preemption-prone cluster,
  ``dask_runner(client, replicate_broadcast=True)`` keeps the small side of a broadcast join
  alive when the worker holding it goes away (#2, written up in #4).

Fixed
~~~~~

* **A run whose partial results exceed 64 KB no longer hangs.** Workers merging with each other
  wrote partials down a pipe synchronously, so a large partial — a 100×100 double-storage
  histogram is around 80 KB — parked the sender until its receiver next polled, and work stealing
  could close that wait into a cycle. Sends now go through a queue that never blocks the actor, so
  every inbox keeps draining (#9).

Documentation
~~~~~~~~~~~~~

* The README and every documentation page were rewritten for someone who wants to run an
  analysis: a worked example first, section titles that answer a question, and engines named by
  what they do rather than by internal tags (#7). Read the Docs builds the reference again (#1),
  and the on-page table of contents is no longer duplicated (#5).

Housekeeping: continuous-integration pins, type checking widened over the test tree, and
test-only corrections (#6, #8, #10, #11, #12, #13, #14).

0.0.1
-----

First release. Thread and process pool runners for a ``graphed`` plan, with the merge tree laid
out before the run starts — so your answer does not depend on the worker count or on who finished
first — straggler-tolerant reduction, work stealing between workers, per-worker file handles
through ``open_once``, worker-to-worker merging that keeps partial results off your driver, and
failures that arrive as the exception they were, pointing at the line in your analysis.
