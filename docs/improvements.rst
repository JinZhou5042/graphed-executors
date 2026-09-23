Current limitations
===================

What does not work yet, and what to do instead.

- **No TaskVine or WorkQueue support, and no direct HTCondor/Slurm submission.**
  ``ParslBackend`` accepts parsl's ``HighThroughputExecutor`` and ``ThreadPoolExecutor`` and
  refuses any other executor type with a ``TypeError``. To run on a batch system, keep the
  graphed side unchanged and let the pool layer do the submission: parsl's providers
  (``SlurmProvider``, ``CondorProvider``) under an HTEX pool, or
  `dask-jobqueue <https://jobqueue.dask.org/>`__ in front of the dask backend. :doc:`dask`
  carries worked recipes for the dask side; on parsl the provider goes in your own parsl
  config and you hand the started executor to ``parsl_runner`` exactly as in :doc:`parsl`.

- **Stopping on statistical convergence is not implemented.**
  ``graphed.core.execution.StopCondition`` ends a run on an event target
  (``target_events``), a wall-clock budget (``max_wall_s``), or an error budget
  (``max_errors``) — not on the precision of the result. Set an event target and check the
  uncertainty yourself between runs.

- **No checkpoint/resume on a cluster.** ``graphed.checkpoint.run_resumable`` and
  ``run_shuffle_resumable`` drive a content-addressed store on the local filesystem; the dask
  and parsl backends run a plan start to finish. If a long cluster run dies, it starts over —
  when that cost matters, split the work into smaller plans and combine their results.

- **No TLS on graphed's own exchange plane on parsl.** ``shuffle_method="transport"`` on a
  parsl pool moves blocks over HTTP endpoints graphed mints in-task, and parsl's
  ``encrypted=True`` protects parsl's own channels, not those. Use it on a trusted cluster
  network, or stay on the default route, which relays through your submit node over parsl's
  own channels. This does not arise on dask, where blocks ride ``distributed``'s worker
  connections and inherit whatever the cluster's comms are configured with.

- **Dashboard events over parsl arrive at task completion, not live.** parsl has no
  worker-to-driver event stream, so a task's ``started``/``finished`` events are buffered on
  the worker and delivered together when its result comes back. Every event still arrives and
  the result is unaffected — you just can't watch a task while it is in flight. Relatedly, on
  either backend the worker-to-worker exchange engine reports the combine count from the
  driver rather than emitting one event per combine.

- **No free-threaded CPython (3.14t) on the dask path.** ``distributed`` declares support up to
  3.14 and nothing about free-threading. The local executors run on 3.14t; on a dask cluster, use
  standard CPython.

- **Run control stops at the local executors.** ``control=`` (pause, resume, cancel) is honoured
  by ``ThreadExecutor``, ``ProcessPoolExecutor`` and ``PinnedPoolExecutor``. The dask and parsl
  plan routes that merge worker to worker (``transport_run_plan``, ``parsl_run_plan``) and the
  exchange engines take no control and emit no task events, so the dashboard cannot see or steer
  them; stop such a run by interrupting it.
