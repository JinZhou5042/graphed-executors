API reference
=============

The plan you hand to a runner is built out of ``Plan``, ``Task`` and ``Partition``, and the
``ExecResult`` you get back — all four live in ``graphed.core``, not in this package. What
follows is everything on the *running* side, grouped by what you are trying to do. Follow a
module link for its classes and functions.

Run a plan
----------

``graphed_executors.local`` is the single-machine side: ``ThreadExecutor``,
``ProcessPoolExecutor``, ``PinnedPoolExecutor``, and the tree-reduction helpers
(``plan_tree``, ``tree_reduce``, ``running_fold``) underneath them. The two cluster modules
give you a one-line runner each — ``dask_runner(client)`` and ``parsl_runner(executor)`` —
and are importable only with their extra installed (``[dask]``, ``[parsl]``). Importing
either module does *not* import dask or parsl; that happens when you construct a runner. Every executor
and runner has ``run(plan)`` and ``submit(plan)``, which returns a future so you can record the
next plan while this one runs.

.. autosummary::
   :toctree: generated
   :recursive:

   graphed_executors.local
   graphed_executors.dask_backend
   graphed_executors.parsl_backend

Reshape data between steps
--------------------------

Repartitioning and joining are their own entry points, not something ``run()`` does for you.
On one machine: ``graphed_executors.local.run_repartition`` and ``run_repartition_by_size``,
which compute every kernel in your driver process — the correctness and development path, not a
way to use a whole node. On a cluster: ``graphed_executors.dask_backend.run_repartition`` and
``run_join``, after one idempotent
``graphed_executors.dask_backend.transport.dask_transport_setup(client)`` per client. Both take
a ``shuffle_method="auto" | "transport" | "tasks"`` argument to pick how blocks move —
directly between workers, or through the scheduler as ordinary tasks. Each returns the
gathered result together with a counters object recording what actually moved.
``graphed_executors.common`` holds the exchange engines the dask and parsl backends share.

.. autosummary::
   :toctree: generated
   :recursive:

   graphed_executors.common

Write a new backend
-------------------

Only needed if you are adding support for a scheduler that is not here yet.
``graphed_executors.submit`` is the interface: implement the ``SubmitBackend`` protocol,
declare what your scheduler can do with ``SubmitCapabilities``, and ``SubmitRunner`` turns it
into something that runs a plan. ``ThreadBackend`` is a complete, working implementation over
a stdlib thread pool — the shortest one to read, and useful on its own for running a plan
with no extra dependencies.

.. autosummary::
   :toctree: generated
   :recursive:

   graphed_executors.submit

.. note::

   ``graphed_exec_local`` is a deprecated alias for ``graphed_executors.local`` — the laptop
   pools only. Old imports of those keep working; everything else lives in its own subpackage
   (``graphed_executors.dask_backend``, ``.parsl_backend``, ``.submit``, ``.common``), so reach
   for those directly.
