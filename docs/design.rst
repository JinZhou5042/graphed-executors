How graphed-executors works
===========================

You ran your analysis on four cores this morning. Tonight the same plan runs on four hundred,
and next month a colleague reruns it on a machine you have never seen. Do the three runs agree
to the last bit? Where does the merging actually happen, and what does it cost? And when worker
47 dies at 3 a.m., what comes back to you?

This page answers those three questions. It assumes you have already run something — start at
:doc:`index` if you have not.

Here is the whole first answer in one program. Nine partitions, one enormous partial result and
eight tiny ones, so that the order the partials are added in is visible in the total:

.. code-block:: python

   import numpy as np
   from graphed.core import Partition, Plan, Task
   from graphed_executors.local import (
       PinnedPoolExecutor,
       ProcessPoolExecutor,
       ThreadExecutor,
       plan_tree,
   )

   def process(partition, resources):
       i = partition.entry_start
       return np.array([1e16 if i == 0 else 1.0])

   def add(a, b):
       return a + b

   def zero():
       return np.zeros(1, dtype=float)

   if __name__ == "__main__":
       tasks = tuple(Task(i, Partition("data", "Events", i, i + 1)) for i in range(9))
       plan = Plan(process=process, combine=add, empty=zero, tasks=tasks)

       combines, root = plan_tree(len(tasks))
       print("combines (result, left, right):", combines)

       # What a completion-order merge would give you, for two arrival orders.
       partials = [process(t.partition, None) for t in tasks]
       for order, name in ((partials, "arrived in order"), (partials[::-1], "arrived reversed")):
           total = zero()
           for p in order:
               total = add(total, p)
           print(f"{name:<24} {total.tobytes().hex()}  {float(total[0]):.17g}")

       runs = {
           "ThreadExecutor(1)": ThreadExecutor(max_workers=1),
           "ThreadExecutor(8)": ThreadExecutor(max_workers=8),
           "ProcessPoolExecutor(1)": ProcessPoolExecutor(max_workers=1),
           "ProcessPoolExecutor(8)": ProcessPoolExecutor(max_workers=8),
           "PinnedPoolExecutor(8)": PinnedPoolExecutor(max_workers=8),
       }
       for label, executor in runs.items():
           value = executor.run(plan).value
           print(f"{label:<24} {value.tobytes().hex()}  {float(value[0]):.17g}")

.. code-block:: text

   combines (result, left, right): [(9, 0, 1), (10, 2, 3), (11, 4, 5), (12, 6, 7), (13, 9, 10), (14, 11, 12), (15, 13, 14), (16, 15, 8)]
   arrived in order         0080e03779c34143  10000000000000000
   arrived reversed         0480e03779c34143  10000000000000008
   ThreadExecutor(1)        0480e03779c34143  10000000000000008
   ThreadExecutor(8)        0480e03779c34143  10000000000000008
   ProcessPoolExecutor(1)   0480e03779c34143  10000000000000008
   ProcessPoolExecutor(8)   0480e03779c34143  10000000000000008
   PinnedPoolExecutor(8)    0480e03779c34143  10000000000000008

Two arrival orders, two different answers. Five executors, one answer. The rest of this page is
why.


Why your result is the same on 1 worker and on 100
--------------------------------------------------

Floating-point addition is not associative. ``(a + b) + c`` and ``a + (b + c)`` are different
numbers when the magnitudes are far apart, which in a real analysis they are — one busy file
contributes a bin count of ten million while a sparse one contributes three. So the moment your
partial results are merged in whatever order the workers happen to finish, your totals wobble
between runs, and a "reproducible" analysis is reproducible only to within a few ulps you cannot
predict.

The usual escape is to wait for every partition, then merge in index order. That is
deterministic and it reintroduces a barrier: one slow file stalls the entire run.

``plan_tree`` avoids the choice. Before any work is submitted, it lays out a binary merge tree
over **leaf indices** — leaf 0 with leaf 1, leaf 2 with leaf 3, and so on up to a single root.
That is the ``combines`` list printed above: ``(9, 0, 1)`` means "node 9 is leaves 0 and 1
merged". Nothing in it mentions a worker, a machine, or a clock. ``tree_reduce`` then accepts
leaf results **in whatever order they arrive** and fires each merge the instant both of its
inputs exist.

Both properties fall out of the same structure:

* **The answer does not depend on the cluster.** The grouping is a pure function of the number
  of partitions, so one thread and eight processes reduce along identical paths and produce
  identical bytes. Integer counts are exact under any grouping; float storages come out
  byte-identical because the grouping is fixed, not because the arithmetic is forgiving.
* **One slow file cannot stall the run.** A straggling partition blocks only the ``log n``
  merges on its own path to the root. Every other subtree keeps reducing while it sleeps. There
  is no barrier anywhere in the reduction.

The edge of this guarantee: the tree is a function of the partition *set*. Change how a
dataset is split — different chunk sizes, a different file list, a repartition step in the
middle — and you have changed which partials meet which, so a float total may move in its last
bits. Reprocessing the same dataset the same way is bit-for-bit; reprocessing it differently is
not, and no scheduler can make it so. For the same reason, a left-to-right fold over the same
partials (a merge in arrival order, or a single-threaded accumulate) is a *different* grouping
and lands on a different bit pattern, as the first two output lines show.


What the pool asks of your four functions
-----------------------------------------

A ``Plan`` is ``process``, ``combine``, ``empty`` and ``tasks``, and a runner uses all four
without ever looking inside them. Two consequences are worth knowing before your first crash.

**Your functions have to survive a trip to another process.** The process pools use the
**spawn** start method rather than ``fork``: a forked CPython process inherits lock and
allocator state that bites precisely when you scale up, and spawn is the one behaviour every
platform shares. A spawned worker re-imports the module it was launched from and unpickles your
callables, so ``process``/``combine``/``empty`` must be module-level functions, a
``functools.partial`` of one, or a frozen dataclass — and a script must guard its entry point
with ``if __name__ == "__main__":``. Thread pools share your address space and impose neither
rule, which makes ``ThreadExecutor`` the fast way to debug a plan before you scale it out.

**Files open once per worker, not once per partition.** ``resources.open_once(uri, opener)``
inside ``process`` hands back a handle the worker keeps for its lifetime: thread-local for the
thread pool, a per-process global installed by the pool initializer for the process pools. Ten
partitions of the same file on one worker open it once.

An optional fifth element, ``next_tasks``, changes the shape of the run — see
`Growing the work while the run is going`_.


Why the tasks are few and large
-------------------------------

The failure this package was built against is a scheduler that spends more time deciding what to
run than running it. Every layer here is arranged so the interpreter and the scheduler see as
few units of work as possible.

Partitions, not rows, are the unit: one task is an entry range of a file, doing a whole fused
run of array operations. Reductions are scheduled as ``n - 1`` merges but only ``O(log n)`` of
them are ever live at once, so a hundred thousand partitions do not become a hundred thousand
resident intermediates. And a data exchange creates one producer task per *worker*, not one per
source block — you will see that in the ``producer tasks`` count below, where six source blocks
become one task on one worker and three on three.


Where your combines run, and what that costs
--------------------------------------------

The obvious place to merge partial results is the driver: every worker ships its partial back to
your submit node, which adds them up. That is fine while partials are small and terrible when
they are not — a thousand workers each returning a large histogram makes your laptop the funnel
for the entire run, and the data crosses the network twice.

So on the laptop pools and on dask, the merges run by default **across the workers, off the
driver** (parsl is the exception, for a reason covered under `On a parsl pool`_). Each worker owns a
contiguous range of leaves and reduces it locally; a partial that straddles a range boundary is
handed worker-to-worker by ownership, where ownership of an interior node belongs to the worker
holding its leftmost leaf. Because every node keeps its *global* position in the tree, moving
the merges around never changes the grouping — the answer is bit-for-bit what a driver-side
merge would have produced, floats included. The driver waits only for the root.

Two knobs shape this:

* ``comms="ipc"`` (the default) runs the peer merges over per-worker inbox queues on one
  machine; ``comms="http"`` runs them over loopback sockets, the same shape a distributed
  scheduler uses. ``comms=None`` puts the merges back on the driver.
* ``pooled_combines=True`` keeps the driver in charge but pushes the merge calls onto the worker
  pool. It is for heavy partials on the driver-side path, and it is refused outright — loudly,
  not silently — when peer merging is on, because the two mean different things about who owns a
  node.

Picking a process pool
~~~~~~~~~~~~~~~~~~~~~~

Workers that talk to each other need addresses for each other, and an address here is an
operating-system file descriptor. Two pools trade that cost differently, and you choose by
naming the class — there is no silent switch at runtime.

``ProcessPoolExecutor`` gives every worker the whole address book. It is simple and it is the
fastest path up to roughly the per-process descriptor limit; that limit is what bounds it,
because ``N`` workers each holding ``N`` inboxes is ``N²`` descriptors. The axis is your
``ulimit -n``, not your core count: each worker holds about two descriptors per peer, and the
warning fires once that would exceed about half the limit — around 224 workers where the limit
is 1024, but already at 32 where it is 256.

``PinnedPoolExecutor`` gives each worker only the addresses it will actually use: its own inbox,
its merge targets, and a small set of peers it may steal work from. Those peers are laid out as
a hypercube — a wiring in which every worker reaches every other in ``log N`` hops while holding
only ``log N`` addresses — so the whole address book is ``N log N`` instead of ``N²``. Same
results, bit for bit; smaller footprint.

``ProcessPoolExecutor`` warns and names ``PinnedPoolExecutor`` when its worker count would
strain the descriptor budget. It warns rather than switching, so the pool you get is always the
one written at the call site. (``ProcessExecutor`` is a deprecated alias for
``ProcessPoolExecutor``; use the explicit name.)

Paying the spawn cost once
~~~~~~~~~~~~~~~~~~~~~~~~~~

Spawn means an import-heavy worker startup, and by default each ``run()`` builds a fresh pool.
That is the right default for isolation and wrong for a notebook running eight small plans or a
sweep running hundreds — pay a full pool spawn per plan and parallel can come out slower than
sequential. ``persistent=True`` keeps one pool across ``run()`` calls, released by ``close()``
or by leaving the ``with`` block:

.. code-block:: python

   import numpy as np
   from graphed.core import Partition, Plan, Task
   from graphed_executors.local import ProcessPoolExecutor

   def count(partition, resources):
       return np.asarray([partition.entry_stop - partition.entry_start])

   def add(a, b):
       return a + b

   def zero():
       return np.zeros(1, dtype=int)

   def sweep(plans):
       with ProcessPoolExecutor(max_workers=4, persistent=True) as ex:
           return [ex.run(plan).value for plan in plans]   # one spawn, amortized

   if __name__ == "__main__":
       plans = [
           Plan(process=count, combine=add, empty=zero,
                tasks=tuple(Task(i, Partition("data", "Events", i * n, (i + 1) * n))
                            for i in range(4)))
           for n in (100, 200, 300)
       ]
       print(sweep(plans))

   # [array([400]), array([800]), array([1200])]

An executor runs one plan at a time: a second ``run()`` from another thread waits until the first
returns, because the kept pool and the peer state belong to the executor, not to the run.

Recording the next plan while this one runs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Recording and compiling a plan is serial driver work, and during ``run()`` the driver only waits.
``submit(plan)`` hands the plan to a driver thread and returns a ``concurrent.futures.Future``
at once, so the next plan is recorded while this one executes. ``future.result()`` is exactly
``run(plan)``: the same value, the same exception (a worker's ``StageError`` intact), the same
retries and the same monitor events. Plans still execute one at a time, in submit order.

``max_in_flight`` (default 2) bounds the submitted plans not yet finished, the running one
included, and ``submit`` blocks at the bound; 2 lets the driver keep one recorded plan in hand.
Cancelling a queued future frees its slot. ``close()``, or leaving the ``with`` block, waits for
every submitted plan before it releases the pool; a local executor accepts ``submit`` again after
it. Every executor and runner here has it — the local pools and ``SubmitRunner`` on any backend:

.. code-block:: python

   import numpy as np
   from graphed.core import Partition, Plan, Task
   from graphed_executors.local import ProcessPoolExecutor

   def count(partition, resources):
       return np.asarray([partition.entry_stop - partition.entry_start])

   def add(a, b):
       return a + b

   def zero():
       return np.zeros(1, dtype=int)

   def build(n):  # stands in for recording and compiling an analysis
       return Plan(process=count, combine=add, empty=zero,
                   tasks=tuple(Task(i, Partition("data", "Events", i * n, (i + 1) * n))
                               for i in range(4)))

   if __name__ == "__main__":
       with ProcessPoolExecutor(max_workers=4, persistent=True) as ex:
           futures = [ex.submit(build(n)) for n in (100, 200, 300)]
           print([f.result().value for f in futures])

   # [array([400]), array([800]), array([1200])]

Three traps. Calling ``submit`` or ``.result()`` from a monitor callback or from inside a plan's
``process`` on the same executor waits on its own driver thread and never returns. And
``ThreadExecutor``/``ThreadBackend`` overlap recording with running only where the kernels
release the GIL; a process pool or a cluster overlaps regardless. A process pool fails plans still
unwaited at interpreter exit, so take every ``.result()``, call ``close()`` or leave the ``with``
block first.

When an idle worker takes work from a busy one
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Partitions are never uniform: one file is a skim, the next is raw. An idle worker therefore
takes **one** leaf from the far end of a busy peer's range, rather than half of it — taking half
drains a victim geometrically once several thieves are idle and piles the work back up
somewhere else. Stealing one at a time from a randomly chosen victim is the scheme with provable
bounds on the time wasted stealing (Blumofe–Leiserson, as in Cilk).

Stealing moves only the ``process`` work. The leaf's original owner still merges it — the thief
ships the partial back — so the tree, and the answer, are untouched. An idle delay plus
exponential backoff makes it cost nothing on balanced work while still rebalancing a genuine
straggler. Pass ``steal=False`` to turn it off.


Growing the work while the run is going
---------------------------------------

A plan with a ``next_tasks`` hook runs as a rolling fold instead of a fixed tree. The driver
folds results as they complete and periodically calls ``next_tasks`` with the elapsed time,
completed counts and errors so far; it answers with more partitions, or with a reason to stop.
That is the path for deciding chunk sizes from observed throughput, and it costs you the fixed
tree's bit-for-bit guarantee: the grouping now depends on what the run discovered. Use it when
the partition set genuinely is not known up front, and the fixed tree everywhere else.


What happens when a worker dies
-------------------------------

There are two ways a worker can fail, and they surface differently.

**Your code raised.** The exception propagates to the driver as the exception it was. In
particular ``graphed.debug.StageError`` — which is picklable, and reconstructs exactly — arrives
carrying the operation that failed, the partition it was on, and your source frames, so
``format_traceback`` still points at the line you wrote. Nothing is wrapped, nothing is
stringified. "The remote error was an opaque string" is the failure this stack exists to avoid.

**The process died.** A segfault, an OOM kill, or a preemption leaves no exception to propagate.
On a cluster this arrives as a scheduler-level event — dask raises ``KilledWorker`` once a task
has cost more than the allowed number of worker deaths — and the backend turns it into an
attributed ``StageError`` naming the partition and the last worker that held it, rather than
handing you the raw scheduler error. The attribution can be unfair when several tasks shared the
dead worker, and the message says so.

Retries are the underlying library's, not a second layer on top: ``dask_runner``'s ``retries``
forwards to dask's per-task retries, which resubmit on another worker. There is no graphed-level
retry loop to double up with it. The default is 3, so a deterministic exception in your
``process`` is executed four times before the driver sees it; ``retries=0`` while debugging.

.. _design-epoch-restart:

When the exchange itself fails, the whole run restarts
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A data exchange between workers is different: the blocks in flight are meaningless once one leg
of it is broken, and half-repaired state is how a shuffle quietly produces a wrong answer. So
the exchange engines do not patch — they restart the run, and they tag each attempt with a fresh
generation number (an *epoch*) that workers check on every incoming block. A block from the
abandoned attempt cannot be mistaken for a live one, because its epoch is no longer accepted.

Four things trigger a restart: a peer-to-peer send that exhausts its retries
(``TransportDeliveryError``), a block pull that times out against a slow or dead holder
(``PullTimeoutError``), a peer reduction that finishes without capturing a root — which is
raised rather than quietly returning ``empty()`` — and a worker death. The restart re-reads the
surviving worker set so it pins onto workers that are still there. After
``epoch_restarts_allowed`` attempts (default 1) you get an attributed ``StageError`` naming what
died. You never get a raw scheduler exception, and you never get a hang.

If you see ``PullTimeoutError`` on a legitimately huge batch rather than on a dead worker, raise
``pull_timeout_s`` before you spend the restart budget. If you see ``TransportDeliveryError``,
a worker is unreachable — check that the pool is on one routable network before retrying.


Watching a run
--------------

Every executor takes an optional ``monitor=``. It is a passive observer implementing
``graphed.core.execution.Monitor``; the executor knows nothing about rendering or transport and
only emits a small vocabulary of ``TaskEvent`` records. ``graphed.debug.Dashboard`` is one
consumer of them.
The monitor is also a public ``monitor`` attribute on every executor and runner, read when a plan
starts, so ``Dashboard.attach(executor)`` (which assigns it) reaches the next plan anywhere.

One task is three events. The driver emits ``SUBMITTED`` when it hands the task to the pool; the
worker emits ``STARTED`` before running it and exactly one of ``FINISHED`` or ``ERRORED`` after.
Where the worker's two events go depends on the pool: thread workers call the monitor directly,
while process workers append to a small in-process buffer that a per-worker daemon thread ships
to the driver in batches, so no task ever pays an inter-process round trip on its critical path.
A driver-side collector thread replays them into your monitor. A per-worker sampling profiler,
if you supply one through the monitor's ``worker_profiler_factory``, rides the same channel.

The property that makes this safe to leave on is that emission is **best-effort and drops when
full**. A slow monitor never becomes back-pressure that changes task timing — which would in
turn change what the adaptive path decides — and a monitor that raises is swallowed. A run's
result and its merge count are byte-identical whether a monitor is attached, absent, or actively
throwing.


Pausing and cancelling a run
----------------------------

A monitor watches; a ``graphed.core.RunControl`` steers. Every local executor and ``SubmitRunner``
takes ``control=``; the runner ``dask_runner`` and ``parsl_runner`` return has its public ``control``
attribute set instead, which is what ``Dashboard(control=True).attach(executor)`` does. Each follows the contract in graphed's
*Pausing and cancelling a run*: pause starts no new task, cancel starts none, waits for what
was submitted and returns the fold of the tasks that completed with
``stopped=StopReason.CANCELLED``, and a failure while it drains raises as it would without a
control. A cancel ends one plan, the one running when it fires (or the next to start); plans
queued behind it by ``submit`` run as usual.

.. code-block:: python

    import time

    from graphed.core import Partition, Plan, RunControl, Task
    from graphed_executors.local import ThreadExecutor

    ctl = RunControl()

    def process(part, resources):
        if part.entry_start == 3:
            ctl.cancel()                          # stands in for the dashboard's cancel button
        time.sleep(0.01)
        return 1

    tasks = [Task(k, Partition(f"f{k}.root", "Events", k, k + 1)) for k in range(100)]
    plan = Plan(process=process, combine=lambda a, b: a + b, empty=lambda: 0, tasks=tasks)
    res = ThreadExecutor(max_workers=2, control=ctl).run(plan)
    print(res.stopped, res.value == res.n_partitions < 100, ctl.state)

which prints::

    cancelled True running

Where the executor looks at the control depends on who merges:

* **Hub** (``comms=None``, with or without ``pooled_combines``) and **adaptive** plans: without a
  control every task goes to the pool at once. With one, the driver hands out at most
  ``max_workers`` tasks at a time and refills a slot as a task finishes, only while the control
  is ``RUNNING``, so a pause holds everything not yet handed out. A process pool may already
  have queued up to ``max_workers`` of them, and those still start.
* **Peer** (``comms="ipc"``/``"http"``, and ``PinnedPoolExecutor``): the driver passes each state
  change to every worker, and a worker acts on it between two of its own tasks. On a cancel each
  worker hands the driver its finished pieces of the merge tree, and the driver merges them.
  A peer run's root deadline counts only time spent running, so a long pause never times it out.
* **SubmitRunner** (thread, dask and parsl backends), fixed and adaptive plans: with a control, the driver
  hands out at most as many tasks as the backend has task slots (``task_slots()`` where the backend has it:
  the dask cluster's threads, the connected parsl HTEX workers or the parsl thread pool's size;
  else ``n_workers()``), and at least one, so a pool that starts with no workers runs one task
  until workers join. While it holds tasks the driver wakes every 50 ms and, when every slot is
  busy, reads the slot count again, so it widens as workers join and a resume starts tasks without
  waiting for one to finish. A merge is submitted only once both of its inputs have finished. On
  dask every task depends on the broadcast plan function, and dask runs a task where its input
  lives, so a controlled dask run should use ``dask_runner(client, replicate_broadcast=True)``;
  without it, tasks queue on the worker holding the broadcast while other slots stay idle. Like
  ``monitor``, ``control`` is read when a plan starts, so assigning it mid-run takes effect from
  the next plan.

On the fixed-tree and peer routes, a cancelled run's total is the fixed merge tree over the tasks
that finished: every merge whose two inputs completed runs, and the pieces left over are added in
leaf order; a run never cancelled gives exactly the total it gives without a control. On the
adaptive route, a cancelled run's total, like an uncancelled one's, adds the partials in the order
they finish, so it is reproducible only when the partials add exactly (integers, counts).


.. _design-shuffle-graph:

Moving data between steps, and what it costs
--------------------------------------------

Some analyses need the data laid out differently partway through: grouped by event or object key
before a per-group step, or joined against another dataset. That is an exchange, and an exchange
is where a naive implementation falls over.

The shape is two phases. Every producer task writes one block per destination partition; every
destination then collects the blocks addressed to it. With ``T`` producers and ``P``
destinations that is ``T × P`` transfers — and if each transfer is a file, a thousand producers
feeding a thousand destinations is a million files open at once. That is why the number of
producers is tied to the number of *workers* rather than to the number of source blocks, why a
producer coalesces several sources into one block per destination, and why the engines carry
explicit byte budgets rather than trusting RAM to hold.

Every exchange hands back a counters object next to the result (``witness``) saying what it
actually did — how many producer tasks ran, how much spilled, which route it took. Running one
on a single machine, six source blocks into four destinations:

.. code-block:: python

   import numpy as np
   from graphed.numpy import NumpyBackend
   from graphed_executors.local import run_repartition

   ROW = np.dtype([("__joinkey__", np.uint64), ("v", np.int64)])

   def block(keys):
       out = np.zeros(len(keys), dtype=ROW)
       out["__joinkey__"] = np.asarray(keys, dtype=np.uint64)
       out["v"] = np.arange(len(keys), dtype=np.int64)
       return out

   src_blocks = [block(range(i * 50, i * 50 + 50)) for i in range(6)]

   for workers in (1, 3):
       result = run_repartition(NumpyBackend(), src_blocks, parts=4, workers=workers)
       print(
           f"workers={workers}",
           "producer tasks:", result.witness.n_producer_tasks,
           "rows per dest:", {d: len(b) for d, b in sorted(result.value.items())},
       )
       print("           dest block hashes:",
             {d: h[:12] for d, h in sorted(result.dest_block_hashes.items())})

.. code-block:: text

   workers=1 producer tasks: 1 rows per dest: {0: 73, 1: 84, 2: 79, 3: 64}
              dest block hashes: {0: '92c450428ea0', 1: '290688d8c484', 2: '902bacbe965c', 3: '75de0dd7fb55'}
   workers=3 producer tasks: 3 rows per dest: {0: 73, 1: 84, 2: 79, 3: 64}
              dest block hashes: {0: '92c450428ea0', 1: '290688d8c484', 2: '902bacbe965c', 3: '75de0dd7fb55'}

Three things to read off that output. Six source blocks became one producer task on one worker
and three on three — producers scale with workers, not with inputs. Every destination's contents
are a sha256 of its serialized bytes, so equality of those hashes *is* the determinism check:
byte-identical bytes, not "close enough" rows. And the hashes are the same at one worker and at
three, because producers are given contiguous ascending source ranges and each destination
concatenates its fragments in ascending producer order. The merge order is decided by the plan,
never by who finished first.

That last property holds across engines as well as across worker counts. The single-machine
engine, the two dask engines and the parsl engines all produce the same destination hashes on
the same inputs, so the route you pick can change what a run costs but never what it computes.

Two notes on the local entry point. ``run_repartition`` here computes every kernel in the driver
process — the cluster it talks to is a stand-in that distributes block *storage*, so this is a
correctness and development path, not a way to use a whole node. ``run_repartition_by_size``
adds a target block size for when your key distribution is skewed. The relational join,
``run_join``, is implemented in the same module but is not currently re-exported from
``graphed_executors.local``; import it from ``graphed_executors.local.shuffle`` until it is.

.. _design-transport-engine:

Letting workers exchange blocks directly
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

On a cluster there are two ways to get a block from the worker that wrote it to the worker that
needs it.

The **scheduler-graph** route makes the exchange part of the task graph: a gather task depends on
its producers, and the cluster's own scheduler tracks who holds what and fetches it. Completeness
is not something the engine has to arrange — a gather cannot start before its inputs exist,
because that is what a dependency edge means. If a worker dies, the scheduler recomputes the lost
producer from its inputs and the result is unchanged. The cost is the ``T × P`` pick tier: one
small task per (producer, destination) pair, so the scheduler sees a lot of tasks.

The **worker-to-worker** route runs the exchange on the workers themselves. A plugin adds a pull
handler to each worker; workers dial each other directly over the cluster's own worker-to-worker
connections and never route bulk bytes through your driver. The scheduler sees ``T`` producers
plus ``P`` consumers plus a short control tail — ``O(T + P)`` tasks instead of ``T × P``, with no
per-row task creation. A destination coalesces its requests to one per holding worker, so a
thousand readers never hit one worker with a thousand separate requests at the same instant.

The blocks a producer is holding for other workers live in that worker's block store, capped by
``holder_budget_bytes``: a block that would push the producer over the cap spills to that
worker's local disk instead of growing its resident set until the nanny kills it. The bytes never
pass through the client either way.

.. _design-budget-scope:

What each budget actually limits
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Three things here are narrower, or slower, than their names suggest. Knowing which is which is
the difference between tuning a run and thinking you tuned it.

* **``holder_budget_bytes`` is a real cap on a real thing.** It bounds the blocks a producer is
  holding for others, and exceeding it really does spill to that worker's disk — the spill count
  and peak bytes come back in the counters.
* **``fetch_budget_bytes`` and ``disk_budget_bytes`` are accounting, on the worker-to-worker
  route.** Under that route a gather task pulls its fragments, holds one destination resident,
  concatenates and returns; there is no live read-side fetch buffer and nothing spills on the
  read side. The two budgets drive a driver-side model that reproduces the single-machine
  engine's read-plane numbers so the counters stay comparable between engines. They are not
  back-pressure here. On the single-machine engine they *are* live caps.
* **A failed send blocks the worker that issued it.** Delivery is retried inside the ``send``
  call itself — five attempts of five seconds each, plus backoff, so up to roughly half a minute
  — before ``TransportDeliveryError`` is raised. It has to be inside the call, because a
  reduction message dropped outside it would vanish with no trace. Budget for the latency; it is
  not a hang.

The bulk-transfer counters are likewise a model, not a per-request ledger: they group a
destination's fragments per node, while the real gather groups them per holding worker, so the
reported count can be lower than the number of requests actually issued. The ceiling — one
request per (destination, holder) pair — holds either way.

.. _design-transport-limitations:

What the direct exchange does not do yet
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

None of these can affect a result; each phase completes before the next begins.

* A gather that happens to need a fragment already on its own worker still asks for it over a
  loopback request instead of reading the local store. A small cost, never a wrong answer.
* The block store's lock is held across a spill write, so the serving handler waits on disk
  during a spill. Because producers all finish before any gather starts, this cannot delay a
  live pull today.
* Live run monitoring is not wired through the worker-to-worker route: it accepts ``monitor=``
  for signature parity but does not emit events over the transport. Use the scheduler-graph
  route if you want a live dashboard during a shuffle.

.. _design-facade:

Choosing a shuffle engine
~~~~~~~~~~~~~~~~~~~~~~~~~

``run_repartition`` and ``run_join`` in ``graphed_executors.dask_backend`` (and their parsl
counterparts) are the front door over both routes, with one knob. On dask, call
``dask_transport_setup(client)`` once per client before the first exchange — it is idempotent,
and it is what the worker-to-worker route needs, including when the default picks it for you.

* ``shuffle_method="tasks"`` — always the scheduler-graph route.
* ``shuffle_method="transport"`` — always the worker-to-worker route.
* ``shuffle_method="auto"`` (the default) — the worker-to-worker route if the backend can both
  pin a task to a named worker and move data between workers; otherwise the scheduler-graph
  route.

``"auto"`` reads the backend's declared capabilities and nothing else — no size heuristics, no
look at the live cluster. That is what makes the choice reproducible: every worker resolves it
identically from the plan, and two runs of the same plan take the same route.

It also means ``"auto"`` does not know whether your cluster is elastic. **On a cluster where
workers come and go, ask for ``"tasks"`` explicitly.** The worker-to-worker route pins tasks to
specific workers, so an owner that leaves forces a whole-run restart, while the scheduler-graph
route just recomputes the lost blocks. On a fixed pool, ``"auto"`` is the cheaper choice.

Knobs that mean something on both routes — ``salt``, and ``on`` / ``how`` / ``broadcast`` /
``mem_budget_bytes`` on joins — are forwarded either way. Set a worker-to-worker-only knob
(``n_tasks``, the byte budgets, ``pull_timeout_s``, ``epoch_restarts_allowed``) on a run that
resolves to the scheduler-graph route and you get a ``ValueError`` naming the knob, before
anything is submitted. The check runs after resolution, so ``"auto"`` landing on the
scheduler-graph route raises too — a knob you set is never quietly dropped.

Both routes return the same portable triple: the destination block hashes, the value, and a
counters object. The engine-specific extra doubles as proof of which route ran, which is the
first thing to look at when ``"auto"`` was slower than you expected.


.. _design-one-engine-many-clusters:

One engine, many clusters
-------------------------

The cluster libraries all offer roughly the same thing — submit a function, get a future — and
differ in what else they can do. Rather than write an executor per library, there is one engine
over a small protocol, ``graphed_executors.submit.SubmitBackend``: ``submit`` with an explicit
key, ``broadcast`` to place a payload once, a driver-side event subscription, ``cancel`` and
``close``.

What varies between libraries is declared, per backend instance, as seven flags: can data move
between workers, can a payload be scattered once, can a task be pinned to a named worker, are
there per-task retries, per-task resources, cancellation of a running task, a worker-side file
cache. The engine may use a capability only behind an explicit check, and **both execution paths
are correct with all seven false** — that is the floor a new backend has to clear, not a
target it has to reach.

One more backend ships alongside the cluster ones. ``ThreadBackend``, from
``graphed_executors.submit``, runs any plan through this engine with no extra dependencies at
all — the quickest way to exercise the cluster code path from a notebook or a test. Its flags
differ from dask's on five of seven, so running the same suite through both is what keeps the
engine correct across capability variation rather than merely correct on dask.

A flag says what the *library* supports, not what is wired today. ``DaskBackend`` reports
per-task resources and worker pinning — dask really has them — but a plan's per-task resource
hints are not forwarded to ``client.submit``, because dask treats a resource request as a hard
constraint and an unsatisfiable one would park the task forever. Shape the cluster instead:
make the workers uniform for the work you are sending.

.. _design-dask-backend:

On a dask cluster
~~~~~~~~~~~~~~~~~

``graphed_executors.dask_backend`` (the ``[dask]`` extra) runs the same ``Plan`` on a
``dask.distributed`` cluster used as a plain scheduler — no dask collections, no high-level
graph, no dask-awkward. It submits opaque callables with explicit keys and future-dependency
edges, and inherits determinism, straggler tolerance and intact errors rather than re-deriving
them. :doc:`dask` is the how-to; what follows is why the pieces are shaped the way they are.

**The reduction is the same tree.** Leaves are ``process`` tasks; the merges are submitted up
front with future dependencies in exactly the ``plan_tree`` shape. The driver waits on one root
future. dask resolves each merge's arguments on whichever worker runs it and fetches inputs
between workers, so the merges run off your submit node; a slow leaf blocks only its own path.
Intermediates are released as their parent consumes them, so only ``O(log N)`` of them are live
at once. The grouping is fixed by leaf index, never by submission or arrival order — which is the
difference from an arrival-batched reduction, where the grouping varies with the order futures
happen to be submitted.

**Keys are explicit and unique per run.** Every submit carries a namespaced key of the form
``graphed-<plan fingerprint>-<nonce>-leaf|combine-<i>``, submitted with ``pure=False``. The
prefix makes a collision with one of your own key strings impossible; ``pure=False`` stops dask
deduplicating two reads of *different* byte ranges that happen to hash alike; the per-run nonce
makes a second ``run()`` on the same client actually re-execute instead of handing back the
first run's cached futures.

**Your pickled functions ship once.** ``process`` and ``combine`` are placed as a single
identity future and referenced by every task, rather than re-serialized per task, and a
worker-side token cache deserializes them once per worker however many tasks it runs. This is
placement by submit rather than by ``scatter``, whose worker-discovery timeout misbehaves on a
cluster that has scaled to zero. ``replicate_broadcast=True`` spreads the payload's replicas if
you find leaves clustering onto the worker that first received it.

**Files open once per worker.** A worker plugin holds one resource set per dask worker, so
``open_once`` gives you the same file locality it does locally. It is registered idempotently, so
workers that join late — an adaptive cluster, a batch queue trickling jobs in — get it too.

**Monitoring rides dask's own event channel.** Workers log to a per-run namespaced topic, the
driver subscribes for the duration and releases it in a ``finally`` once trailing events drain.
As locally, emission is off the data path and errors are swallowed.

Everything that touches dask lives under ``dask_backend`` and is imported lazily, so importing
``graphed_executors`` on a machine without dask installed works fine.

.. _design-deployment:

Deployment recipes
^^^^^^^^^^^^^^^^^^

The only thing the backend wants is a **ready** ``distributed.Client``. Building the cluster is
yours; hand over the client and the rest is unchanged. The batch recipes below are
site-dependent — the syntax is right, the values are yours:

* **SLURM, via dask-jobqueue.** ``SLURMCluster(cores=…, memory=…, walltime=…, interface="ib0",
  worker_extra_args=["--lifetime", "55m", "--lifetime-stagger", "4m"])``, then
  ``cluster.scale(jobs=N)``. Jobs are not workers — ``scale`` converts by ``worker_processes``.
  Point ``local_directory`` at node-local scratch, never a network mount. Size ``scale`` to the
  plan and use ``adapt()`` only to absorb the tail.
* **LPC HTCondor, via lpcjobqueue.** ``Client(LPCCondorCluster(ship_env=…, image=…))``, minding
  the CVMFS singularity image, the shipped environment, the worker port band, and the
  graceful-then-``condor_rm`` teardown. This is a pattern, not a dependency — nothing here
  imports lpcjobqueue.
* **Preemptible queues.** Set ``--lifetime`` strictly below the queue walltime so workers drain
  themselves and migrate their in-memory keys to peers instead of being killed outright, add a
  ``--lifetime-stagger``, and raise ``distributed.scheduler.allowed-failures`` to 5–10 so
  innocent tasks on evicted workers are not blamed. With draining, in-flight leaves reroute and
  the tree is unaffected, because the grouping is by index and not by worker.
* **Preemption during a long exchange.** A producer recomputes from scratch if its worker dies,
  so set ``--lifetime`` comfortably above a single producer's runtime. Otherwise a worker
  evicted mid-exchange forces its producer — and every gather depending on it — to recompute.
  This costs time, never correctness.
* **Match versions.** Keep ``dask``, ``distributed`` and ``graphed`` the same on client and
  workers; ``client.get_versions(check=True)`` tells you.
* Use ``processes=True`` with one thread per worker for GIL-holding compiled stages, and
  ``dashboard_address=":0"`` on a local cluster to avoid a port clash.

Free-threaded CPython (3.14t) is not a supported target for the dask backend: ``distributed``
declares support up to 3.14 and nothing about free-threading. The local executors keep theirs.

.. _design-parsl-backend:

On a parsl pool
~~~~~~~~~~~~~~~

``graphed_executors.parsl_backend`` (the ``[parsl]`` extra) runs the same plans and the same
exchange contracts over a parsl executor, through the same protocol. :doc:`parsl` is the how-to;
the design points that matter are these.

**It submits to the executor directly.** ``ParslBackend`` takes a *started*
``HighThroughputExecutor`` (or ``ThreadPoolExecutor``) and calls its ``submit`` — it does not go
through parsl's DataFlowKernel. The DataFlowKernel resolves future arguments on the submit host
anyway, so routing through it would add a process-global singleton and config-wide retries and
caching, which fight the reproducibility guarantee, in exchange for nothing.

**Your merges run on your submit node, not between workers.** That same argument-resolution rule
is why: a merge task is submitted with two partials as arguments, and HTEX pulls both to the
submit host before the task is dispatched. The grouping and therefore the answer are unchanged —
this is the fixed tree either way — but every leaf partial and every intermediate crosses your
driver, so a parsl pool wants small partial results. Histograms are; per-event arrays are not.

**A parsl pool is the all-false capability floor.** HTEX resolves future arguments on the submit
host, so it declares no worker-to-worker data movement, no pinning, no per-task retries or
resources. That is the case the engine is built to be correct in, and it is why the default
exchange route on parsl relays through your submit node: producers write, the driver takes
delivery once at a barrier, regroups each destination locally, and hands ``P`` gathers their
fragments as concrete arguments. The scheduler sees ``T + P`` tasks and no pick tier. The cost is
real and is reported rather than hidden — the counters carry the fact that the driver relayed and
how many bytes went through it.

**There is a direct worker-to-worker route, and it is opt-in.** parsl exposes no in-memory
transfer between workers, so graphed builds its own overlay when you ask for
``shuffle_method="transport"``: one persistent task per worker slot, each minting an HTTP
endpoint in-task, announcing itself to a rendezvous endpoint on the driver, and waiting on a
barrier until every peer has announced before the driver broadcasts the assembled address book.
No peer can send to an inbox that does not exist yet, because nobody has an address until all of
them do. Blocks then move peer to peer, coalesced to one request per holder and evicted after
serving; the driver's endpoint carries control traffic only.

**Reachability is checked, not assumed.** Whether two parsl workers can dial each other is a
property of your site — NAT, firewalls, multi-node overlays — not of parsl. So a probe runs at
rendezvous time, before any data moves. ``on_unreachable="error"`` (the default) raises an
attributed ``StageError`` naming the unreachable pair; ``on_unreachable="fallback"`` re-runs the
relay route on the same inputs and records why in the counters, so a fallback is never silent.
``probe_peer_reachability`` runs the same check as a pre-flight. Recovery from a failure mid-run
is the whole-run epoch restart described above.

Because HTEX declares no worker-to-worker data movement, ``"auto"`` never selects the direct
route on parsl — you reach it by asking for it. Declaring the capability just to force the
choice would also tell the scheduler-graph engine that peer movement exists, and it would then
route every block through your submit node believing otherwise.


Not supported yet
-----------------

* **Checkpoint and resume on a cluster.** ``graphed.checkpoint.run_resumable`` and
  ``run_shuffle_resumable`` are self-driving loops over a content-addressed store on the local
  filesystem; they are not runners, so there is no ``run_resumable(executor=dask_runner(...))``.
  Resumable execution on a cluster needs a distributed store first. Checkpoint locally, or
  partition your run into pieces you can resubmit.
* **TaskVine and Work Queue.** ``ParslBackend`` refuses executor types it has not verified
  rather than guessing a capability vector, so those raise a ``TypeError`` naming the two
  supported classes. Use HTEX.
* **Direct HTCondor and SLURM submission.** There is no batch-system executor here; go through
  dask-jobqueue, as in the recipes above, or a provider in your own parsl config.
* **TLS on graphed's own HTTP exchange plane.** parsl's ``encrypted=True`` covers parsl's
  channels, not this one. Keep an exchange inside a trusted network.
* **Live monitoring during a parsl run.** Worker events are buffered and delivered when a task
  completes, so a dashboard over parsl updates per task rather than continuously.
* **Free-threaded CPython on the dask path**, as above.
* **Convergence-based stopping.** ``next_tasks`` can stop a run on elapsed time, task counts or
  errors; stopping when a measurement reaches a target precision is not implemented.
* **Per-task resource hints and NUMA-aware placement.** Shape the cluster instead.

:doc:`improvements` tracks these.
