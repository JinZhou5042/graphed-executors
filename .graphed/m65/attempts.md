# m65 — implementer iterations (graphed-executors)

Debug lane (plans in `graphed-workdir/lanes/debug/`). One section per sub-plan.

## A2 (plan-A2.md, frozen `freeze-m65a2` = `27ce729`)

Run: `python -m pytest tests/frozen/m65 tests/extra/m65 -p no:cacheprovider`.

### Iteration 1 — A2.1 hub and adaptive routes (hub/pooled/adaptive 53/53; peer routes still fail)

`control=` on both constructors, public `control`. `run()` holds the entry check (before every route's
`n == 0` return) and the CANCELLED reset in `finally`. One `_Window` is the dispatch point of the three
hub routes: `take()` reads the state once, submits only while RUNNING, drops held work on CANCELLED;
`wait()` blocks in `control.wait()` when nothing runs, and waits 50 ms when `take()` saw PAUSED, so a
resume refills free slots while a task still runs. Fixed hub: a separate `_run_fixed_windowed` over
`LazyReducer` (the uncontrolled `_run_fixed` is untouched: a `wait()` over every future per completion
would make it O(n²)). Pooled and adaptive: one loop each, a control-less `_Window` passes everything
through. SUBMITTED is emitted when tasks become known. The cancel fold is `LazyReducer.frontier()` /
the pooled `ready` nodes by first leaf, through one `_folded`. Adaptive: `t0` at window submission; a
timeout wake calls neither `next_tasks` nor `stop`; once a check sees CANCELLED the loop drains without
the stop exit. Extra tests (`tests/extra/m65/test_a2_hub_window.py`) fail on a mutant without the timed
wake (next STARTED 0.74 s after resume) and on one stamping `t0` at batch time (1.66 s duration).

### Iteration 2 — A2.2 peer routes (m65 frozen 97/97, three serial runs 97/97)

Worker (`process_and_reduce`): `pause`/`resume` set a flag; the loop pops no leaf and sends no steal
while paused or cancelled. `cancel` acts once: clears `mine`, sends `("cancelled", address, processed,
reducer.hand_in())`; `PeerReducer.hand_in` returns the parked nodes and switches `settle` to forwarding
every later node to the driver as `("item", level, pos, value)`. Driver: one `PeerControl` in `_peer.py`
serves both loops (`relay` per poll through one `_Outbox` per worker, inline on HTTP; `take` dedups
items by node and folds through a driver `LazyReducer` then `frontier()`; `close` before `done`).
`_run_peer` waits in `control.wait()` after SUBMITTED, before any actor. `_PEER_ROOT_TIMEOUT_S`,
reset on every paused poll; the message reads it at call time.
Deviations from the plan text:
- r2 L1 said to dedup `steal_resp` by reusing `seen` keyed `(0, leaf)`. That drops a leaf a thief hands
  back to its owner (the owner's own settle then finds `(0, leaf)` seen) and a leaf granted to the same
  thief twice. Each grant instead carries a victim-scoped id `(-1 - victim index, given#)`, a negative
  level no node key uses, kept in the same `seen`.
- The driver's "first `cancelled` per address" check was dropped: the worker sends one, a retried copy
  is identical, and re-applying it is idempotent (a mutant without the check passes every test).
- Dead `n == 0` branches in both driver loops removed (`_run_peer` returns before them). The thread
  driver now broadcasts `done` before raising its `TimeoutError`; without it the `finally` joins each
  unreleased actor for 30 s.
Extra tests (`tests/extra/m65/test_a2_peer_control.py`), each failing on a mutant: the paused deadline
(no reset → `TimeoutError within 3.0s`), the timeout message (literal `300s`; no release → run past
30 s), the driver fold (first-leaf fold without the tree → `…78.2 != …78.1`), the idempotent handlers
(no grant dedup → processed 2; cancel twice → two `cancelled`).

### Iteration 3 — A2.3 CI pin, docs; the late-cancel test made deterministic

`GRAPHED` pinned to graphed PR-A1's head `61bde20`; `docs/design.rst` "Pausing and cancelling a run"
(example executed: `cancelled True running`, 3/3); `docs/improvements.rst` names the transport-peer
routes as uncontrolled. The persistent late-cancel extra test cancelled on key 39's FINISHED, which
under `-n 8` load sometimes arrived after the root, so no tag went out. It now holds leaf 19 (w0's
last, which forms the root) until the spy sees the cancel relayed, then releases it: the root forms
after the cancel. A driver that ignores a root once it has relayed a cancel fails it
(`TimeoutError`).

## A3 (plan-A3.md, frozen `freeze-m65a3` = `273e420`)

Run: `pytest tests/frozen/m65/test_m65a3_control_{submit,dask,parsl}.py` (parsl: venv `bin` on `PATH`).

### Iteration 1 — A3.1 SubmitRunner honours the control (A3 frozen 40/40 first run)

`control=` and public `control`, read once per run beside `monitor`; entry check before subscribe;
CANCELLED reset in `run()`'s `finally`. A2's `_Window` is the dispatch point (imported from
`local.executors`); `_fill` runs `take`, and when the window is full with work held re-reads
`_task_slots()` (`task_slots()` via `getattr`, else `n_workers()`, floored at 1) and widens. The loop
waits on `done_q` with `_PAUSED_WAKE_S` while `window.held`, else blocks; paused with nothing
outstanding it blocks in `control.wait()`. Controlled fixed path is a separate `_run_fixed_windowed`
(uncontrolled `_run_fixed` untouched): SUBMITTED up front, combines submitted in `(out, a, b)` order
once both input futures completed, failures surfaced by `exception()` then `_result`, cancel folds the
completed nodes by first leaf. Adaptive: batches go to `window.held`; `window.cancelled()` before the
stop check; `stopped` never assigned in engine.py. `DaskBackend.task_slots` = sum of `nthreads`;
`ParslBackend.task_slots` = connected HTEX workers or TPE `max_threads`, no wait.
Deviation: the controlled fixed path has no `n == 0` return (`plan_tree(0)` is `([], None)` and the
loop returns the empty EXHAUSTED result; probed).

### Iteration 2 — A3.2 CI legs and docs

`test-dask` runs `test_m65a3_control_{dask,submit}.py`, `test-parsl` runs
`test_m65a3_control_parsl.py`; design.rst names SubmitRunner in the run-control opener and adds its
bullet (slots, timed wake, merges after inputs, `replicate_broadcast=True` on dask, control read per
plan).

### Iteration 3 — default-path trim

The interleaved A/B (`graphed-workdir/lanes/debug/probes/a3_adapt_ab.py`, base = `git archive 5e519c6
src`) showed the uncontrolled adaptive run slower by ~1.5 ms min / 2.7 ms median over 4000 tasks: the
loop called `_fill` and `window.cancelled()` per completion. Both are now skipped when they cannot act
(`_fill` only while `window.held`, `cancelled()` only with a control); the rerun is inside noise.

### Iteration 4 — review r1 repairs

M1: the uncontrolled adaptive `refill` starts each batch directly instead of routing it through the
window, so a `next_tasks` that drips batches no longer pays `_fill`/`take` per completion.
A/B `graphed-workdir/lanes/debug/reviews/a3i1_default_ab.py 20 base=<git archive freeze-m65a3 src>
head=installed`: output in `graphed-workdir/lanes/debug/impl/a3.4-ab.out`. L2: `_fill` sets
`window.size` to every re-read and loops only when it grew, so a shrunk pool narrows the window
(`reviews/a3i1_shrink.py` peak 4 -> 1). L1: design.rst says only SubmitRunner and the local
executors take `control=`; the dask/parsl runners get the attribute set.

### Iteration 5 — review r2 repair (M1')

`run()` sends an uncontrolled adaptive run to `_run_adaptive` exactly as at `freeze-m65a3`; the
windowed loop is `_run_adaptive_windowed(control: RunControl)`, with its `control is None` branches
gone, mirroring the fixed split. A/B `reviews/a3i1_default_ab.py 60`, both leg orders with a base2
leg (base = `git archive freeze-m65a3 src`): `graphed-workdir/lanes/debug/impl/a3.5-ab.out`.

### Iteration 6 — review r3 repair (H1, owner ruling 'refreeze approved')

Dispute `disputes/test_m65a3_controlled_stop.md` ruled; witness `tests/frozen/m65/test_m65a3_control_stop.py`
(tag `freeze-m65a3-fixup`). `test-dask` now runs it beside the two A3 files, so its diff-cover reaches
the windowed stop exit. No src change. Diff-cover outputs (`graphed-workdir/lanes/debug/impl/`):
`a3.6-diffcover.out` (the three A3 frozen files vs `freeze-m65a3`) and `a3.6-dask-diffcover.out`
(the test-dask job's command vs `origin/main`), both exit 0 with only the stray-callback `continue`
missing. `a3.6-stop-mutant.out`: the windowed `reason = None` mutant fails the new test.
`a3.6-dask-job.log` had one m44 worker-death HARD TIMEOUT under load (455 s wall vs 170 s);
it passes alone 3/3 and the job rerun `a3.6-dask-job-2.log` is 312 passed.

### Iteration 7 — residual fold (L3)

`origin/main` (A2 squash `0b085f1`) merged with `-s ours`: its tree equals `lane/debug`'s, so the merge
changes no file. CI `GRAPHED` re-pinned from `61bde20` to graphed `c551533` (the A1 squash on main). No src change.

### Iteration 7 — PR #24 CI: the monitored-run drain counter raced without the GIL

`test (experimental) py3.14t` hung `test_pause_stops_dispatch_then_resume_completes[fixed|adaptive]`
(run() past the 30 s join). Reproduced on a local 3.14t venv (2 hangs in 3 drives of the body);
`faulthandler` put the runner thread in the trailing-event `_wait_until` with all 40 tasks finished.
Cause: `ThreadBackend` calls the subscribed handler on its worker threads and `events_seen[0] += n`
is a read-modify-write, so without the GIL increments were lost and the drain waited out
`_DRAIN_TIMEOUT_S`. The handler now increments under a lock; 12/12 drives complete, and the A3
submit + stop, m63 thread and m42 frozen files pass twice on 3.14t. Searched `src/graphed_executors`
for other list-cell counters written from callbacks: single occurrence. The py3.13 failures on the
same run are `test_m65a2_control_routes.py::test_paused_at_entry_holds_the_run[proc-http]` (local
HTTP hub route, untouched by A3; the pre-existing hang class from the journal); re-sampled by the push.

### Iteration 8 — PR #24 CI: HTTP peer hand-offs that beat a worker's registry were parked

`test ubuntu py3.13` (x86 and arm) timed out `test_m65a2_control_routes.py::test_paused_at_entry_holds_the_run
[proc-http]` on every attempt while the test alone passed. Instrumented the transport on a scratch branch
(every tag on send and receive, a worker-side `faulthandler` dump): all 40 leaves finished, w1's three
subtree `node` hand-offs reached w0 44 ms before w0's own `registry` did, then both workers traded
steal requests until the join expired. `http_peer_actor` keeps such early messages as `(sender, payload)`
pairs, but `process_and_reduce` handed each pair to `handle`, whose tag match then saw the sender name
and dropped it; the sibling settle-only actor unpacks the pair. Reproduced on macOS by delaying w0's
registry (deterministic hang); the fix unpacks the pairs. Regression test `tests/extra/m65/
test_a2_http_prebuffer.py` monkeypatches the handshake to delay w0's registry and witnesses that w1's own
leaves finished before it went out: fails on the old tree (join expires), passes 3/3 on the fix. Searched
`src/graphed_executors/local` for other `prebuffered` consumers: the settle-only actor is the only other
one and already unpacks. The test's pause never reaches this route (`_run_peer` waits at entry before
any actor exists), so the hang was an ordinary monitored proc-http run; nothing in A3 touches it.

## B (plan-B.md, frozen `freeze-m65b` = `6c82849`; graphed `b5a2a71`)

### Iteration 1 — B3 hub routes: lean events, lazy labels, per-worker push

`_run_with_emit` drops STARTED and the label in lean mode; `_thread_task` and `_proc_task_shared` call
`process` directly when there is nothing to emit (no monitor; no buffer and no push monitor).
`_proc_init(profiler_factory, event_q, *, monitor_factory, lean)` stops and joins a live drain thread,
assigns every worker global, builds the push monitor before registering `_proc_drain_final`, and
`_proc_drain_final` sends the exit profile to that monitor. The driver reads `worker_monitor_factory`/
`lean_events` once per run; the hub pool gets them through a `functools.partial` initializer and no
event queue, the collector is skipped when pushing, and a kept (persistent) pool is respawned when the
pickled push factory or the lean flag changes (test 12b). Hub frozen legs (tests 11 hub/pooled/adaptive,
12 proc-hub, 12b hub, 14) pass; test 14 then m37 `test_inprocess_paths.py` pass in one process.

### Iteration 2 — B4 peer actors and SubmitRunner

`process_and_reduce` and the four actors take `monitor_factory`, `lean` and `keys`: a factory builds
the actor's own monitor, which receives its task events and profile trees instead of `("events", …)`/
`("profile", …)` to the driver; lean drops STARTED and the label; events carry `keys[leaf]`. `_run_peer`
passes the sorted task keys only when a monitor is attached and they are not `0..n-1` (the pinned pool
takes positional task arguments only). `SubmitRunner` builds one `RunContext` per run in `run()` with
two appended defaulted fields (pickled factory, lean); workers push through `_WORKER_MONITORS` under
`_WORKER_MONITORS_LOCK`; with push the driver neither subscribes nor waits, and the four drain sites wait
for `ctx.events_per_leaf` events per leaf. B frozen executors files 31/31 plus the dask file pass.

### Iteration 3 — B5 CI pin and docs

`GRAPHED` pinned to graphed `lane/debug-b`'s head; the `test-dask` line gains the two B submit/dask
files so that job's `submit/**` diff-cover sees the in-process worker-side `engine.py` lines. The
design page's "Watching a run" states lean events and per-worker push; improvements notes live parsl
events under push.

### Iteration 4 — review r1 repair (L1; H1 lands in graphed)

A kept hub pool is respawned when anything its initializer fixes changes — the push and profiler
factories, the lean flag, and whether it feeds the collector (`_pool_init` is the one source for both
the pool and the key); the old key named only push and lean, so a pool built for an unmonitored run
kept `event_q=None` and a later monitored run saw no worker events. New
`tests/extra/m65/test_b_kept_pool_init.py` fails on the old key (no FINISHED) and passes. `GRAPHED`
re-pinned to graphed `lane/debug-b`'s head, which carries the H1 server fix. The cross-run
misdelivery of a kept pool's trailing events into the next run's monitor is unchanged.

### Iteration 5 — review r2 fold (N2)

The `GRAPHED` pin comment names PR-B's head, and the pin moves to graphed `lane/debug-b`'s new head
(one Perspective update per ingest frame).

### Iteration 6 — thread-http trailing events

The r2 fold's gate run failed `test_peer_events_carry_task_keys[thread-http-default]` (one FINISHED
missing) under a loaded machine. Cause (pre-existing since the thread peer drain): a returned actor's
HTTP lane can still be POSTing its last events when the driver joins the threads and polls once. The
driver now closes the worker transports (draining their lanes, bounded by `CLOSE_DRAIN_S`) before that
poll. New `tests/extra/m65/test_b_http_trailing_events.py` slows w1's event POSTs; it fails on the old
drain and passes.
