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
