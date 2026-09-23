# m65 frozen suite — graphed-executors (debug lane: run control)

Milestone **m65** (lane plan `graphed-workdir/lanes/debug/plan.md`; one unit per sub-plan, each
appended here at its freeze and never rewritten). **Frozen — read-only after each unit's freeze tag.**

## Unit A2 — the local executors honour the run control (`plan-A2.md`, tag `freeze-m65a2`)

Contract: plan-A1 A-2 (runner contract) and plan-A2 §Frozen-test contract. Route set R =
`m65a_probe.ROUTES` (hub, peer ipc/http, pinned, pooled combines, adaptive; thread and process),
`max_workers=2`, 40 tasks.

| File / test | Contract | What it witnesses |
|---|---|---|
| `m65a_probe.py` | helper (A3, B import it) | picklable `Probe` (one-hot / order-sensitive float scalar / int partials; `fail_key`, `hold_key` file handshake; `cancel_key` through the `CONTROLS` registry), `add_tuples`, `zeros(n)`, `tasks`, `make_plan` (`OneBatch` adaptive `next_tasks` counting calls), `Recorder` monitor, `Background` run with a 30 s bound, `ROUTES` |
| `test_unused_control_is_bit_identical` | 13 | value, `n_partitions`, `n_combines`, `stopped` equal the uncontrolled run; float partials on fixed/peer routes, int on adaptive |
| `test_pause_stops_dispatch_then_resume_completes` | 14 | pause at the first FINISHED: STARTED count flat over 0.6 s and below 40; 40 SUBMITTED seen by then; resume completes EXHAUSTED |
| `test_paused_at_entry_holds_the_run` | 14b | paused before `run()`: 0 STARTED after 0.5 s, then resume completes; paused then cancelled: CANCELLED, nothing run, control reset |
| `test_cancel_drains_and_folds_exactly_the_completed_tasks` | 15 | cancel at the first FINISHED: CANCELLED, each completed task folded once, every STARTED key folded, `n_combines == n_partitions - 1`, control reset, returns within 30 s |
| `test_failure_during_cancel_drain_raises` | 15b | key 0 in flight at the cancel fails: `run()` raises `m65 fail 0`, control reset |
| `test_cancel_no_check_saw_is_reset` | 15c | cancel from key 39 (thread routes): control reset, next run full; on the hub and pooled thread routes the run itself equals the uncontrolled float value, EXHAUSTED |
| `test_late_node_reaches_a_cancelled_worker` | 15d | thread peer ipc/http, leaf 23 held past the cancel: leaves 20–23 all folded, CANCELLED |
| `test_stop_condition_does_not_end_a_cancel_drain` | A2 r3 L1 | adaptive routes: a `StopCondition` only key 0's completion can satisfy fires inside the cancel drain; the run still reports CANCELLED and folds every started task |
| `test_cancelled_control_does_no_work` | 16 (+ A2 r2 E8) | entered CANCELLED, 40 tasks and zero tasks: CANCELLED, empty value, no events, `next_tasks` never called, control reset |
| `test_control_attribute` | 17 | `control is None` by default; honoured when given as `control=` and when assigned after construction |
