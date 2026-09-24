# Dispute record: the controlled adaptive StopCondition exit has no frozen witness

**Artifact:** `tests/frozen/m65/test_m65a3_control_submit.py` (+ `m65a3_bodies.py`). Its only
adaptive stop-condition test is `test_stop_condition_does_not_end_a_cancel_drain`, whose condition
fires only inside a cancel drain (plan-A3 A3-2: "the stop exit is reachable only while no check has
seen CANCELLED"). No frozen test runs a controlled adaptive plan whose `StopCondition` fires
outside a drain; `test_unused_control_is_bit_identical` runs an adaptive plan with no stop condition.

**Gap:** review r3 (`graphed-workdir/lanes/debug/reviews/unit-A3-impl-r3.md`, H1). The r2 repair
(7d1b738) gives the uncontrolled adaptive run the pre-A3 `_run_adaptive` verbatim and moves the
controlled run to `_run_adaptive_windowed`, so the stop exit (`stopped = reason`,
`backend.cancel(list(outstanding))`, `break`) and the stray-callback `continue` became added lines
reachable only under a control. `diff-cover --compare-branch=freeze-m65a3` on the A3 frozen files
gives 171 lines, 4 missing (`engine.py` 562, 571-573): 97.7 % < 98 % (§B.3 diff gate, frozen hits).
A mutant that ignores the controlled stop (`reason = None`) passes all 40 A3 submit + dask frozen
tests (`reviews/a3r3_stop_mutant.out`); HEAD returns TARGET_EVENTS with 5 partitions where the
mutant returns EXHAUSTED with 40.

**Why not routed around:** the shared-loop alternative pays the window checks on every completion
of the default path (review r1 M1 / r2 M1', measured +0.7–2.1 ms median drip over ~47 ms), against
the brief's "the default path's cost must not grow"; a shared stop-check helper puts a call on the
same path. The controlled copy is correct at HEAD; only the frozen witness is missing.

**Proposed correction (needs an owner ruling; `--allow-refreeze tests/frozen/m65/test_m65a3_control_stop.py`):**
- NEW frozen file `tests/frozen/m65/test_m65a3_control_stop.py` (test author, not the implementer):
  `SubmitRunner(ThreadBackend(2))` on an adaptive plan with `StopCondition(target_events=5)` under an
  unused `RunControl()`; assert `(res.stopped, res.n_partitions)` equals the bare run's and equals
  `(StopReason.TARGET_EVENTS, 5)`, and `ctl.state is RunState.RUNNING`. Closes when the `mut` leg of
  `a3r3_stop_mutant.out` fails it and the A3-frozen diff-cover exits 0 at >= 98 %.
- Tag `freeze-m65a3-fixup` (precedent: `freeze-m47-fixup`); a `README-m65a3-fixup.md` row.
- Implementer: add the file to the `test-dask` job's pytest list in `.github/workflows/ci.yml`
  (that job's diff-cover gates `submit/**` from its own list); rerun the gate; attempts.md.

**Disposition:** RULED — refreeze approved by the owner ("refreeze approved", 2026-09-23). Recorded by the lane lead (team-lead), 2026-09-23.
