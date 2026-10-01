# m68c executors TEST_SANITY, round 1

**Verdict: FAIL. The suite cannot be sanity-checked yet. No defect was found in the tests.**
plan.md §7 and plan-executors §6 set the sanity revision as executors `db8fb0a` over graphed's
`feat(runners)` tip, meaning unit G implemented. That tip does not exist. The `m68c-graphed` worktree
is detached at main `d0ad16b`, and `git for-each-ref` in graphed lists no m68c, shuffle or runners
ref. So this round ran against executors `e41032a` over graphed `d0ad16b`, which is the pre-G state.

## Issues

1. **The sanity revision is missing.** On `d0ad16b`, all 20 tests fail at the fixture, before any
   executors V2 code runs, so the §6 "Fails" reasons cannot be observed. Those reasons are
   `next_tasks`, `empty`, `UnboundService` and `process`. Rerun this round at G's `feat(runners)` tip.
2. **mypy is not clean yet.** Repo config (`strict = true`): `mypy tests/frozen/m68c` reports 3
   `call-arg` errors, all on `join_plan(reduce=, combine=, empty=)` in `m68c_harness.py`. So the
   precommit gate FAILs at `prek`. These errors should clear at G's tip. Recheck there.
3. **Discrimination is not shown for the tests that need G's payload format, stage processes or V2
   `SequentialRunner`.** No wrong implementation could be built for them at this revision:
   - peer edge
   - driver edge
   - given, managed and no-service value tests
   - dask and parsl service joins, including the byte bounds
   - HTCondor pre-check and derivation legs

## Checks run

- **(1) Collects.** 21 items collect: thread 12 (incl. 2 refusal params), dask 2, parsl 3,
  htcondor 4.
- **(2) Failure reasons.** Command: `pytest tests/frozen/m68c -q -p no:cacheprovider -rfEs --tb=line`.
  Result: 20 failed, 1 skipped.
  - 19 fail with `m68c_harness.py:379: TypeError: join_plan() got an unexpected keyword argument 'reduce'`.
  - The peer edge fails with `ImportError: cannot import name 'split' from 'graphed.shuffle'`.
  - The htcondor live test is skipped (no `htcondor2`).
  - `sanity-evidence.md` is correct for this revision and needs no change.
  - The executors-side reasons come from the stand-in plan (`probes/e_failreasons.py`), rerun here,
    and match §6: `next_tasks` for SubmitRunner, ThreadExecutor and ProcessExecutor; `tasks` for
    `transport_run_plan` and `parsl_run_plan`; `process` for HTCondorRunner and `submit_driverless`.
  - The pre-cancelled control's reason was probed separately (SubmitRunner, control cancelled on
    entry, V2 stand-in): `AttributeError 'DurablePlanV2' object has no attribute 'empty'`, which
    matches §6.
- **(3) Determinism.** Two full runs give identical sorted FAILED/SKIPPED lines. Under the stand-in
  below, the mid-run cancel, boundary cancel, failed-submit and unbound tests passed in 5 of 5 repeats.
- **(4) Lint.** `ruff check` and `ruff format --check` on `tests/frozen/m68c` are clean. mypy: see
  issue 2.
- **(5) Coverage.** The main-matrix step `pytest tests/frozen tests/extra --cov=graphed_executors`
  collects `tests/frozen/m68c`. Over `test_m68c_submit_thread.py`, `submit/engine.py` measures 18%
  (import only, pre-G). Driving the V2 path through the stand-in raises it to 27%, so the instrument
  is live. The test-dask, test-parsl and test-htcondor steps do not list m68c yet. That belongs to the
  executors `ci` commit (plan.md §7), not to this suite.
- **(6) Discrimination** against a stand-in, in a throwaway copy of the suite outside the worktree:
  - The G stand-in `join_v2` is main's `join_plan` output with `services`, `next_tasks=None` and
    `empty` set.
  - A ~40-line `_run_stages` model is patched over `_run_fixed[_windowed]`. It has a barrier,
    completion-time and `wait()` cancel checks, and submits through `_RunTasks`.
  - A refusal helper wraps the three entry points. A fake V2 `require_bound` raises `UnboundService`.
  - Rerun from `lanes/services-v2/probes/e_sanity1_standin/`.

  The correct stand-in passes all 9 selected tests. Each wrong implementation fails its test:

  | Wrong implementation | Test that refuses it | Failure |
  |---|---|---|
  | map tasks via `self.backend.submit` (bypass `_RunTasks`) | `test_a_failed_map_submit_cancels_the_run_s_queued_map_tasks` (also the mid-run test) | `queued ⊄ cancelled()` (`[]`) |
  | CANCELLED checked only at stage submission | `test_a_mid_run_cancel_cancels_the_queued_map_tasks` | `unrun` empty; every map ran |
  | `control.wait()` result ignored | `test_a_cancel_at_a_stage_boundary_submits_no_next_stage` | `stopped is None`, next stage submitted |
  | V2 stages dispatched before the ServiceSet prologue | `test_an_unbound_service_without_a_launch_is_refused_before_any_stage_task` | `plan_keys()` lists 16 `map_write` keys |
  | refusal after `require_bound` | local ×2, dask and parsl refusal tests | `UnboundService` raised, not TypeError |
  | TypeError not naming `SubmitRunner(` | same 4 | message assertion |

  - Not in this suite: "identity unchanged by binding" and "post-join operations run". Those are
    graphed unit-G tests. `join_v2` has no operation after the join.
  - **Byte-bound premise**, measured on graphed main with AwkwardBackend `partition` + `to_wire`, over
    one side, 8 map tasks × 8 dests, pickled as the measure pickles:
    - per-dest slices / map payloads = 1.05, under the driver bound of < 2;
    - peer reship = 8.0, over the peer bound of ≥ 4.

    This covers only G's stated `{dest: to_wire(block)}` format.

## No test found that cannot pass under the plan as written

- The boundary test requires `control.wait()` after the stage's input barrier, which is §4's "a
  pause holds the next stage".
- The refusal tests do not assert "server count stays 0". The README explains why: no endpoint is
  bound, so the assertion would be vacuous. This is a justified deviation from §6.
