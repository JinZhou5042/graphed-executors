# m68c executors TEST_SANITY, round 3

**Verdict: FAIL, blocked on sequencing. No defect was found in the tests.** Nothing has changed since
round 2:

- The suite is still `e41032a`. `git diff --stat e41032a..HEAD` shows only the sanity records.
- The plans are unchanged. `plan-executors.md`, `plan-graphed.md` and `plan.md` all have mtimes before
  the round-2 commit.
- graphed is still at `d0ad16b`. No `git for-each-ref` entry names runners, shuffle or m68c. In
  `graphed.shuffle`, `dir()` has no `split` or `pick`. No other checkout under `~/vibe-coding/cloud`
  carries unit G.

plan.md §7 runs this check at graphed's `feat(runners)` tip, so the §6 "Fails" reasons still cannot be
observed.

## Issues

1. **Check (2) cannot be run.** All 20 tests fail at the fixture:
   - 19 fail with `m68c_harness.py:379 TypeError: join_plan() got an unexpected keyword argument 'reduce'`.
   - The peer edge fails with `ImportError: cannot import name 'split' from 'graphed.shuffle'`.
2. **Check (4) is not clean.** `mypy tests/frozen/m68c` reports exactly 3 `call-arg` errors, for
   `reduce`, `combine` and `empty` on `join_plan` at `m68c_harness.py:379`. These are G's keywords.

## Checks, as rerun this round

- **(1) Collects.** 21 items: thread 12, dask 2, parsl 3, htcondor 4.
- **(2) Failure reasons.** 20 failed, 1 skipped (the htcondor live leg, no `htcondor2`), for the reasons
  in issue 1. `sanity-evidence.md` matches this revision and needs no correction.
- **(3) Determinism.** Two full runs gave identical sorted FAILED/SKIPPED lines (21 lines, diffed).
- **(4) Lint and types.** `ruff check` and `ruff format --check` are clean (rc 0). mypy: see issue 2.
- **(5) Coverage.** `--cov=graphed_executors --cov-branch` over the thread file measures
  `submit/engine.py` at 18%, which is import only. The same file under the round-2 stand-in's `good`
  variant measures 28%.
- **(6) Discrimination.**
  - **Setup.** The round-2 stand-in (`lanes/services-v2/probes/e_sanity2_standin`) was reapplied to a
    throwaway copy of the suite and run over the 10 mechanism tests.
  - **Correct model.** `good` passes all 10.
  - **Mutants.** Each wrong implementation fails the same tests as in round 2's table:

    | Mutants | Failed tests (each) |
    |---|---|
    | `pick_per_map`, `pick_whole`, `driver_on_peer`, `pick_on_driver`, `driver_whole`, `driver_futures` | 1 |
    | `bypass` | 2 |
    | `submit_only_check`, `ignore_wait` | 1 |
    | `dispatch_first` | 6 |
    | `refuse_after`, `refuse_badmsg` | 4 |

  - **Not constructed**, for the same reasons as round 2:
    - "identity unchanged by binding" and "post-join operations run" are unit-G tests. A grep of
      `tests/frozen/m68c` finds neither.
    - The dask key-without-stage mutant and the HTCondor `stages[i].process` mutants need G's plan
      objects.
    - Value equality with `SequentialRunner` needs G's values.

## Tests that cannot pass under the plan as written

None. The plans have not changed since round 2's check.

## Note (not an m68c defect)

`stop_htex` leaves HTEX `process_worker_pool` processes behind after the fixture: one pool per module
run. The existing `tests/frozen/m63/test_m63_submit_parsl.py` leaves them behind the same way, so this
behaviour predates m68c. The pools from this round were killed.
