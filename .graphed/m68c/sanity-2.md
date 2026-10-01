# m68c executors TEST_SANITY, round 2

**Verdict: FAIL, blocked on sequencing. No defect was found in the tests.** The state is the same as in
round 1. plan.md §7 and plan-executors §6 set the sanity revision as graphed's `feat(runners)` tip,
which means unit G has to be implemented first. That tip still does not exist:

- `m68c-graphed` is detached at main `d0ad16b`.
- `git for-each-ref` lists no m68c, runners or shuffle ref. It shows only `origin/m68-services` and the `freeze-preserve-m68*` tags.
- `graphed.shuffle` has no `split` or `pick`.

The suite is at `e41032a`, the same as round 1.

## Issues

1. **Check (2) cannot be run.** On `d0ad16b` all 20 tests fail at the fixture, before any executors V2
   code runs:
   - 19 fail with `join_plan() got an unexpected keyword argument 'reduce'`.
   - The peer edge fails with `ImportError ... 'split'`.

   So the §6 "Fails" reasons cannot be observed. Those reasons are `next_tasks`, `empty`,
   `UnboundService` and `process`. Rerun this round at G's `feat(runners)` tip.
2. **Check (4) is not clean.** `mypy` over the whole repo config reports exactly 3 errors, all `call-arg`
   on `join_plan(reduce=, combine=, empty=)` in `m68c_harness.py`. They are G's keywords.

## Checks

- **(1) Collects.** 21 items: thread 12, dask 2, parsl 3, htcondor 4.
- **(2) Failure reasons.** The outcome is 20 failed, 1 skipped (the htcondor live leg, no `htcondor2`),
  with the reasons in issue 1.
  - `sanity-evidence.md` is accurate for this revision and needs no correction.
  - `probes/e_failreasons.py`, rerun, still gives the executors-side reasons §6 names:
    - `next_tasks` for SubmitRunner, ThreadExecutor and ProcessExecutor;
    - `tasks` for both transport runners;
    - `process` for HTCondorRunner and `submit_driverless`.
- **(3) Determinism.** Two full runs give identical sorted FAILED/SKIPPED lines. Under the stand-in
  (below), the 10 mechanism tests passed in 5 of 5 repeats.
- **(4) Lint and types.**
  - `ruff check` and `ruff format --check tests/frozen/m68c` are clean.
  - mypy is not clean: see issue 2.
- **(5) Coverage.** `pytest tests/frozen/m68c/test_m68c_submit_thread.py --cov=graphed_executors --cov-branch`
  measures `submit/engine.py` at 18%, which is import only before G. Driving the V2 path through the
  stand-in raises it to 28%, so the instrument sees these tests.
- **(6) Discrimination.**
  - **Setup.** The stand-in lives in `lanes/services-v2/probes/e_sanity2_standin/` and runs in a
    throwaway copy of the suite, never in the worktree. It is round 1's stand-in with two additions:
    - G's stated payload format: a map returns `pickle({dest: wire})`, `split` = loads, and
      `pick(mapping, dest)` re-encodes `{dest: …}`.
    - A (map, dest) edge that branches on `peer_data_movement`.
  - **Result.** The correct model passes all 10 selected tests, and every wrong implementation below
    fails its test:

  | Mechanism | Wrong implementation | Test that fails | Failure |
  |---|---|---|---|
  | pick edge counts | one pick per map, not per (map, dest) | peer edge | pick set ≠ maps×dests |
  | pick edge counts | pick task returns the whole map dict | peer edge | gather input keys ≠ {dest} |
  | pick edge counts | driver picks on a peer backend | peer edge | no `-pick.` keys |
  | pick edge counts | pick tasks on a `peer_data_movement=False` backend | driver edge | `-pick.` keys present |
  | pick edge counts | driver hands each gather whole map bytes | driver edge | measure 32640 vs 4080 (8×, needs < 2×) |
  | pick edge counts | driver hands gathers map futures | driver edge | data args are `Future`, not bytes |
  | cancel before release | map tasks via `self.backend.submit` | failed-map-submit and mid-run cancel | queued ⊄ `cancelled()` |
  | completion-time cancel | CANCELLED checked only at stage submission | mid-run cancel | every map ran |
  | `wait()` before a stage | result of `control.wait()` ignored | stage-boundary cancel | `stopped is None` |
  | unbound refusal before any process call | V2 stages dispatched before the ServiceSet prologue | unbound (and 5 others) | 16 `map_write` keys submitted |
  | unbound refusal before any process call | refusal after `require_bound` | local ×2, dask and parsl refusals | `UnboundService`, not TypeError |
  | unbound refusal before any process call | TypeError not naming `SubmitRunner(` | same 4 | message assertion |

  - **Byte-bound premise.** This was measured with G's stated format (AwkwardBackend `partition` +
    `to_wire`) over the harness's `LEFT`/`RIGHT` data, with 8 maps per side and 8 dests
    (`bytes_premise.py`):
    - driver edge: 1.051× the map results, so it passes < 2×;
    - peer edge: 8× from the pick args alone, so it passes ≥ 4×.
  - **Not constructed:**
    - "identity unchanged by binding" and "post-join operations run" are unit-G tests in graphed. The
      executors suite has no such test: a grep of `tests/frozen/m68c` finds neither.
    - The dask key-without-stage mutant needs a real value, and the HTCondor `stages[i].process`
      mutants need G's `_Fold` stage process. Neither can be built at `d0ad16b`.
    - Value equality with `SequentialRunner` needs G. The stand-in's value is a constant, so it
      discriminates nothing about values.

## Tests that cannot pass under the plan as written

None found. These were checked against plan-executors §4 and plan-graphed §3:

- the gather's positional `(…, Task, *slices)` shape that the peer and driver predicates need;
- the ThreadBackend broadcast handle, which is the payload object itself and so is excluded by identity;
- `stage_index(plan, "reduce")` for the `_Fold` stage;
- the lambda `combine`, which pickles as opaque and is then refused by `_require_importable`;
- `n_combines` = gather + reduce tasks.
