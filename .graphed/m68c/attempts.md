# m68c executors implementer — iteration log

Freeze: freeze-m68c (e41032a suite). `git diff freeze-m68c -- tests/frozen/` must stay empty.
Graphed under test: editable m68c-graphed at 95b1b7b (G's feat(runners) tip).

## Iteration 1 — plan-executors.md §4/§5 as one pass, split into the §7 commits
- submit: `SubmitRunner.run` takes a `DurablePlanV2` (overloads); `_run_scoped` branches only on the
  early CANCELLED value (`None`) and the body dispatch to `_run_stages`; the ServiceSet prologue,
  bind, cancel-before-release callback and resolve are the V1 lines. `_run_stages`: every task
  through `_RunTasks`; keys `<kind>.<s>`/task index and `pick.<s>.<t>`/dest; per-stage barrier on its
  inputs' tasks, then `control.wait()`, then that stage's picks and tasks; stage process broadcast
  once per stage (`_fingerprint`/token); map tasks return `graphed.shuffle.split`; the (map, dest) edge
  is a `pick` task when `peer_data_movement`, else one driver fetch per map and a driver-side `pick`;
  all stage inputs are top-level positional args; the first failed future in a barrier raises via
  `_result`; a completion that finds the control CANCELLED returns `stopped=CANCELLED`, `value=None`;
  value `plan.value(last)`, counts as SequentialRunner (no-input stages / the rest; picks uncounted).
- refusals: `common.refuse_staged` first statement in `_BaseExecutor.run`, `transport_run_plan`,
  `parsl_run_plan` (before `require_bound`).
- htcondor: one `_require_plan_importable` for `HTCondorRunner.run` and `submit_driverless`
  (V1 roles as before; V2 each stage's resolved process as `stages[i].process`).
- ci: m68c files added to the test-dask, test-parsl and test-htcondor steps; `.coveragerc-htcondor`
  header. The `GRAPHED` pin bump waits on graphed's m68c merge SHA (none exists yet).
- tests/extra/m68c: a failed map task raises at its barrier before any gather/pick submit; a cancel
  during the last stage returns `value=None`, CANCELLED, counts (3, 1), control reset.
- docs: design.rst "Running a join or repartition plan" (executed two-file example, output matches),
  improvements.rst ceiling + refusal list, changelog bullet.
- Finding (not changed, plan's stdlib `_fingerprint` idiom kept): a V2 plan whose callables live in
  `__main__` fails `SubmitRunner` even on ThreadBackend (`PicklingError: not the same object as
  __main__.scale`), unlike V1: builder `OpSpec`s are cloudpickled, so `resolve()` returns copies of
  `__main__` functions that stdlib pickle cannot name. The docs say so; the cause-level fix (builders
  keep the original as `live`, or a by-value broadcast) is a decision for the lead.

Mutants on the real implementation (each restored, `cmp` against the saved post file): no barrier
exception check → extra failed-map test FAILS; no final-settle cancel check → extra last-stage cancel
FAILS; no CANCELLED check in the barrier → frozen mid-run cancel FAILS; ignoring `wait()`'s result →
frozen stage-boundary FAILS; stage tasks via `backend.submit` → frozen cancel-before-release FAILS;
ignoring `peer_data_movement` → frozen peer edge FAILS; key without the stage index → thread peer edge
passes, frozen dask join FAILS (as the README states).

## Iteration 2 — gates measured at c7d3c9f (graphed 95b1b7b), no code change
- Full `pytest tests/frozen tests/extra` green once `grpcio-health-checking` (CI installs it) is in the
  venv; m68c: all pass, the live HTCondor test skips (no bindings on macOS).
- Main scope: per-file gate 15/15 >= 90% (engine 98.9%), diff-cover vs db8fb0a 100% (65 lines).
- test-dask scope: per-file 22/22, diff-cover 100% (69 lines incl. common/__init__, dask transport_peer).
- test-parsl scope (HTEX + subprocess coverage): report >= 90, per-file ok, diff-cover 100% (2 lines).
- htcondor scope (non-live only): diff-cover 100% on backend.py/driverless.py (15 lines); per-file
  needs the pool (CI test-htcondor job).
- precommit `--fast --no-coverage` ok; sphinx -W clean; design.rst example executed, output matches.
- Open: `GRAPHED` pin bump in ci.yml waits on graphed's m68c merge SHA.

## Iteration 3 — __main__/lambda V2 callables (r2, after 275c802's builder `live=` was rejected)
- `_run_stages` broadcasts each resolved stage process with `cloudpickle.dumps` (V1 keeps stdlib
  pickle); workers' `pickle.loads` unchanged. Covers the services-bound process too (`bind_services`
  wraps the cloudpickled copy: stdlib pickle of the bound gather_join raised PicklingError before).
- `tests/extra/m68c/test_m68c_main_reduce.py`: plain and bound __main__ reduce/combine equal
  `SequentialRunner`; both fail with the stdlib broadcast. dask process workers probed: __main__ and
  lambda reduce equal SequentialRunner after, PicklingError before.
- HTCondor pre-check stays stdlib (frozen refusal tests rely on it); design.rst says so.

## Iteration 4 — lambda stage processes accepted on HTCondor (owner ruling 2026-10-01, `freeze-m68c-fixup`)
- `_require_plan_importable` returns at once for a `DurablePlanV2`: its stage processes ship by value
  (cloudpickled OpSpec, cloudpickle broadcast), so pilots need not import them; V1 roles keep the
  stdlib/`__main__` check. design.rst, htcondor.rst and the changelog say join plans accept lambdas.
- Control: on the pre-change src the driverless acceptance test fails with
  `AttributeError: Can't get local object '_lambda_combine_plan.<locals>.<lambda>'` from the pre-check.
- macOS `pytest tests/frozen tests/extra -n 8`: 1490 passed, 10 skipped, 2 failed — both pass alone and
  reproduce at fc9e316 under `-n 4` (m66 ports test: `test_m66_ports.py` and `test_services_sites.py`
  both bind port 8786; m66 pilot-driver-gone flake).
- minicondor container (`m68b-minicondor:local`, graphed 322bc49 installed, CI test-htcondor order
  m66–m68c, no Triton): pytest rc 0, both m68c lambda tests PASSED (live join over pool pilots
  included); per-file gate 11/11 >= 90% (min `backend.py` 98.04%); diff-cover vs db8fb0a 100% (14 lines).
- precommit `--fast --no-coverage` ok; mypy src ok; sphinx -W ok.

## Merge of main 8c299f7 (#41) — 2026-10-01
- Merge-group run 36870516270 ejected #42: #41's `RemoteParam` read in `submit_driverless`'s DAG branch
  meets the frozen m68c `_FakeHTCondor`, which lacks it (one failure across tests/frozen m66–m68c).
- Owner-sanctioned refreeze: the fake gains `RemoteParam` (dispute
  tests.frozen.m68c.test_m68c_htcondor._FakeHTCondor.md); tag freeze-m68c-fixup2.
