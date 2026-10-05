# m68c frozen suite: join and repartition plans (`DurablePlanV2`) through `SubmitRunner`

Source of truth: `lanes/services-v2/plan-executors.md` §4 (design) and §6 (tests), `plan.md` §7 (layout
and CI jobs). **Frozen: read-only after the `freeze-m68c` tag.**

## How it runs

- `test_m68c_submit_thread.py`: every OS (ThreadBackend, local executors); also listed in the test-dask
  step (§7), whose diff-cover gates `submit/**`.
- `test_m68c_submit_dask.py`: the test-dask job (`importorskip("distributed")`), a process `LocalCluster`.
- `test_m68c_submit_parsl.py`: the test-parsl job (`importorskip("parsl")`), a started HTEX.
- `test_m68c_htcondor.py`: the driverless legs run anywhere under a bindings recorder; the live
  legs need `htcondor2` and a personal HTCondor (skipped without the bindings), in the test-htcondor job.
- `m68c_harness.py`: the fixtures (join plans whose left side calls a server through the `SCALE`
  External), the counting HTTP stand-in (`StandIn`, GETs of `/sf` counted apart from readiness GETs of
  `/`), `RecordingBackend` (submits with args, broadcast handles, cancels, task starts, gather args at
  run time, the byte measure), `ParkingControl`, and the managed child (`python m68c_harness.py <port>
  <report>`, stdlib only).

The fixtures need unit G (`join_plan(reduce=, combine=, empty=)`, `DurablePlanV2.services`,
`graphed.shuffle.split`, `SequentialRunner.run(v2)`), so the stated failure reasons are those at
graphed's `feat(runners)` tip; on graphed main every test fails at the fixture
(`.graphed/m68c/sanity-evidence.md`).

## Traceability

"Fails" is the first failure at the sanity revision (executors `db8fb0a` over graphed with unit G).

| Test | Plan clause | Fails | Discriminates |
|---|---|---|---|
| `thread::test_a_given_endpoint_join_equals_the_sequential_runner_and_calls_the_server` | §6 service join, given leg; §4 lifecycle | `next_tasks` | stages that skip the bound process (0 `/sf` GETs); a wrong fold or stage order (value ≠ SequentialRunner) |
| `thread::test_a_managed_child_lives_exactly_as_long_as_the_join_run` | §6 service join, managed leg (m68a lifecycle) | `next_tasks` | release before the stage tasks (no body = child pid), a child outliving `run` |
| `thread::test_an_unbound_service_without_a_launch_is_refused_before_any_stage_task` | §6 unbound with no launch; §4 dispatch after the V1 prologue | passes (guard) | a V2 dispatch placed before the ServiceSet prologue |
| `thread::test_the_peer_edge_picks_each_map_dest_pair_on_a_worker` | §6 pick edge, peer leg; §4 keys, `pick` per (map, dest) | `next_tasks` | missing/duplicate pick keys, a gather fed whole map dicts or other dests' slices, driver-side picking on a peer backend (measure < parts/2) |
| `thread::test_the_driver_edge_ships_gathers_their_own_slices` | §6 pick edge, driver leg | `next_tasks` | pick tasks on a `peer_data_movement=False` backend, future args to gathers, whole maps per gather (measure ≥ 2×) |
| `thread::test_a_join_without_services_equals_the_sequential_runner_bit_for_bit` | §6 no service; G §3.3 `plan.value`, G §3.4 counts | `next_tasks` | a value not from `plan.value(last)`, counts that include picks |
| `thread::test_a_control_cancelled_before_the_run_submits_nothing` | §6 no service, pre-cancelled control; §4 early return | `empty` | a V1 early return (`plan.empty()`), any submit, a control left CANCELLED |
| `thread::test_a_mid_run_cancel_cancels_the_queued_map_tasks` | §6 mid-run cancel; §4 completion-time check | `next_tasks` | a CANCELLED check only at stage submission (every map runs, none cancelled) |
| `thread::test_a_cancel_at_a_stage_boundary_submits_no_next_stage` | §6 cancel at a stage boundary; §4 `control.wait()` before a stage, picks after it | `next_tasks` | ignoring `wait()`'s result; picks submitted before the wait |
| `thread::test_a_failed_map_submit_cancels_the_run_s_queued_map_tasks` | §6 cancel before release (m68a twin) | `next_tasks` | `_run_stages` submitting through `self.backend.submit` (queued maps never cancelled) |
| `thread::test_a_local_executor_refuses_a_join_plan_naming_submit_runner[thread,process]` | §6 refusals; §4 refusal first, before `require_bound` | `UnboundService` | refusal after `require_bound`, an AttributeError, a message not naming `SubmitRunner(`/`DurablePlanV2` |
| `dask::test_dask_runs_a_given_endpoint_join_equal_to_the_sequential_runner` | §6 DaskBackend; §4 keys carry stage and dest | `next_tasks` | keys without the stage index (equal partition counts, different side values: dask returns the other side's result) |
| `dask::test_the_dask_peer_transport_refuses_a_join_plan_naming_submit_runner` | §6 refusals | `UnboundService` | as the local refusal |
| `parsl::test_parsl_htex_runs_a_service_join_moving_each_map_result_once` | §6 ParslBackend over HTEX, driver edge | `next_tasks` | pick tasks on HTEX (each map dict reshipped per dest: measure ≥ 2×) |
| `parsl::test_parsl_htex_with_the_pick_task_edge_moves_each_map_result_per_dest` | §6 ParslBackend live control | `next_tasks` | an edge that ignores `peer_data_movement` (measure < parts/2) |
| `parsl::test_the_parsl_peer_transport_refuses_a_join_plan_naming_submit_runner` | §6 refusals | `UnboundService` | as the local refusal |
| `htcondor::test_submit_driverless_accepts_a_lambda_stage_process_and_its_plan_pkl_runs_elsewhere` | §6 HTCondor, lambda stage processes accepted (owner ruling 2026-10-01, `freeze-m68c-fixup`): a recorder logs a submit, and the run dir's `plan.pkl` run by `SequentialRunner` in a fresh process (harness on `PYTHONPATH`, as a `user_modules` ship) equals the in-process value | `ValueError` naming `stages[i].process` at `fc9e316` | a stage-process pre-check that refuses a lambda; a `plan.pkl` the fresh process cannot load or that runs to another value |
| `htcondor::test_submit_driverless_derives_a_join_plan_s_service_nodes` | §6 SERVICE-node derivation (m68b) | `process` | a pre-check that refuses importable stage processes; `plan.services` not reaching `_service_nodes` |
| `htcondor::test_a_live_join_over_pool_pilots_calls_the_given_server` | §6 live V2 run | `process` (skipped without bindings) | pilots that never call the server, a wrong value |
| `htcondor::test_a_live_join_over_pool_pilots_runs_a_lambda_stage_process` | §6 HTCondor, lambda stage processes accepted (owner ruling 2026-10-01, `freeze-m68c-fixup`): `htcondor_runner` over pool pilots runs the lambda-`combine` join to `SequentialRunner`'s non-empty value | `ValueError` naming `stages[i].process` at `fc9e316` (skipped without bindings) | a pre-check that refuses a lambda; a broadcast pilots cannot unpickle; a wrong fold |

## Choices the plan leaves open

- *Mid-run cancel:* the map bodies are made slow deterministically, not by their cost: under
  `hold(tag)` every partition read after the first waits until the test sees a map key in
  `cancelled()` (then `release`), so a correct driver always cancels queued maps and a
  submission-only check always runs them all.
- *Refusals:* no endpoint reaches a refused plan, so the plan's "server count stays 0" is unreachable
  and is not asserted; the refusal's type and message, raised in place of `UnboundService`, are.
- *ProcessExecutor:* the refusal uses `ProcessPoolExecutor`, which the deprecated `ProcessExecutor`
  aliases (same `_BaseExecutor.run`).
- *Counts:* the no-service test also pins `n_partitions`/`n_combines` to SequentialRunner's (the
  unit-review constraint "counts as in SequentialRunner").
- *parts=8:* `join_plan(..., steps_per_file=8)`: 8 map tasks per side and 8 dests.
