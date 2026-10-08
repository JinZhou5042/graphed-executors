# m68d frozen suite: driverless service liveness (C + B + A and the port race)

Source of truth: `lanes/htcondor/m68d/plan.md` §2 (the cuts) and §4 (the frozen-test table, rows C1-C6,
B1-B3, A1-A4, P1-P4, L1-L4), and `addenda.md` (A1's absent text is m69b's "ended before it announced";
A2's match subtracts the holders' claims). **Frozen: read-only after the m68d freeze tag.**

## How it runs

- `test_service_failure.py` (C1-C6), `test_announce_wait.py` (A1-A4), `test_port_owner.py::...proc_tree`
  (P4) and `test_service_restart.py::test_a_rerun_waits_for_a_fresh_announce` (B3): every OS, in the main
  matrix's `pytest tests/frozen`. The A rows record the htcondor bindings (`FakeHTCondor`, a fake
  `classad2`), so they need no pool.
- `test_service_restart.py` B1, B1b, B2: POSIX (`announce.py` runs as a subprocess and its child is
  signalled).
- `test_port_owner.py` P1-P3: Linux (the race is replayed against `/proc`).
- `test_liveness_live.py` L1-L4: the `htcondor2` bindings and a personal HTCondor with one simulated GPU
  whose jobs share this host's `/tmp` and run as the test's user (the `test-htcondor` job's pool);
  skipped without the bindings, a failure naming what is missing when no pool answers. They run only
  where a job lists `tests/frozen/m68d`; `ci.yml`'s `test-htcondor` step does not list it yet.

## Files

| File | What it is |
|---|---|
| `m68d_harness.py` | Accessors for the planned names (looked up in test bodies, so the suite collects before the implementation exists); bounded calls (`run_bounded`, `raised_by`, `wait_for`); `Web` (a counting `LookupFreeHTTPServer`); the plan parts (`GatedGet`, `SlowToLoad`, `RaiseIn`, `StageGet`, `stop_leaf`, `gated_plan`, `stage_plan`, `node_spec`, `given_spec`); the managed child's report (`child_starts`, `end_child`); machine and job ads; announce signing (`announce_node`, `announce_later`); `AnnounceRun` (a bounded `announce.py` process); driver job files (`service_json`, `run_json`, `write_driver_job`, `driver_log`, `site_copy`); the recorded bindings (`FakeAd`, `NodeSchedd` with phased svc0 answers and a query log, `FakeCollector`, `FakeHTCondor`, `record_bindings`). Shipped to pilots as a user module. |
| `m68d_child.py` | The managed child, stdlib only (`<python> m68d_child.py <mode> <port> <report> [arg]`): each start appends `start <pid> <port>` to `<report>.starts`; `<report>.exit.<pid>` makes it exit with that code; modes `serve`, `gated` (binds once a gate file exists, retrying while the port is taken), `slow` (the first start in a cwd sleeps `arg` seconds, then binds, retrying) |

## Traceability

"Fails on main" is the first failure at executors `582d3dc` (macOS for every-OS and POSIX rows; Linux rows
and L1-L4 in `m68b-minicondor:local` with graphed `a51bee4`).

| Test | Plan clause | Fails on main | Wrong implementation caught |
|---|---|---|---|
| `test_service_failure::test_a_fixed_run_raises_at_its_first_failed_leaf[thread\|htcondor]` | C1 "stops dispatching at the first such failure" | all 8 leaves started by the raise | the root-only wait |
| `...::test_a_fixed_run_with_a_monitor_raises_well_within_the_drain_timeout[thread\|htcondor]` | C1b: raises in under `_DRAIN_TIMEOUT_S / 2`; C1's stop witness after close (no bound on leaves started by the raise: on `HTCondorBackend` a leaf's events arrive only through its `result()`, so the drain over leaves seen done starts more than 3) | thread: all 8 leaves started by close; htcondor: raised after 32.2 s | a drain counting `events_per_leaf * n`; the root-only wait |
| `...::test_a_task_whose_service_is_gone_raises_service_unreachable[thread\|htcondor]` | C2 | a raw `URLError` | no classifier; one that drops the cause or the endpoint; a re-check run on the driver rather than the worker (the htcondor pilot sees a different `host_identity`) |
| `...::test_a_plan_error_with_a_live_service_is_intact_and_was_rechecked[backend × OSError\|ConnectionRefusedError]` | C2 "told apart from a plan error" | 0 extra GETs over the twin run | a type-based classifier; a wrapper that never re-checks |
| `...::test_a_service_free_plan_submits_its_task_functions_unwrapped` | C2 engine shape | the served run submits `_leaf_task`/`_combine_task`, no `_service_checked` | wrapping every plan; no wrapper |
| `...::test_a_driverless_run_whose_given_service_dies_exits_1` | C + D6 | exit 3 with `URLError` | exit 3; rerunning a service that was not announced |
| `...::test_a_stage_task_whose_service_is_gone_raises_service_unreachable` | C over `_run_stages` | a raw `URLError` | a classifier wired into `_leaf_task` only |
| `test_service_restart::test_watch_mode_restarts_a_dead_child_on_another_port_and_reannounces` | B restart | `announce` has no `RESTARTS` | no restart; a restart reusing an announced port; no re-announce; unbounded restarts |
| `...::test_an_attached_child_that_dies_still_ends_the_job` | B scope | its watch-mode control: one start, no restart | restarting in attached mode |
| `...::test_the_driver_reruns_once_a_restarted_service_node_reannounces` | B rebind | after the kill, no second announce ("the child exited -9") | exit 3; exit 1 with no rerun; a release that pops the re-announce |
| `...::test_a_rerun_waits_for_a_fresh_announce` | B bound | exit 3 with `URLError` | rerunning against the old endpoint |
| `test_announce_wait::test_a_held_or_departed_service_node_fails_the_wait_at_once[held\|absent]` | A1 | hard timeout (20 s) in the plain wait | the plain wait; matching on `ClusterId` rather than the node name |
| `...::test_an_unschedulable_service_node_fails_the_wait_naming_its_request[above-every-slot\|fits-only-without-the-driver-claim]` | A2 "never matched" + addendum | hard timeout (20 s) | no check for DAG nodes; a match against a busy slot's free resources; a match blind to the driver job's claim |
| `...[idle-and-matchable]` | A2 | `TimeoutError` at `timeout_s=0.5` | a deadline on an idle matchable node |
| `...[running-after-idle]` | A2 | the raise came before any running answer | `timeout_s` counted from the start rather than the first `JobStatus == 2` |
| `...::test_without_schedd_access_the_wait_is_the_plain_one[no-schedd-locate\|no-job-ad]` | A3 | the control (locator and job ad present) made no node query | querying without a locator or job ad |
| `...[no-bindings\|a-raising-locate\|a-raising-query]` | A3 | no `driver.log` line naming the error | `ImportError` or a locate/query error ending the try; a silent fallback |
| `...::test_a_dag_run_json_names_its_schedd_where_jobs_can_submit` | A4 | `schedd_locate` is `None` on both DAG sites | A that never engages in a real DAG |
| `test_port_owner::test_a_port_another_process_takes_after_the_scan_is_skipped[http:/\|tcp]` | port race, `announce.py` | http: not ready (501 until `timeout_s`); tcp: ready on the foreign port | the dial-only check |
| `...::test_a_child_that_listens_through_a_shell_is_its_own_listener` | port race control | `announce` has no `held_by` (main has no ownership check to be too narrow) | ownership by the child pid alone |
| `...::test_a_driver_hosted_service_refuses_a_port_held_by_another_process` | port race, `_on_driver` | returned the foreign listener's endpoint | `_on_driver` left dial-only |
| `...::test_listener_ownership_reads_a_proc_tree[services\|announce]` | port race helpers | no `listeners` | `tcp` parsed alone; a walk stopping at the child; an any-inode `held_by`; a crash on a missing proc root |
| `test_liveness_live::test_a_killed_service_child_is_restarted_and_the_dag_completes` | B live | one child start; no restart within 60 s | no restart or no rebind on a pool |
| `...::test_a_removed_service_node_fails_each_driver_try_fast` | A + C live | one driver try, exit 3 (`URLError`), no retry | the 3 × `timeout_s` wait |
| `...::test_a_slow_binding_service_beside_the_driver_takes_another_port` | port race live | svc0 not ready: 501 from the driver's task server until `timeout_s`; DAG failed | P1d on one host |
| `...::test_an_unmatchable_service_node_fails_the_driver_fast` | A2 live | the DAG still running after 300 s | waiting out `timeout_s` on a node that can never match |

## Readings

- A1 asserts the projected query through `NodeSchedd`'s log: the constraint names the DAG and `svc0`, and
  the projection holds `JobStatus`, `HoldReasonCode` and `HoldReason`.
- A2's machine ads give the busy partitionable slot 2000 MiB free of 16000; svc0 asks for 23000
  (above every slot) or 12000 (fits the totals only if the running driver job's 8000 MiB claim is
  ignored).
- C2's htcondor leg writes the pilots' machine ad before they start and a different one for the driver
  afterwards, so `.worker` must come from the pilot.
- L3 delays the driver's first plan load by 20 s (`SlowToLoad`) so svc0 scans the driver's port first,
  and asserts the child's first start was on the driver's port. A run where svc0 starts after the driver
  binds fails on that witness instead of passing without a race.
- L1 kills the child by the pid in its report, so the pool's jobs must run as the test's user.
