# m67 frozen suite — driverless runs on the HTCondor backend

This suite freezes the acceptance tests for driverless runs: `submit_driverless` ships a pickled
runtime `Plan` and `run.json` in ONE condor job, whose driver
(`python -m graphed_executors.htcondor_backend.driver <dir>`) runs `HTCondorRunner` over `LocalPilots`
in a fat slot or over pilots it submits itself, and returns `result.pkl`; a `RunHandle` tracks and
collects the job. The source of truth is `lanes/htcondor/plan-services.md` (§2 is the m67 unit, D4–D6
the binding decisions). **Frozen — read-only after the m67 freeze tag.**

Run it with `pytest tests/frozen/m67`. Everything except `test_driverless_live.py` runs on every OS
without the bindings: `launch._htcondor` is replaced by a recorder. The live file needs the
`htcondor2` bindings (Linux wheels only) and a personal HTCondor; the `test-htcondor` CI job provides
both.

## Files

| File | Plan item | What it shows | A wrong implementation it fails |
|---|---|---|---|
| `driverless_harness.py` | §2 harness | The m66 pieces the suite uses (bounded runs, concat and `StageError` plans, a shippable fake venv), `JobLeaf` plans that record (pid, job `ClusterId`, pilot url) per leaf, `RecordingSchedd`/`FakeHTCondor`/`record_bindings`, `PilotSchedd` (its `submit` starts the pilots as local subprocesses), `job_dir` (a job scratch dir from `transfer_input_files` alone) | — |
| `data/driver-ads.json` | §2 `test_run_handle` fixture | Synthetic job ads in the shapes of the status lines of `probes/services-lpc/p1-dag/transcript-p1c-spool-tif.txt`, each with its expected status | — |
| `test_driverless_payload.py` | §2 files (`submit_driverless`, `sites.py`), D4, D5, D6 | ONE submit whose description has `driver.sh`, `plan.pkl`+`run.json` (+`env.tgz` when shipped) in, `result.pkl,driver.log` out, `request_cpus == n_pilots` (local) or 1 (condor), `max_retries 2`, `retry_until 3`, `graphed-driverless-` batch, site keys then `extra_submit`; lxplus spools; a fresh interpreter in the job dir (no `PYTHONPATH`) unpickles `plan.pkl` and matches `SequentialRunner` bit-for-bit with the user module imported from the job dir; `run.json.schedd_locate == [COLLECTOR_HOST, chosen schedd]` for condor pilots; refusals (lambda, `__main__`, condor on a profile without `worker_ports`, lpc `log_dir` outside the sandbox, non-AFS lxplus condor `log_dir`) with zero bindings calls; the D4 port rows and `service_hosts` | an unshipped `run.json` or user module, a retried plan error (no `retry_until`), a wrong fat slot, outputs that never return, late/absent/over-broad refusals, a `schedd_locate` the job cannot use, drifted site data |
| `test_driver_entry.py` | §2 `driver.py`, D6 exit codes | The entry run from a job dir built from a recorded submit: local pilots on ≥2 pids other than the driver's, task server on a `worker_ports` port (lxplus: not the login `driver_ports` 8786), `result.pkl == (True, ExecResult)` bit-for-bit, `driver.log`; a plan error exits 3 with the intact `StageError`; pilots that cannot start and a missing `run.json` exit 1 with `(False, exc)` and `driver.log`; condor pilots (in-process, `PilotSchedd`) reach the schedd only via `Collector(pool).locate(Schedd, name)` and dial the `Machine` of `$_CONDOR_MACHINE_AD` | a driver computing on its own pid, a swallowed or stringified exception, a plan error exiting 1, a pilot failure exiting 3, an uncaught traceback, a hang, the task server on `driver_ports`, a pilot url not on the machine ad's host, `Schedd()` in the job path |
| `test_run_handle.py` | §2 `RunHandle` | `status()` per ad (1 queued, 2 running, 5+16 queued, 5+13 held, 3 removed, 4+0 done, 4+1/4+3 failed) from one projected query, history once the job left the queue; `wait` polls to a terminal state and raises `TimeoutError` at its bound; `result()` refuses before done even with a stale `result.pkl`, retrieves a spooled sandbox, re-raises a pickled `StageError`; `remove`; `save`/`load` through JSON; every schedd located by name | a spooling hold reported held, done-whatever-the-exit-code, no history read, a stale result, no retrieve, a lost exception, an unbounded wait, `Schedd()` |
| `test_driverless_live.py` | §2 live (a)–(d) | (a) fat slot on the generic profile: bit-for-bit, leaves on ≥2 pids inside the driver job's cluster, `driver.log` in `log_dir`, queue empty, history `ExitCode 0`; (c) that row has `JobMaxRetries == 2` and `OnExitRemove` ending retries on `ExitCode =?= 3`; (b) condor pilots: leaves ran in a cluster other than the driver's, ≥3 history rows; (d) a plan error: `failed`, `StageError` re-raised, `ExitCode 3` after one start | a driver that never runs pilots, a result that never returns, a driver job that cannot self-submit, a retried plan error, a lost exception |

Plan §2 "Fails on", each caught by (sanity mutants in `lanes/driverless/ta/sanity/`):
a driver computing on its own pid → `test_local_pilots_run_the_plan_inside_the_slot`; a lost exception →
`test_a_plan_error_exits_3_with_the_intact_stage_error`, `test_result_re_raises_the_pickled_exception_intact`;
a hang → `test_wait_is_bounded` (and every bounded run); `Schedd()` in the job path →
`test_condor_pilots_reach_the_schedd_by_location_only`; a retried plan error →
`test_local_description_carries_the_driverless_keys` (`retry_until`), the exit-3 test; an unshipped
`run.json` → `test_local_description_carries_the_driverless_keys` and every driver test.

The live file is skipped with its reason where `htcondor2` is not installed. Where it is installed,
each test first requires a schedd in the collector within 30 s and fails with the reason when there
is none.

## The contract these tests pin

Beyond the names in plan §2, the tests rely on these readings of it:

- Every bindings call of `submit_driverless`, `RunHandle` and the driver's condor path goes through
  the module-level `launch._htcondor()` (looked up at call time). The driver reads `Machine` from the
  classad text file `$_CONDOR_MACHINE_AD` names without the bindings.
- `submit_driverless` writes its submit-side files into `log_dir`; `transfer_input_files` names them
  relative to `initialdir` (or absolute). It accepts a site name; `SITES` is the dict looked up, so a
  test registers a profile with `monkeypatch.setitem`. `env` is the venv to ship, as for `CondorPilots`.
- A `SiteProfile` built from the six m66 kwargs has `worker_ports = service_ports = None`,
  `service_hosts == ()`; `service_hosts` holds `"driver"` iff `service_ports` is
  set and `"cluster"` iff `worker_ports` is set, in that order.
- Refusal messages name their subject: `user_modules` (import), `worker_ports`, `3DayLifetime` (lpc
  sandbox), `/afs` (lxplus self-submission). Only `pilots="condor"` needs `worker_ports`.
- The driver runs in its cwd `<dir>`, writes `result.pkl` and `driver.log` there on every exit, and
  its local pilots run `sys.executable` (the bogus-python test sets it before the driver imports).
  Waiting for pilots happens before `runner.run`, so a pilot failure exits 1. `main(argv)` returns
  the exit code or raises `SystemExit`, and runs off the main thread.
- `RunHandle(site=, schedd=, cluster=, log_dir=, submitted_at=)`; a status query's constraint names
  the cluster and projects at least `JobStatus`, `HoldReasonCode`, `ExitCode`; an empty queue answer
  falls back to `schedd.history`; `wait(timeout=, poll_s=)` returns on done/failed/removed and raises
  `TimeoutError`; `result()` returns the `ExecResult`, and its refusal before done names the status.
- The "poisoned plan" of the driver test is the m42 user-code `StageError` plan, not
  `PoisonUriProcess`: a killed pilot is detected only after `LEASE_S` (30 s) per loss, which a
  subprocess driver cannot shorten.
