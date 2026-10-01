# m67 implementer — attempts log

Milestone **m67**: driverless runs on the HTCondor backend, branch `m67` on `lane/htcondor` 613aaa7, frozen suite
`freeze-m67` @ 713a624 (`tests/frozen/m67/`). Plan: `lanes/htcondor/plan-services.md` §2, D4–D6.
Baseline at 613aaa7 (`pytest tests/frozen tests/extra`, macOS py3.12): 711 passed, 11 skipped, 0 failed.

## Iteration 1 — commit 1, driver entry + schedd_locate + D4 site rows
- `sites.py`: `service_ports`/`worker_ports` after `driver_ports` (default `None`), `service_hosts` derived
  (`"driver"` iff service_ports, `"cluster"` iff worker_ports); D4 rows lpc (10000,10100)/(10001,10100), lxplus
  (10000,10100)/None, generic both (10000,10100). No `services` field (m68).
- `launch.py`: `CondorPilots(schedd_locate=(pool, name))` → `_choose` returns
  `Schedd(Collector(pool).locate(Schedd, name))`; `submit_description(url, n, base)` lets a caller replace base
  keys before the site keys and `extra_submit`; `_stage` (job script + env.tgz) and `_submit` (submit + spool)
  split out of `start` for reuse; `collectors(htc, param)` shared. The default-schedd name is now `SCHEDD_HOST`
  else the local schedd ad's `Name` (was `socket.getfqdn()`): a later session locates the schedd by that name.
- `driver.py`: `main([dir])` reads `run.json` + `plan.pkl`, builds `LocalPilots(python=sys.executable,
  pythonpath=[dir])` on loopback (`worker_ports`, else an ephemeral port) or `CondorPilots(schedd_locate=…)`
  dialled at `Machine` of `$_CONDOR_MACHINE_AD` on `worker_ports`; waits for `min_pilots` before `run`;
  writes `result.pkl` `(ok, ExecResult | exception)` and `driver.log` on every exit; 0 / 3 (raised in
  `run`) / 1 (anything else). An unpicklable outcome comes back as a `RuntimeError` naming it.
- `tests/extra/m67/test_m67_driver.py`: host fallback, unpicklable outcome, default-schedd name, service_hosts.
- Expected on this commit alone: the m67 frozen files fail at `submit_driverless` (commit 2), per the plan's order.
- Gates at 6efd4a4: `pytest tests/frozen tests/extra` → the 34 non-live m67 frozen tests fail at the missing
  `submit_driverless`/`RunHandle` accessor (commit 2); zero other failures (baseline 711 passed). precommit `--fast
  --no-coverage` ok. `driver.py` per-file coverage needs the frozen driver tests, so it is measured at commit 2.

## Iteration 2 — commit 2, submit_driverless + RunHandle
- `driverless.py`: `submit_driverless` refuses (pilots mode, un-importable process/combine/empty/next_tasks/stop,
  sandbox/image/env via `CondorPilots._refuse`, `pilots="condor"` without `worker_ports`, lxplus condor `log_dir`
  outside `/afs`) before any bindings call; then writes `plan.pkl`, `driver.sh` (+`env.tgz`), `run.json` and submits
  ONE job: base keys via `submit_description(..., base)` with `executable=<abs driver.sh>`, `arguments=.`,
  `transfer_output_files=result.pkl,driver.log`, `max_retries=2`, `retry_until=3`, `graphed-driverless-<nonce>`,
  then site keys, then `extra_submit`. `run.json.log_dir = <log_dir>/pilots` (inner pilots' submit dir),
  `schedd_locate = [COLLECTOR_HOST, chosen schedd]` for condor pilots.
- `RunHandle`: every call locates the schedd by name through the site's collectors (`Collector()` where the site
  has no `schedd_query`); status from one projected query, history when the job left the queue; `wait` bounded;
  `result` refuses unless done/failed, retrieves a spooled job still in the queue, re-raises `(False, exc)`;
  `remove`, `logs`, `save`/`load`.
- Every bindings call is `launch._htcondor()` looked up per call (a `from .launch import _htcondor` escaped the
  frozen recorder; fixed before any gate).
- `tests/extra/m67/test_m67_driverless.py`: collector failover + none-found error, forgotten job, JobStatus 6 and
  4-without-ExitCode, unbounded wait, `logs()`, unknown pilots mode. Discrimination: one-edit mutants (failover
  catch narrowed, Machine parse bypassed, default schedd name hard-coded, unpicklable fallback narrowed) each fail
  their extra test.
- Gates at commit 2: `pytest tests/frozen tests/extra` → 758 passed (711 baseline + 35 m67 frozen + 12 extra),
  0 failed; the live m67 module skips without bindings. Per-file coverage (m66+m67 frozen + extra, subprocesses
  measured with an absolute `COVERAGE_FILE`): driver.py 100%, driverless.py 100%, launch.py 95%, sites.py 100%,
  every file ≥ 90% (`scripts/coverage_gate.py`). Frozen m67 alone: driver.py 93%, driverless.py 93%.

## Iteration 3 — commit 3, CI leg + docs
- `test-htcondor` runs `tests/frozen/m67 tests/extra/m67` beside m66, with `COVERAGE_FILE` absolute: a driver
  subprocess runs in a job scratch dir, and a relative data file landed its coverage there (driver.py read 80%
  locally before, 93% frozen-only after). The main matrix already runs the non-live m67 files on every OS.
- Docs: `htcondor.rst` "Running without a login session", `design.rst` "Driverless" paragraph, changelog entry.
  Sphinx `-W` clean locally; the example calls bind against the real signatures. The live legs (a)–(d) and the
  lxplus/LPC site checks are unmeasured here (no bindings on macOS, Docker broken): CI's test-htcondor measures (a)–(d).

## Iteration 4 — rebase onto main + review/site-check folds
- Rebased onto origin/main eefc0f3 (#34 squash of m66; its tree equals 613aaa7's, so no conflicts); frozen commit
  8cfb843, `freeze-m67` moved there with its message. `git diff 713a624 8cfb843 -- tests/frozen/m67` and
  `git diff origin/main HEAD -- tests/frozen/m66` are empty.
- db3971e `RunHandle._poll` passes `match=1` to `schedd.history` (lxplus bigbird26 scans its whole history without it).
- 74fee50 driver: a `StageError` whose `cause_type` is `KilledWorker` exits 1 (owner D6 ruling: lost workers are
  retried), any other run error 3; a failing `runner.close()` is logged and keeps the outcome; `driver.log` names the
  pilot cluster under `pilots="condor"`; the job file names moved to `launch`, so the package no longer imports
  `driver` and `python -W error -m …driver <dir>` prints nothing to stderr.
- 0d29aa1 `SiteProfile.jobs_can_submit` (default True, False for lpc); `submit_driverless` refuses
  `pilots="condor"` there before any bindings call.
- fe64ffa docs: exit-code table, `remove()` after `result()` on spooled sites, `wait(timeout=None)` under a hold.
- Discrimination (one-edit reverts, each fails its new test): no `match`; `where=""` in the log line; exit code
  back to `EXIT_PLAN_ERROR`; close handler narrowed to `ZeroDivisionError` (both params fail); driverless importing
  `driver` again (stderr carries the runpy RuntimeWarning); refusal disabled; lpc datum dropped.
- Gates: `pytest tests/frozen tests/extra` 765 passed, 11 skipped, 0 failed. test-htcondor replica (m66+m67 frozen +
  extra, 118 passed, subprocesses combined): driver.py 100%, driverless.py 100%, sites.py 100%, launch.py 95%,
  backend.py 94%, every file >= 90%; diff-cover vs origin/main on htcondor_backend 100% (234 lines, 0 missing).
  Sphinx -W clean; mypy --strict clean; precommit gate ok.

## Iteration 5 — impl-r2 Low folds
- cfcf9f6 L2: `HTCondorRunner.wait_for_pilots()` waits for `min_pilots` and marks the runner waited; the driver calls
  it, so `run()` no longer waits a second time. Pilots lost after the driver's wait fail the run's tasks as
  `KilledWorker` (exit 1, retried) instead of timing out a second wait inside `run()` (exit 3). New extra test
  `test_pilots_lost_after_the_driver_waits_exit_1` kills every local pilot after the first wait returns. One-edit
  revert (driver back to `runner.backend.wait_for_pilots(...)`, scratch clone): `assert 3 == 1`; fix leg passes.
- cfcf9f6 L1: the extra driver test's module docstring lists the tested paths once.
- 3e5eb28 L3: `design.rst` exit-code paragraph and `htcondor.rst` held-wait paragraph reflowed to width 100.
- Gates: `pytest tests/frozen tests/extra` 766 passed, 11 skipped. test-htcondor replica 119 passed; per file:
  `__init__`/driver/driverless/pilot/sites 100%, backend 95%, launch 95%, server 99%; coverage_gate ok.
  diff-cover vs origin/main eefc0f3: 239 lines, 0 missing, 100%. Sphinx -W, mypy, precommit ok.

## lxplus submit fixes (branch fix/htcondor-lxplus-submit, base db8fb0a)
- D2 4da13a3: `launch.ensure_credential` stores the Kerberos credential before a `SendCredential` submit (a credd
  without one left the job looping ReconnectFailed with NumJobStarts 0 on bigbird26) and names the store command
  when storing fails. D1 93554d3 and D3 76e672b are m68b's (see its attempts.md).
- Owner-sanctioned refreeze ee1a297: the m67 harness fake gains `Credd`; tag freeze-m67-fixup2.
- lxplus evidence: lanes/htcondor/probes/site-lxplus/m68b-dag.txt attempts 1-3; D2 probes in
  probes/site-lxplus/submit-fixes/.
- Gates at 76e672b: precommit ok; macOS frozen+extra m66/m67/m68b 244 passed, 6 skipped; container m66/m67/m68a/m68b
  441 collected, 3 skipped, 0 failed; per-file >= 97.92%; diff-cover 100% (17 lines).
