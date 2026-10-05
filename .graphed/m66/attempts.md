# m66 implementer — attempts log

Milestone **m66**: direct HTCondor backend (`graphed_executors.htcondor_backend`), branch `lane/htcondor`,
frozen suite `freeze-m66` @ 8dd983b (50 tests in `tests/frozen/m66/`). Plan: `lanes/htcondor/plan.md`.

## Iteration 1 — commit 1, the CI leg
- `test-htcondor` job (ubuntu-latest, py3.12): get.htcondor.org minicondor under sudo, `systemctl enable --now condor`
  falling back to `condor_master`, a bounded wait for a schedd and a startd, then `pytest tests/frozen/m66` as the
  runner user (non-root) with bare `--cov --cov-config=.coveragerc-htcondor`, combine, per-file + diff gates.
  `ci-required` needs it. `[htcondor]` extra `htcondor>=25.13`; main omit and main diff-cover exclude gain
  `*/htcondor_backend/*`.
- Expected on this commit: the m66 tests (except the 5 packaging pins) fail at the accessor with
  `ModuleNotFoundError: graphed_executors.htcondor_backend`, in the main matrix and in `test-htcondor`. The job
  still reports the installer/pool-start steps on a real runner, which is what this commit measures.
- Local: `pytest tests/frozen/m66/test_htcondor_packaging_pins.py` → 5 passed.
- CI measured on 5cfc5b8 (run 36103619305, test-htcondor): the get.htcondor.org installer itself runs
  `systemctl enable condor` + `systemctl start condor` on the runner; `condor_version` 25.14.1, one partitionable
  slot (4 CPUs, 15989 MB) Unclaimed within ~25 s of the install; both live-pool tests pass `require_pool()` and
  fail at the accessor, like the rest of m66, as expected.

## Iteration 2 — commit 2, site profiles + launcher
- `sites.py` (SiteProfile, SITES, schedd_weight, choose_schedd, counts_as_alive), `launch.py` (`_htcondor`,
  PilotLauncher, LocalPilots, CondorPilots: submit_description, refusals before `_htcondor()`, secret file 0600
  as hex text, `pilot.sh`, `env.tgz`, collector failover node by node + `Collector.locate`, spool + retrieve +
  remove on `(schedd name, ClusterId)`).
- `tests/extra/m66/test_m66_launcher.py`: collector failover and the no-collector error on a fake bindings
  namespace; the default `log_dir` is a fresh dir under the sandbox root. The test-htcondor pytest command now
  includes `tests/extra/m66` (the test-parsl precedent runs its extra dir in the scoped job).
- Local: test_htcondor_sites.py + packaging pins + extra → 26 passed. The rest of m66 still fails at the
  accessor for the server/backend names (commit 3).

## Iteration 3 — commit 3, task server + pilot + backend
- `server.py`: TaskServer (HMAC hex signature checked before `pickle.loads` on every route; `/hello`, `/beat`,
  `/next` long poll, `/result`), the dep rule (a task queues when its `_ParslFuture` args are done; a failed or
  cancelled dep fails it with that exception), first lease only → `set_running_or_notify_cancel`, requeue once
  then `WorkerLost(key, pilot)`, no-pilots-left failure once the launcher reports 0 alive, `settle` outside the
  Condition's lock, close → notify_all → 410. `allow_reuse_address` is False on Windows only (the r3 premise
  is Windows' SO_REUSEADDR; on POSIX it only skips TIME_WAIT, which back-to-back servers in the port range need).
- `pilot.py`: hello, daemon beat thread, pull loop; 410 → exit 0; 403 → prints "wrong secret file", exit 2;
  driver unreachable for a lease → exit 1; an unpicklable exception comes back as a RuntimeError with the traceback.
- `backend.py`: HTCondorBackend (all-False caps, `n_workers()` = live pilots, `wait_for_pilots`, `describe_failure`,
  ExitStack so a refused `launcher.start` releases the port), `_require_importable` (pickle + a `find_class` probe
  refusing `__main__`), HTCondorRunner (min_pilots=1 wait before the first run), `htcondor_runner`.
- `CLOSE_WAIT_S = 2 * server.POLL_S` now lives in launch.py, its only reader.
- `tests/extra/m66/test_m66_server.py`: cancelled queued task + idle poll, stale/unusable result + close, port
  range exhausted, pilot exit 1 after its driver vanishes. Discrimination: with the cancelled-task drop removed,
  the cancel witness fails; with collector failover replaced by a raise, both failover witnesses fail.
- Local (macOS, py3.12): `pytest tests/frozen/m66 tests/extra/m66` → 56 passed, 1 skipped (live pool: no bindings).

## Iteration 4 — LPC site check, first attempt → fix
- Site check (driver in coffea-almalinux9-noml:2026.9.0-py3.12 with bootstrap.sh's binds, non-editable venv of
  88d3972, htcondor 25.13.2): `schedd.spool` failed — "reading from file <cwd>/pilot.sh: No such file or directory".
  HTCondor resolves a relative `executable` against the submitter's cwd, not `initialdir` (condor_submit manual,
  `executable`), so the generic (non-spooled) live-pool test would hold its pilots the same way. The schedd removed
  the half-staged cluster 30427220 itself (history: JobStatus 3, "Staging of job files failed"); 0 jobs left.
- Fix: `start` submits `executable=<log_dir>/pilot.sh`; `submit_description` keeps `pilot.sh`, as frozen.
- Site check, second attempt on f245a83 (transcript `lanes/htcondor/probes/site-check-lpc/transcript.txt`): schedd
  lpcschedd5.fnal.gov (≈12.7k idle jobs), ClusterId 30427221, submit 3.7 s, first pilot live 33.3 s, second 116.3 s;
  `concat_plan(16)` bit-for-bit vs SequentialRunner (357 B); tasks ran on cmswn2276 and cmswn2179 with
  `sys.prefix=/srv/env` (the shipped venv relocated into the job scratch); close 14.7 s; 0 jobs left (history: both
  ExitCode 0); `pilot.{0,1}.{out,err}` + `pilots.log` retrieved; scratch dir deleted.

## Iteration 5 — CI on 88d3972 (run 36104880243)
- test-htcondor: 56 passed, 2 failed — both live-pool tests, on the relative-executable bug fixed in f245a83
  (generic: pilots never started → 240 s bound; spooled: `spoolJobFiles` could not read `<cwd>/pilot.sh`).
- test-parsl py3.12 + 3.14t: frozen m47 `test_ci_parsl_step_runs_m47_with_a_bare_cov_and_no_path_valued_cov`
  failed. Its regex runs from the parsl pytest line over every indented line to EOF, and the new job's comment
  quoted a path-valued `--cov=`. Fix: the comment no longer spells it. Not a dispute: the frozen test reads ci.yml
  as intended; the comment was the defect.

## Iteration 6 — commit 4, docs
- `docs/htcondor.rst` (how-to: laptop run with LocalPilots, four warnings, install, LPC walk-through with the
  bootstrap.sh bind set and the non-editable venv, lxplus marked not yet run, generic pool, SiteProfile, argument
  table, failure modes, who can talk to the task server), design.rst "On an HTCondor pool", README/index install
  line + runner row, api.rst autosummary, changelog "Unreleased", improvements.rst + design.rst "Not supported yet"
  keep SLURM/TaskVine only.
- Executed: the laptop example (prints `[700] 7 6` after the two pilot lines), the `__main__` refusal (the quoted
  ValueError), the SiteProfile example (templates render). The LPC recipe ran as the site check. `sphinx-build -W`
  clean.

## Iteration 7 — CI on 4a8b295 (run 36105612341)
- test-htcondor: 58 passed (both live-pool tests green on the personal pool); total 97%, but the per-file gate failed
  pilot.py at 84.15%: its lost-driver exit ended in `os._exit`, which never saves subprocess coverage, and no test
  drove an unpicklable task error.
- Fix: `post` raises ConnectionError after a lease without the driver; the beat thread stops on it and the main
  thread prints it and returns 1 (a pilot mid-task now exits once that task ends, documented). New witness: a task
  raising an unpicklable exception settles its future with a RuntimeError carrying the traceback. Local pilot.py 98%.
- windows-latest py3.13: frozen m37 `test_errored_task_emits_errored_and_propagates[process-ProcessExecutor]`
  (0 ERRORED events seen). ProcessExecutor in `graphed_executors.local`, untouched here; the other 15 matrix legs pass.
  Treated as a flake; the next run is the check.

## Iteration 8 — implementation review r1 (H1, M1, L1)
- Coverage correction for iterations 5–7: the figures there are frozen+extra. On frozen hits alone the diff is
  94.9% (542/571) and pilot.py 83–87% (review-measured); the CI gate is unchanged pending the owner's decision.
- H1: the heartbeat or main thread that finds the driver gone for a lease sends its own process SIGTERM, so a
  pilot mid-task stops at once and coverage (`sigterm = true`) still saves. Witness
  `test_a_pilot_whose_driver_is_gone_exits_at_once[idle|mid-task]`: a 1 s lease, a 10 s task, exit asserted by
  SIGTERM within 9 s of the server going away.
- M1: the pilot id is `hostname:pid:token` (8 hex of uuid4). Witness
  `test_pilots_sharing_a_hostname_and_pid_stay_distinct`: two pilots patched to `pilot-box`/pid 14 count as 2,
  and a task that kills its first pilot is re-run on the survivor.
- L1: the signature header is compared as latin-1 bytes, so a malformed one is a plain 403. Witness
  `test_a_malformed_signature_header_is_refused` (`\xe9` → 403, no traceback).
- Discrimination: with 89a2eac's pilot.py + server.py swapped in, all four witnesses fail; with this change they pass.
- Docs: htcondor.rst says the pilot stops at once, even mid-task; the id is `host:pid:token`; `retries` has no effect
  (the engine forwards it to `HTCondorBackend.submit`, which ignores it). The N_WORKERS_WAIT_S comment drops its figure.
- CI on 9efabb5 (run 36117544357): test-htcondor 62 passed, per-file pilot 98.8 / server 98.6 / launch 99.0 / backend
  95.9, diff 99%. Three matrix legs failed the malformed-header witness: it closed after `recv(64)` while the server
  was still writing the 403, so the server logged a reset. The test now reads to EOF; it still fails on 89a2eac.

## Iteration 9 — lxplus site check: the driver port range is site data
- Measured on lxplus (probes/site-check-lxplus/pilot-logs-run2.txt): batch nodes get `Connection refused` on the
  driver's 10000–10100 range on the login node; with the driver on 8786 (transcript-8786.txt) the site check passes,
  first pilot live after 105.5 s. 8786 is the port CERN opens from workers to a submit host (dask scheduler), one per node.
- `SiteProfile.driver_ports` (default 10000–10100, set explicitly in every SITES entry; lxplus = 8786–8786).
  `HTCondorBackend(port_range=None)` and `htcondor_runner(port_range=None)` take the launcher's profile range
  (`generic` for a launcher without a profile). A range with no free port raises
  `OSError: no free port for the task server: site=<name> ports=<lo>-<hi>` from the server's OSError.
- Witnesses (tests/extra/m66/test_m66_ports.py): the lxplus profile binds 8786; `htcondor_runner(site="lxplus")` binds
  8786 unless `port_range=` is given; a second lxplus backend fails naming `site=lxplus ports=8786-8786`.
  Discrimination: with the source change stashed, three fail `AttributeError: 'SiteProfile' object has no attribute
  'driver_ports'` and the fourth `DID NOT RAISE OSError`.
- Local: frozen+extra m66 64 passed / 1 skipped (live pool); per-file backend 94%, sites 100% (launch 81% locally —
  the bindings-only paths, 99% on the CI pool job); precommit gate ok; sphinx -W clean.
- Docs: the lxplus note now states the measured facts and the transcript paths; `port_range` documented.
