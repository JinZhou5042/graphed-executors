# m68d — attempts log (executors)

Milestone **m68d**, executors unit: driverless service liveness (C stop + classify, B restart + rerun, A fail-fast,
the port race). Branch `m68d` from freeze-m68d 60288ee (main 582d3dc + the frozen suite). Plan
`lanes/htcondor/m68d/plan.md` §2/§4/§5 with `addenda.md`; constraints `ta/m68d/sanity-check/README.md`.
Venv `~/vibe-coding/cloud/.venv-m68d` (macOS py3.12, graphed a51bee4, histogram 3830aca).

## Iteration 1 — commit 1: the fixed path stops at its first failed task
- `_run_fixed` puts every leaf and combine future on a done-queue and raises through `_result` at the first
  one with an exception; the drain waits for the events of the leaves taken off the queue (all `n` once the
  root resolved).
- Frozen C1/C1b (thread, htcondor): 4 passed.
- `tests/extra/m68d/test_m68d_engine.py`: a failed combine raises while two gated leaves still hold their
  pilots. Mutants: no loop (root-only wait) and leaves-only registration both fail it (HARD TIMEOUT 50 s).

## Iteration 2 — commit 2: a task whose service fails its check raises ServiceUnreachable
- `_service_checked(checks, fn, *args)` (module level, pickled by reference): on an `Exception` it runs
  `check_ready(endpoint, check, PROBE_CHECK_S)` per service on the worker, and the first failing check
  raises `ServiceUnreachable(name, endpoint, host_identity(), f"{why}; the task raised {exc!r}")` from it;
  else the exception re-raises unchanged. `_RunTasks(backend, checks)` submits through it when `checks`
  is non-empty; `_run_scoped` builds `checks` from the endpoints bound for `plan.services`.
- Frozen C1–C6 (13): passed.
- Extras: the first failing service among several is named (mutant: first check only → fails); a
  `RunControl`-windowed run classifies too (mutant: no wrapper → fails).

## Iteration 3 — commit 3: a port counts only when the child's process tree holds its listener
- `listeners(port, proc="/proc")` (LISTEN inodes on the port from `net/tcp` + `net/tcp6`) and
  `held_by(pid, inodes, proc="/proc")` (the pid's tree, through each `stat`'s ppid, holds every inode among
  its fd links) in `submit/services.py`, copied into the stdlib-only `announce.py`. No platform gate:
  without `/proc` the set is empty and the path is today's dial.
- `announce.start`: a listener the child's tree does not hold reaps the child, logs `port P is held by
  another process, next`, and moves on. `ServiceSet._on_driver`: the same raises `ServiceUnavailable`
  with `legs["managed"] = "port P on H is held by another process"`.
- Frozen P4 (both modules) passes on macOS; P1–P4 (6) pass in `m68d-impl-pool` (Linux).
- Extras (every OS, stand-in probe answers): the driver-hosted start refuses / takes its port by
  `held_by`'s answer; announce moves past the port. Mutant (both checks removed) fails all three.

## Iteration 4 — commit 4: the SERVICE node restarts its service; the driver reruns on its re-announce
- `announce.py`: `RESTARTS = 3`; in watch mode a child that dies after it was ready is started again by
  `start(cfg, ident, skip=<ports an earlier child served>)` and announced to the current pair (`last`
  reset); past `RESTARTS` the job exits with the child's code, a failed restart with 3. Attached mode
  unchanged.
- `driver.main`: `_attempt` = the driver's `ServiceSet` (opened before `wait_for_pilots`) around one
  `runner.run`; a `ServiceUnreachable` whose `.name` is in `announce_only` logs `rerun: <error>`, closes
  the set and runs again. `_release_announced` does nothing.
- m68b extras that pinned the old behaviour: the release now keeps a pending announce (the next
  `host_service` resolves it); the refused-pair watch test sets `RESTARTS = 0`.
- Extra: a restart with no usable port exits 3 (kills a restart that reuses a served port: exit 5).
- Frozen B1/B1b/B2/B3 + C pass on macOS (17) and in `m68d-impl-pool` with P1–P4 and the m68b/m68d
  announce extras (54 passed).

## Iteration 5 — commit 5: the driver fails fast on an ended, held or unschedulable SERVICE node
- `htcondor_backend/services.py`: `ServiceJob.match_refusal`'s core lifted to `queued_refusal(what, ad,
  machines, claims)` (same text, `what` names the job); `machine_ads(locate)` takes the locate pair.
  `launch.py`: `located_schedd(htc, locate)` (shared with `CondorPilots._choose`), `CLAIM_ATTRS`.
- `backend.py`: `_Node` (key, dir, `ad()`) feeds m69b's `_await_announce`: its ad query is
  `DAGManJobId == <own> && DAGNodeName == "<key>"`; on the first idle ad it matches the whole ad against
  the collector's slots with the DAG's running jobs, plus the runner's running condor pilots, as claims.
  `_host_announced` holds queued pilots as `_host_service` does; an import/locate/query failure logs one
  line and falls back to `wait_announce(node, timeout_s)`. `driverless.py` writes `schedd_locate` for a
  DAG run; `driver._runner` passes it and `dag_dir` to the backend.
- Frozen A1–A3 + all m68d pass on macOS (31, 8 Linux/pool skips) and in `m68d-impl-pool` (39).
- Extras: running pilots counted + queued pilots held then released (recorded bindings); nothing held
  before the pilots are submitted; no Machine ads → plain wait; live pool: a rerun holds the queued
  condor pilot (pilots.log 012 with graphed's reason, then 013) and the DAG completes with the twin's
  value. The pilot hold was unmeasured before this leg: it passes, and with the hold removed from
  `_host_announced` both the recorded-bindings and the live test fail.

## Iteration 6 — commit 6: docs and the test-htcondor paths
- `htcondor.rst` (re-check on a failed task; the port ownership rule beside the driver and in a service
  job; restart + rerun and the queue-ad wait in a driverless run; the held-node cost), `design.rst`,
  changelog. `ci.yml`'s `test-htcondor` pytest line gains `tests/frozen/m68d tests/extra/m68d`.
- Not done: `engine.py` in `test-htcondor`'s diff-cover. Frozen
  `m66::test_coveragerc_htcondor_gates_exactly_the_backend` forbids it in `.coveragerc-htcondor`'s source
  (tried: that test fails), and without it the include gates nothing; the main matrix's diff-cover
  gates `engine.py` (addendum).
- Gates: macOS frozen + extra 1205 passed / 140 skipped (the one failure was the rcfile try, reverted);
  per-file >= 90 % and diff-cover 100 % (engine, services) on that data; `m68d-impl-pool` test-htcondor
  set 594 passed / 10 skipped, 99 % total, per-file gate ok, diff-cover 99 %.

## Iteration 7 — commit 7: the ownership scan follows the dial
- Frozen P2 (`test_a_driver_hosted_service_refuses_a_port_held_by_another_process`) failed once in the
  pool's coverage run: the scan ran before the foreign server bound, then the dial reached that server.
  Both members (`announce.start`, `_on_driver`) read the listeners after each dial (addendum).
- Extras: a listener that appears during the first dial is refused (driver) / passed over (announce);
  both fail on the scan-first order and pass after.

## Iteration 8 — gates at 411ddcf (no code change)
- macOS: frozen + extra 1208 passed / 140 skipped (frozen m68d 31 passed, 8 Linux/pool skips); per-file
  >= 90 % (15 files); diff-cover vs 582d3dc 100 % (engine, submit/services); precommit ok (ruff, format,
  mypy, coverage 97 %, sphinx -W); mypy --strict and --platform win32 clean.
- `m68d-impl-pool`: ci.yml's test-htcondor set 596 passed / 10 skipped (frozen m68d 39/39); 99 % total,
  per-file gate ok, diff-cover 99 % (one line: `queued_refusal`'s empty-machines return).
- Fork CI on 411ddcf: run 37151741521 success; test-htcondor ran the m68d paths (598 passed, 8 skipped).

## Iteration 9 — review r1 repairs (F1–F6, S1, S2)
- F1: `_run_fixed` drains nothing on the raise path (addendum "C1, the raise-path drain"); the reviewer's
  drain tests: `[htcondor]` 30.3 s and the service-gone test 30.0 s at 8ced49b, both pass now.
- F2: htcondor.rst drops the `__cause__` claim. F3–F6: the reviewer's tests, verbatim but for docstrings.
- S1: `machine_ads` asks the collector for `MyType == "Machine" && SlotType =!= "Dynamic"`, no projection,
  no client filter; `queued_refusal`'s empty-machines guard goes (both callers guard). Real `classad2`:
  Partitionable/Static/no-SlotType kept, Dynamic dropped; a `!=` mutant fails the no-SlotType case.
- S2: a node with no queue ad reads `history(<node>, …, match=1)` and the error names `LastHoldReason`,
  else `RemoveReason`; empty/raising history logs one line and keeps "no job ad". Live leg: driver and
  svc0 held (via `CondorReason`: htcondor2 drops a plain `str` reason) before either starts, svc0 removed
  by its `periodic_remove`, the driver released: the error names the hold (fails at 8ced49b: "no job ad").
- Gates at 2976dbf: macOS 1221 passed / 145 skipped (frozen m68d 31 + 8 guarded skips); per-file >= 90 %;
  diff-cover 100 %; precommit ok; mypy strict + win32 clean. `m68d-impl-pool` test-htcondor set 614
  passed / 10 skipped (frozen m68d 39/39), per-file ok, diff-cover 100 %.

## Iteration 10 — review r2 (R2-F1)
- design.rst and dask.rst: a completed run waits for its trailing events; a fixed-tree run that raises
  releases the monitor topic at once. `git grep -n "once trailing events drain" -- docs/` is empty
  (control `git grep -c "Monitoring rides" -- docs/design.rst` = 1); sphinx -W and precommit ok.

## Iteration 11 — review r3 (R3-F1, R3-F2)
- R3-F2: `_run_fixed_windowed` (a fixed tree with a `RunControl`) drains nothing on a raise and every
  submitted leaf on a return, as `_run_fixed`; the reviewer's `test_m68d_windowed_drain.py[htcondor-True]`
  raised after 30.3 s at d78d051 and passes now (the three other cases pass at both). design.rst and
  dask.rst scope the monitor sentence to fixed-tree runs, with or without a `RunControl`.
- R3-F1: design.rst says the port-ownership rule holds on Linux (`/proc`); elsewhere the check decides.
- Gates at bad8047: macOS 1225 passed / 145 skipped (frozen m68d 31 + 8 guarded skips); per-file >= 90 %;
  diff-cover 100 %; precommit ok (sphinx -W); mypy strict + win32 clean.

## Iteration 12 — PR #49: test-dask diff-cover
- test-dask's diff-cover (its own coverage, `.coveragerc-dask`) missed `_service_checked`, `listeners`/
  `held_by` and `_on_driver`'s refusal: no m68d test was on its pytest line. ci.yml adds
  `tests/frozen/m68d tests/extra/m68d` there. That job's steps, reproduced (aarch64 Linux, a51bee4 wheel,
  distributed 2026.8.0): 487 passed / 14 skipped, per-file gate ok, diff-cover vs 582d3dc 54 % before
  (macOS) and 100 % after (macOS and Linux).
