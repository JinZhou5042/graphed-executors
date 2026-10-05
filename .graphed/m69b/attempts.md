# m69b — attempts log (executors)

Milestone **m69b**, executors unit: `service_hosts` placement, schedulability (engine driver-memory check, condor
match + start-based timeout), the H→γγ diagnostics on histserv servers, `run_lpc.py`, ci, docs. Branch `m69b` at
7afad04 (freeze-m69b ed08da6 + upstream/main 8c299f7). Plan `plan-services.md` §5.2 (+ D2, D8, §6–§8);
constraints `reviews/m69b-exit-items.md`. Venvs: `.venv-m69b` (macOS py3.12; graphed a0638719, histogram 3830aca,
histserv 0.2.1), `.venv-m69b-hgg` (+ coffea fork b2612ab, uproot ca3a8a2, higgs_dna d179305 --no-deps).

## Baseline (7afad04, macOS)
- frozen m69b + m68b: 8 fail — m69b narrowing refusal, the five driver-memory rows, m68b `idle`/`spooling`
  (timeout from first `JobStatus 2`). Live pool rows skip (no bindings); hgg rows skip (no coffea).

## Iteration 1 — commit 1: `service_hosts` narrowing
- `HTCondorBackend(service_hosts=)` / `htcondor_runner(service_hosts=)`: subset of the row's hosts (attached: the
  profile's; in a driver job: `("driver",)`), kept in the row's order; a host not offered → `ValueError` naming
  the offered tuple, before `TaskServer`. `host_service` attaches iff `"cluster"` survives the narrowing.
- `tests/extra/m69b/test_m69b_placement.py` (8): fail 8/8 on the unchanged src; a mutant that accepts and ignores
  the kwarg fails 5/8.
- Frozen narrowing refusal passes.

## Iteration 2 — commit 2: schedulability
- Engine (`submit/services.py`): a driver-hosted managed server must fit `backend.driver_memory_mb`, else the
  host's physical memory (`os.sysconf` pages; Windows `GlobalMemoryStatusEx`), summed over the set's
  driver-hosted servers; a misfit goes to `host_service` with `detail` naming both numbers, or is refused
  (`legs["managed"]`) when there is none. A backend's refusal merges with the earlier legs.
- Condor: one collector query before submit; after submit the job ad `symmetricMatch`es slot copies with the
  partitionable `TotalSlot*` as `Memory/Cpus/GPUs`; no match → removed unrun, `ServiceUnavailable` naming the
  requests and the largest slot memory. `_await_announce`: idle/spooling has no deadline, logs key + state every
  `IDLE_LOG_S`; `timeout_s` counts from the first `JobStatus == 2`. A driver job's `driver_memory_mb` = slot
  `Memory` from `$_CONDOR_MACHINE_AD`.
- Finding: `ctypes.c_ulong` is 8 bytes on LP64, so the Windows struct uses `c_uint32` DWORDs (64 bytes on
  every OS; the extra test pins `dwLength == 64`).
- Mutants: driver check always fits → 2 extra + 6 frozen managed fail; deadline from submit → 2 extra +
  m68b `idle`/`spooling` fail. Restored tree: 85 passed (extra m69b, frozen managed, m68b job tests).
- Linux pool (`m69b-impl-pool`, CI test-htcondor equivalent): 481 passed, 3 skipped, 1 failed —
  `tests/extra/m68a/test_m68a_services.py::test_a_child_that_exits_after_its_check_passed` (a 1 s window for a
  child to exit; passes 3/3 alone, with and without coverage). Per-file ≥98% (submit/services.py 99.15%);
  diff-cover 100% (100 lines).

## Merge — upstream/main b0dfd2e (#42 m68c, #43 atomic write_secret)
- `git merge upstream/main` at 4759ba8: no conflicts (4ecd444). ci.yml GRAPHED arrived at 7e048bf; lane venvs and
  the pool container upgraded to graphed 7e048bf.
- Linux pool after the merge (+ frozen/extra m68c): see Iteration 4. macOS: frozen m68a/m68c/m69b + extra
  m68c/m69b 191 passed, 16 skipped (pool and hgg rows).

## Iteration 3 — commit 3: H→γγ diagnostics
- `analysis.DIAGNOSTICS` (seven, Weight, weight=`weight`), `diagnostics(record, context)` (gh.boost, or
  histserv on a context), `HggReduce`/`HggCombine`/`HggEmpty` composing `Counters`/`accumulate` with
  `pieces.reduce`/`combine`/`empty`; `dataset_plan`/`plan(..., context=None)` → `pieces.serve(aggregate_plan(...))`.
  `run_local.report` prints the counters and saves the diagnostics as UHI JSON.
- test-hgg equivalent (`.venv-m69b-hgg`, graphed 7e048bf, `GRAPHED_HGG_REQUIRED=1`): frozen m69a + m69b
  `test_hgg_*` + extra m69b: 57 passed, 0 skipped.
- Mutants (frozen diagnostics + extra hgg): combine drops b's histograms → no-context row + empty-identity fail
  (served rows pass: every chunk's receipt names the same server histogram); reduce without
  `resolve_services` → all three served/no-context rows fail; empty without `"diagnostics"` → empty-identity fails.

## Iteration 4 — commit 4: LPC runner, ci, docs
- `examples/hgg/run_lpc.py`: manifests → each dataset's first `--files` files in `--parts` steps (entry counts via
  `run_local.fileset`); `htcondor_runner(site="lpc", service_hosts=(--placement,), user_modules=[analysis.py])`, or
  `--driverless` via `submit_driverless(pilots="local", request_memory_mb=driver + pilots + every server)`;
  `Context(memory_mb=--server-mb, workers=--pilots, ports=lpc service_ports)`; parts to EOS by default; UHI JSON.
  Not run at a site. `tests/extra/m69b/test_m69b_run_lpc.py` (runner stand-ins): mutants placement ignored /
  servers left out of the slot / one file too many each fail.
- ci.yml (§6): HISTOGRAM = `graphed-histogram[histserv]` @ 3830aca (test-experimental strips the extra: no cp314t
  grpcio); test-htcondor adds frozen m69b `test_histserv_*.py` + extra m69b; test-hgg runs `-rs` m69a + m69b
  `test_hgg_*` + extra m69b. GRAPHED stays 7e048bf (from the merge).
- Docs: htcondor.rst timeout wording, "Schedulability", "Histograms on histserv servers", "An H→γγ run",
  `service_hosts` row; hgg.rst "Diagnostics"; README; changelog. The histserv example ran (printed output
  matches); the hgg.rst fragment ran on the MC fixture; sphinx -W clean.
- Gates at the head: macOS main job 1136 passed, 116 skipped (bindings/coffea/dask/parsl/Triton only), per-file
  min 98.33% (submit/threadpool.py), diff-cover 100% (44 lines). Linux pool (merge state; src unchanged since):
  503 passed, 5 skipped (dask/parsl absent, no Triton container), htcondor scope min 98% (backend.py), diff-cover
  100% (98 lines); extra m69b in the container 25 passed, 2 skipped (no coffea). test-hgg equivalent 59 passed.

## Iteration 5 — PR #44 round 2: the test-dask diff-cover
- CI (d3f2820): test-dask py3.12/3.14 diff-cover 65.9% on `submit/services.py` (missing 176,178-179,191-195,466-469,
  483-484,499): the job runs no m69b file. Reproduced verbatim (job's pytest list + `.coveragerc-dask` + its
  diff-cover include) in `.venv-m69b-dask` (the job's installs): 384 passed, same 15 lines, 65%.
- `tests/extra/m69b/test_m69b_dask_driver_check.py` (dask runner, LocalCluster 2×1): a 64 MiB service under
  `driver_memory_mb=64` starts beside the driver and every task dials it from a dask worker; 1024 MiB is refused
  before it starts naming 1024 and 64; with no limit, physical+1 is refused naming the physical size. The job's
  list adds it and `test_m69b_schedulable.py` (the Windows branch, the fall to a cluster host, the merged refusal).
  Same command: 404 passed, diff-cover 100% (44 lines). Include globs untouched.
- test-parsl: its include (`parsl_backend/**`, `common/relay_engine.py`, `common/http_plane.py`) meets none of the
  PR's changed src files (`htcondor_backend/{backend,services}.py`, `submit/services.py`): no exposure.

## Iteration 6 — PR #44 round 2: run_lpc parts from pilot scratch
- Site run (driver, `lanes/htcondor/probes/site-lpc/m69-hgg*.txt`): the EOS `--out` failed the first pilot write
  (`ArrowInvalid: Unrecognized filesystem type in URI: root://…`; graphed's `_ArrowParquet` hands the path to
  `pq.write_table`); a local `--out` passed and stayed in pilot scratch. Plan §9's fallback:
  `--out` defaults to `output_inclusive` in each pilot's scratch; pilots carry `transfer_output_files=<out>` and
  `output_destination=<--destination>` (default `root://cmseos.fnal.gov//store/user/<user>/hgg/`) through
  `extra_submit`; a `://` `--out` is refused via `parser.error` naming pyarrow. Driverless returns `<out>` beside
  `result.pkl,driver.log` instead (an `output_destination` on the driver job would take `result.pkl` too).
- Tests: the attached and driverless submit keys, the refusal before any runner. Mutants (no
  `output_destination`; driverless takes the pilot keys; root `--out` accepted) each fail one test.

## Iteration 7 — review r1 (REJECT) round: M1, M2, m1, m2
- Hygiene: a zsh `rm .coverage*` glob had deleted the three `.coveragerc-*` files (restored by the coordinator);
  coverage data is cleaned with `.coverage .coverage.*` only.
- M1: `HTCondorRunner.close()` (and `HTCondorBackend.close()`) call `backend.stop_waiting()` first; a service wait
  still idle or spooling (no deadline yet) raises `RuntimeError` naming the key at its next poll (≤ `POLL_S`), its
  ExitStack removes the job and forgets the key. A started job keeps its `timeout_s`; the no-deadline wait stays
  (owner ruling). Docs: the false "never left waiting" sentence rewritten (waits while the scheduler offers no
  room, own pilots included; key logged every 30 s; `close()` ends it).
  Test `test_close_ends_a_wait_for_a_slot_removing_the_job` (stand-in pool, every job idle): fails on d536f04
  ("close() still waits for the slot"), fails with the runner's `stop_waiting()` call removed. Reviewer's
  self-starve probe in the pool: close returned (~13 s after the 90 s watch), run raised naming the key, queue 0.
- M2: `_as_whole` also takes `TotalSlotDisk` → `Disk`. Dict row fails on d536f04 (`Disk 524288 != 2097152`);
  classad2 row `test_a_job_waiting_only_on_a_busy_node_s_disk_matches` passes in the pool; the reviewer's disk
  probe now prints "match as shipped: True".
- m1: `run_lpc` logs through `ServerTimes`, which appends each status's `started_at`/`ready_at`. Row fails on
  d536f04 (the stand-in status line carries no times).
- m2: the empty-ads guard moves into `match_refusal`; row `test_a_pool_whose_collector_lists_no_slot_submits_and_waits`
  (collector lists nothing; a stand-in classad2) reaches the announce; with the guard deleted it fails with
  `ValueError: max() iterable argument is empty`.
- Gates: macOS main job 1138 passed / 120 skipped, per-file min 98.33 %, diff-cover 100 %; test-dask job 406
  passed, diff-cover 100 %; pool test-htcondor job 506 passed / 10 skipped, per-file min 99.79 %, diff-cover 100 %
  (109 lines); sphinx -W, ruff, mypy src+tests clean.

## Iteration 8 — 40470e6, 84e6fb7: run_lpc writes its parts to EOS again; CI's graphed at a51bee4
- 40470e6 reverts d536f04's output route (parts left in pilot scratch and shipped by `output_destination`, a
  `root://` `--out` refused): no LPC slot offers the root file-transfer plugin. Each job again writes its parts to
  the `root://` `--out` through graphed's parquet writer; df4d059's `ServerTimes` status lines stay. The
  root-`--out` refusal row left with the revert (review r2: test-hgg 62 passed).
- 84e6fb7 moves CI's `GRAPHED` from 7e048bf to a51bee4, whose writers open fsspec URLs, as that default `--out`
  needs; the docs put fsspec-xrootd beside the analysis. Until this pin, 40470e6's "now opens root://" did not hold
  at CI's graphed (review r2 exit item 5).

## Iteration 9 — §5.2 "Ordering", commit 2: a run's service jobs go before its pilots
- `CondorPilots.prepare(url, secret)` is `start` without the pilots' submit, once; `start` = `prepare` + submit;
  `stop()` waits for no pilot when none was submitted. `HTCondorBackend` over `CondorPilots` with `host_service`
  (the `"cluster"` hosts, or `announced`) prepares when built and submits its pilots at the first `n_workers()`,
  `submit()` or `wait_for_pilots()`; the first `n_workers()`/`submit()` waits once for `min_pilots`
  (`HTCondorRunner` sets it; 1 bare), an explicit `wait_for_pilots(n >= min_pilots)` counting as that wait.
  `HTCondorRunner.run` no longer waits. `driver.py`'s start-up line says the pilots go at the first need.
- Extra: `test_m66_ports.py` intercepts `prepare` (the url is now first seen there).
- Frozen recorder rows 2–4 pass; rows 1 and 5 wait on commit 3.

## Iteration 10 — commit 3: close() finishes a waiting plan; later plans hold the pilots; act reasons
- Probe (pool, htcondor2 25.13.2; 25.14.1's `Schedd.act` source is the same): `act(..., reason="text")` leaves
  `HoldReason` `''` (only a `(text, code)` tuple is applied), and the schedd appends `" (by user <name>)"`. So
  `CondorReason(text)` (a `(text, None)` tuple whose `str()` is the text) is every `act` reason (the pilots' and a
  driverless run's removal too), the release matches `substr(HoldReason, 0, len) == "<reason>"`, and `alive()` a
  `HoldReason` starting with it. A plain-`str` hold or an exact `HoldReason ==` release would leave a later
  plan's pilots held for good.
- Later plans: once the pilots are submitted, `_host_service` holds their idle jobs once (`JobStatus == 1`,
  graphed's reason) before its submit, releases at the backend's next need of a worker, and matches each slot
  less the running pilots' claims (`<r>Provisioned`, else `Request<r>` evaluated in the ad: the pool's pilot ads
  carried no `*Provisioned` and `RequestDisk` is an expression), the refusal naming the pilots' cluster.
- close(): `HTCondorRunner.close()` drains in `try`/`finally` without `stop_waiting()`; `__exit__` with an
  exception calls `stop_waiting()` first. `HTCondorBackend` records each `ServiceJob` before its submit (under a
  lock, refused once `_closing` is set), and `close()` removes every recorded job in its own thread before the
  server and pilots. `ServiceJob.stop()`/`submit()` share a lock; a stopped job is never submitted. A wait with
  no deadline checks `_closing` before the ad, so a job close() removed reports the close.
- Extra (`test_m69b_schedulable.py`): df4d059's close row removed (the frozen close row supersedes it).
  New rows, each failing its mutant (control passes): backend `close()` removing a waiting job before it returns
  (`backend-close-no-remove`: the Remove comes at the waiter's poll); two threads' `stop()` on a removal held on
  an Event (unserialized, early-return flag); a stopped job never submitted (`_stopped` check gone); no submit
  after `stop_waiting()` (`_closing` check gone); `alive()` (held not counted / any hold counted); a claim's
  share (parent slot not found / `Request` before `Provisioned`); a real schedd releasing graphed's hold and not
  the user's (pool: `str` reason, exact `HoldReason ==`, a release of any hold).
- CI: `test-htcondor`'s pytest line gains `tests/frozen/m69b/test_service_order.py` (the five pool ids ran nowhere).
- Gates (the tip): macOS main job 1148 passed / 126 skipped, per-file min 93.02 % (`local/shuffle.py`),
  diff-cover 100 % (44 lines). Pool `test-htcondor` line 522 passed / 10 skipped, htcondor scope per-file min
  98.69 % (`backend.py`), diff-cover 100 % (239 lines), queue empty. test-dask 411 passed, diff-cover 100 % (44).
  test-hgg 67 passed / 5 skipped. ruff, ruff format, mypy --strict (also win32), sphinx -W clean. The ten
  ordering ids: identical outcomes over two runs (macOS 5 pass / 5 skip; pool 10 pass).

## Iteration 11 — review r3 (O1, O2): a need moves the pilots only while no server waits to announce
- Gate (`backend.py`): `host_service` (`_host_service`, or a driver job's `_host_announced`) is wrapped by
  `_announcing`, which counts a server waiting to announce for the call's span. `_need()` (every
  `n_workers()`/`submit()`/`wait_for_pilots()` poll) moves the pilots (release a hold, or the deferred submit)
  only while none waits, else records the need; the last server to return or raise moves them (a failure
  logged; the next need retries). `wait_for_pilots`' `timeout` counts from the later of the call and the
  pilots' submit (none while the submit is put off). Frozen row 1 (one Hold, one Release per later plan)
  passes with the gate.
- `driver.main` waits for its pilots inside its `ServiceSet`, before `runner.run` (and outside its `try`), so
  a driver job's pilots follow its SERVICE nodes' announces and pilots that never start exit 1, services or
  none.
- `ServiceJob.stop()` (Windows CI at 67ba8f2, `PermissionError(13)`): the secret-file unlink ran outside the
  lock, so the second of two concurrent stops raced the first's unlink, which Windows refuses while the
  delete is pending. The first stop now removes and unlinks under the lock; a later one returns at once.
- Rows, each failing its mutant (controls pass): O2 `test_m69b_announce_gate.py` over the frozen
  `OrderSchedd`: no Release while plan B's server waits beside plan A's task, one after B's announce
  (`release-on-any-need`, `last-server-does-not-move`); O1 a DAG driver job's pilots submitted after its
  SERVICE node's announce (`driver-waits-before-services`, 67ba8f2's order); a driver job whose pilots never
  start exits 1 (`wait-inside-the-run-try`: exit 3; the frozen m67 bogus-python row fails at construction,
  so it does not reach the wait). `test_m69b_schedulable.py`: a need beside a waiting server times its wait
  from the pilots' submit (`deadline-from-the-call`); `wait_for_pilots(0)` then `n_workers()` with
  `min_pilots=1` (`any-wait-is-the-first-need`, `no-wait-is-the-first-need`); the close row asserts
  "when the runner closed" with the removed job gone from the queue (`closing-after-ad-check`); the stop row
  asserts a later stop repeats nothing (`unlink-every-stop`, `unlink-outside-the-lock`).
- Docs: htcondor.rst "Schedulability" says the runner's queued pilots never take a waiting server's room,
  and states the need rule, the driver job's order, and the wait counted from the submit.
- Gates (78df2e9's src and tests): macOS main job 1154 passed / 126 skipped, per-file min 98.33 %
  (`submit/threadpool.py`), diff-cover 100 % (44 lines). Pool `test-htcondor` line 528 passed / 10 skipped,
  htcondor scope per-file min 98.80 % (`server.py`), diff-cover 100 % (264 lines), queue empty. test-dask 413
  passed / 2 skipped, per-file gate ok, diff-cover 100 % (44). test-hgg 73 passed / 5 skipped. ruff, ruff
  format, mypy --strict (also win32), sphinx -W clean. The 18 ordering ids (frozen `test_service_order.py`,
  the gate file, the four schedulable rows): identical outcomes over two runs (macOS 13 pass / 5 skip; pool
  18 pass).

## Iteration 12 — review r4 (G1, G2, T1): the gate reads a plan's service start and the run's end
- Cause (owner ruling, plan 3ddd3a4): 78df2e9 counted `host_service` calls in flight, a stand-in for two states.
  A need between two services of one `ServiceSet` released the plan's hold (two Holds, two Releases in one set),
  and a server ending because the run closed submitted the deferred pilots after `close()`.
- `ServiceSet.start` resolves inside the backend's optional `starting_services()` context (`nullcontext`
  otherwise), its worker `_probe` outside it. `HTCondorBackend.starting_services` counts the phases; a need during
  one is recorded and the last phase to end moves the pilots. `_announcing`/`_serving` are gone. `_move_pilots`
  submits no pilot once `_closing` is set (`stop_waiting()` or `close()`), and still releases. `wait_for_pilots`
  raises naming the close when the run is closing and no pilot was submitted, or once `close()` ran.
- Rows (each kills its mutant; controls pass): the O2 row now drives plan B's real `ServiceSet` (pilots answer its
  probe); G2: a need between a set's two services leaves one Hold and one Release, the Release after the last
  announce (`phase-per-resolve`, 0008028's src); G1, `stop_waiting`/`close`: a need beside a first plan's starting
  service submits no pilot and ends naming the close (`submit-while-closing`, `wait-ignores-the-close`, 0008028's
  src); a later plan's held pilots are still released when the run ends (`no-release-while-closing`,
  `last-phase-does-not-move`); `close()` alone ends a wait on submitted pilots (`wait-ends-only-unsubmitted`,
  `wait-ends-at-any-close`); T1 both sides of the `max` (`deadline-from-the-call`, `deadline-from-the-submit`);
  `no-phase` and `release-on-any-need` (O2); `probe-inside-the-phase` hangs the O1 row. r3's rows' mutants still die.
- r4's probes re-run: `probe_close_need_exec_rv4` submits no pilot in the four runner cases (the result form
  leaves the block at 3.00 s, as its control); its direct case calls a bare `host_service`, not a plan's service
  start, so the need submits at once and `close()` removes the cluster and ends the need. `probe_set_gap_exec_rv4`:
  inside the set 1 Hold, 1 Release, the Release after service 2's announce.
- Correction to iteration 11: its macOS per-file minimum was 93.02 % (`local/shuffle.py`), not 98.33 %.
- Gates (3a977ac): macOS main job 1160 passed / 126 skipped, per-file min 93.02 % (`local/shuffle.py`),
  diff-cover 100 % (47 lines). Pool `test-htcondor` line 534 passed / 10 skipped, htcondor scope per-file min
  98.80 % (`server.py`), diff-cover 100 % (271 lines), queue empty. test-dask 418 passed / 2 skipped, per-file min
  91.70 % (`dask_backend/transport_shuffle.py`), diff-cover 100 % (47). test-hgg 79 passed / 5 skipped. ruff, ruff
  format, mypy --strict (also win32), sphinx -W clean. The 24 ordering ids: identical over two runs (macOS 19
  pass / 5 skip; pool 24 pass).

## Iteration 13 — review r5 (H1): close() waits for a pilot submit in flight
- `_move_pilots` read `_closing` under `_pilots_lock`, but `close()` set it under `_lock` alone and stopped the
  launcher while `cluster` was still None; a submit already running finished afterwards and left its cluster
  queued (present since 67ba8f2's deferred submit). `close()` now takes `with self._pilots_lock, self._lock:`, the
  rule `ServiceJob._lock` keeps. Lock order: no path holds `_lock` while taking `_pilots_lock` (`_host_service`
  takes them in separate blocks; nothing under `_pilots_lock` reaches `_lock`). `stop_waiting()` takes no lock: a
  submit in flight when it runs is removed by the later `close()`. htcondor.rst "Closing" is true as written.
- Row `test_close_waits_for_a_pilot_submit_in_flight_and_removes_its_cluster`: the pilots' submit held on an
  Event, `close()` on another thread stays blocked until it is let go, then returns with that cluster's Remove
  acted (`close-without-the-pilots-lock`, 6877abc's `close()`, killed; control passes). The other 21 mutants
  still die. `probe_start_close_exec_rv5`: both cases act `Remove ClusterId == 7001` before `close()` returns and
  leave nothing queued (the 20 s before it is `CLOSE_WAIT_S`).
- Gates (df176a2): macOS main job 1161 passed / 126 skipped, per-file min 93.02 % (`local/shuffle.py`),
  diff-cover 100 % (47 lines). Pool `test-htcondor` line 535 passed / 10 skipped, htcondor scope per-file min
  98.80 % (`server.py`), diff-cover 100 % (271 lines), queue empty. test-dask 419 passed / 2 skipped, per-file min
  91.70 % (`dask_backend/transport_shuffle.py`), diff-cover 100 % (47). test-hgg 80 passed / 5 skipped. ruff, ruff
  format, mypy --strict (also win32), sphinx -W clean. The 25 ordering ids: identical over two runs (macOS 20
  pass / 5 skip; pool 25 pass).

## Iteration 14 — review r6 (D1, D2; D3 awaits the owner)
- D1: the last service-start phase to end moved the pilots only when a need was recorded; a set that failed has
  no worker probe after it, so its hold stayed until some later need. `starting_services`' end now also acts
  when `_held` (`_held` implies submitted, so it only releases). Plan ada3a49: held pilots "are released once
  those services have started or failed"; htcondor.rst says the same. Frozen row 1 keeps one Hold and one
  Release, the Release after the last announce.
- D2: `ServiceSet._managed` fell through from a driver refusal to any callable `host_service`; a driver job's
  (`_host_announced`) hosts only its SERVICE nodes, so a too-big image-less spec raised `ValueError`. It now
  falls through only when `"cluster"` is among the hosts, and otherwise refuses with the sizes.
- Rows (each kills its mutant; controls pass): a later plan's set that fails (the job leaves the queue
  unannounced) acts Hold then Release with no need made (`wanted-only-phase-end`, 245d0c6's condition); a driver
  job with a SERVICE node refuses a 200 MiB spec beside its 100 MiB slot with `ServiceUnavailable` naming both
  (`too-big-falls-through-to-any-host-service`, raises `ValueError`). The 22 earlier mutants still die.
- Gates (2c77051): macOS main job 1163 passed / 126 skipped, per-file min 93.02 % (`local/shuffle.py`),
  diff-cover 100 % (47 lines). Pool `test-htcondor` line 537 passed / 10 skipped, htcondor scope per-file min
  98.80 % (`server.py`), diff-cover 100 % (271 lines), queue empty. test-dask 421 passed / 2 skipped, per-file min
  91.70 % (`dask_backend/transport_shuffle.py`), diff-cover 100 % (47). test-hgg 82 passed / 5 skipped. ruff, ruff
  format, mypy --strict (also win32), sphinx -W clean. The 27 ordering ids: identical over two runs (macOS 22
  pass / 5 skip; pool 27 pass).

## Iteration 15 — D3 (owner-approved; plan e546dfe §5.2): a set's earlier servers hold their slots
- A set whose servers each fit a slot but not together waited forever for the later one: the match subtracted
  only a later plan's running pilots. `_host_service` now also passes the running ads of the set's earlier
  service jobs (`_services` keys starting `f"{scope}-"`, via `running_claims(jobs)`), in every plan; other
  plans' servers stay out. `match_refusal` takes claims by holder, names each one that holds something (pilots
  and servers, by cluster), and no longer asserts the pilots' cluster. htcondor.rst's slot-match bullet carries
  the claims sentence, with the set's servers.
- The precommit integrity scan flags the removed assert unless the same file adds one; `match_refusal` now
  asserts its precondition `self.cluster is not None` (its only caller submits first).
- Rows. Recorder (Linux, `classad2`): a first plan's web2 is refused beside its running web1 on a 1000 MiB slot,
  naming web1's cluster, at 400 MiB, not naming another plan's running server. Pool: 2 × 3/8 of
  `TotalSlotMemory` both serve the run; 2 × 9/16 refuse web2 naming web1's cluster, with web2 unrun
  (`NumJobStarts 0`) and the queue empty. Killed in the pool container (controls pass): `state-5ba868f`
  (recorder: "ended before it announced"; pool over leg: 240 s timeout), `sibling-claims-only-once-submitted`,
  `assert-in-refusal`, `every-plan-s-servers` (recorder), `slots-as-advertised` (fit leg: web1 counted twice).
- Gates (1b3ba0f): macOS main job 1163 passed / 129 skipped, per-file min 93.02 % (`local/shuffle.py`),
  diff-cover 100 % (47 lines). Pool `test-htcondor` line 540 passed / 10 skipped (a first run, with the
  startd started before the GPU config, failed the two m68b GPU rows; rerun after its restart), htcondor scope
  per-file min 98.80 % (`server.py`), diff-cover 100 % (282 lines), queue empty, container removed. test-dask 421
  passed / 3 skipped, per-file min 91.70 % (`dask_backend/transport_shuffle.py`), diff-cover 100 % (47). test-hgg
  82 passed / 8 skipped. ruff, ruff format, mypy --strict (also win32), sphinx -W clean. The 30 ordering ids:
  identical over two runs (macOS 22 pass / 8 skip; pool 30 pass).

## Iteration 16 — review r7 (H1): a backend starts one service set at a time, through its probe
- H1: two plans of one runner starting their sets together each admitted a second server beside the other's first
  and waited for room the other kept. r7's cut (an `HTCondorRunner` runs one plan at a time, plan 71a5deb) fails
  frozen m68b `test_a_cluster_hosted_http_server_serves_its_run_and_leaves_with_it` on the pool (B1's overlap leg,
  350.5 s, "the direct run's end removed the submitted run's service"): dispute 776b171, withdrawn here once plan
  ba30c9c replaced the sentence.
- `HTCondorBackend.starting_service_set()` holds a lock across `ServiceSet.start`'s resolve phase and probe (duck-typed
  as `starting_services`); a set waiting its turn polls the lock every `POLL_S` and raises once `_closing` is set
  (`stop_waiting`, `close`), whatever the holder waits for. htcondor.rst: the "once no other plan's services are
  starting" clause is gone, a bullet says sets start one at a time.
- Rows. Recorder (all OS), `[stop_waiting]`/`[close]`: plan a's started server never announces (600 s timeout), plan
  b waits its turn; after the end b raises naming the close within 5 s, a ends at close, only a's job was submitted
  and it was removed. Pool: plans x and y from two threads, two servers of 3/8 of `TotalSlotMemory` each, both
  return their values (56 s) and the queue is empty. Killed (controls pass): `bare-set-lock` and
  `set-wait-ends-only-on-close` ([stop_waiting]), `no-set-lock` (both legs: b submits beside a), on the pool
  `state-7e6e9c5` and `probe-outside-the-set-lock` (r7's narrower cut): both hang to the 240 s bound. The 24 earlier
  mutants still die (PHASE re-anchored to the new indentation).
- Gates (3cdff78): macOS main job 1165 passed / 130 skipped, per-file min 93.02 % (`local/shuffle.py`), diff-cover
  100 % (48 lines). Pool `test-htcondor` line 543 passed / 10 skipped (B1's row passes), htcondor scope per-file min
  98.80 % (`server.py`), diff-cover 100 % (292 lines), queue empty, container removed. test-dask 423 passed / 3
  skipped, per-file min 91.70 % (`dask_backend/transport_shuffle.py`), diff-cover 100 % (48). test-hgg 84 passed / 9
  skipped. ruff, ruff format, mypy --strict (also win32), sphinx -W clean. The 33 ordering ids: identical over two
  runs (macOS 24 pass / 9 skip; pool 33 pass).
