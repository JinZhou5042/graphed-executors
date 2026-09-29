# m68a — attempts log

Milestone **m68a**: the engine service set (three legs, readiness checks, worker probe, recipes), driver-hosted
services, the site table's `services`, driverless endpoints. Branch `m68a` from `main` c2298d7 (m67 and m69a merged).
Plan: `plan/plan-services.md` §1, D1–D10, §3.1, §6–§9 on `lgray/graphed-executors` `plan/m68` eafd92f; implementer
constraints `plan/reviews/m68a-exit-items-r8-r10.md` (and r7 E1–E7).

## Decisions (coordinator)
- The brief says `git fetch origin plan/m68`; this clone had no `origin`, and `plan/m68` exists only on the fork, so `origin` = `https://github.com/lgray/graphed-executors` and `upstream` = graphed-org (the brief's diff-cover base).
- GRAPHED pin: graphed-org/graphed `cf4520d` (#61's merge; its parent is the current pin 49454d4, #58 is below both), the smallest move that carries `ServiceSpec`/`split_endpoint`/`bind_services`/`require_bound`.
- **Blocker, recorded, not routed around:** the plan's graphed "resolve walk" (§3.2: `graphed.services.resolve_services`, `Resolvable`, forwarding through `_Collated`/`_PartitionReduce`) is on no graphed branch (main 6e9e55e, all heads, no `freeze-preserve-m68-3`). graphed changes are out of this brief's scope. The engine calls `graphed.services.resolve_services` when it exists and otherwise resolves through the duck-typed hook on `plan.process` (the same call for a non-composite process). The frozen legs that need graphed's forwarding (the `collate` and `aggregate_plan` spies) are written to the plan and stay red until graphed lands the walk. See `blockers.md`.
- Commit author Lindsey Gray, no trailers: the brief's Rules (the owner's instruction) govern attribution.
- Local gates run on CPython 3.12 (the `test-htcondor`/`docs` interpreter); the all-OS/all-version matrix is CI's.
- Live legs (minicondor + Triton container): `get.htcondor.org` and `nvcr.io` are refused by this session's egress policy (403). A `htcondor/mini` container pool was tried: the condor CLI submits, but the pip `htcondor2` bindings fail FS authentication against it. The `test-htcondor` leg is therefore gated by the PR's CI, as the brief allows.
- Pre-existing, not m68a: under `pytest -n 8`, `tests/extra/m66/test_m66_ports.py::test_a_bind_failure_names_the_site_and_the_range` and `test_m66_server.py::...exits_at_once[mid-task]` fail on port contention; both pass serially (CI runs serially). Local gates run serially.

## Freeze
- Test author wrote `tests/frozen/m68a/` (144 tests). Sanity r1 was NOT SANE, with three required fixes: README contract readings, bounding every planned-API call, and a probe fault that discriminates a connect-only probe. Optional A/B/C/E were also applied. Sanity r2 was SANE: 130 failed, 12 passed and 2 skipped on the unimplemented tree, identical across two runs, with 0 collection errors; ruff, format and mypy clean.
- Deliberately not applied (sanity r2 optional nits, non-blocking): three `ServiceSet(...)` constructors sit outside `run_bounded`, but the plan's constructor only runs `split_endpoint`; and the README has no line saying the bare-endpoint refusal is graphed's `split_endpoint` text.
- The test author validated attainability against a throwaway prototype outside `src/`. It was deleted before the implementer started, and the implementer never saw it.
- **Freeze sha: `83ca76fbd0b39ecedbd651584f9573d059de71bd`** (`test(m68a): frozen m68a acceptance suite`). The annotated tag `freeze-m68a` exists locally at that commit. The session's git proxy drops every push that carries the tag (`send-pack: unexpected disconnect`, 5 tries with backoff), while the same commit pushed as a branch. Per the brief, the sha is recorded here. Integrity check for reviewers: `git diff 83ca76f -- tests/frozen` must be empty.

## Implementer iteration 1
- Changed: `submit/services.py` (ServiceSet, three legs, check_ready, probe, host_identity, releases), `submit/recipes.py`,
  engine (`run` opens one ExitStack per submission: ServiceSet then plan-task cancel; bind before the first submit,
  resolve while up; `_RunTasks` holds only not-done futures), `ThreadBackend.advertise_host`, `services=` on
  `dask_runner`/`parsl_runner`/`htcondor_runner`/`HTCondorRunner`, `require_bound` in `_BaseExecutor.run`,
  `transport_run_plan`, `parsl_run_plan`; htcondor sites/backend/launch/driverless/driver; ci.yml, `.coveragerc-htcondor`,
  docs; `tests/extra/m68a/`; `tests/extra/m67/test_m67_driver.py` updated for E1 (`machine_host` -> `services.host_identity`),
  E12 (the "did not round-trip through pickle" wording) and the driver's service phase (a real `Plan` in the failing-close fake).
- Ambiguities resolved (one line each):
  - r23 `_RunLeaves` does not fit as-is: its `_done` calls `fut.cancelled()`, which `SubmitFuture`/`_ParslFuture` lack; the engine has `_RunTasks` in the same idiom (add under the lock, callback outside, cancel snapshot a list under the lock; done futures filtered, since a done future may not have called back yet).
  - One probe task per set covers every spec's check; its answer wait is the largest `timeout_s` of the set; a leg-1/leg-2 check is one attempt bounded by min(timeout_s, 30 s).
  - Statuses are logged at resolution (once per spec per start), before the probe; `closed_at` is stamped on `statuses()` at close.
  - `ServiceUnreachable.worker` is "" when no worker answered.
  - `recipes.http_server(root=)`: `root`, unless ".", is the recipe's staged `inputs`; argv unchanged (the suite pins argv).
  - In-job `HTCondorBackend`: `service_ports` = row's `worker_ports` for self-submitted pilots (CondorPilots), else None (E8); `host_identity()` = the module function (E9: exposed, not absent).
  - `HTCondorBackend.close` = its kept stack: server.close, launcher.stop, (server.close again, a no-op), shutdown; each step logged, never raised.
  - `test-dask` gates `submit/` (per-file 90%, diff 98% over `submit/**`), so it now also runs `tests/extra/m68a` and installs `grpcio-health-checking`; `test-htcondor` timeout 30 -> 45 min for the Triton image pull (not a quality gate).
  - driver.main adds a `graphed_executors` log handler to `driver.log` (service statuses and failed releases land there).
- Commits: 4e572a4 feat(services) (+1305/-21), b16842d feat(htcondor) (+371/-93), 60a3ead ci+docs(m68a) (+162/-14).
- Gates (CPython 3.12, serial):
  - main `pytest tests/frozen tests/extra --cov`: 1216 passed, 9 failed, 37 skipped (parsl/coffea/perspective absent; the two
    Triton legs without GRAPHED_TRITON_GRPC). Failed = expected-red: the two resolve-walk legs; the pool legs of m66/m67/m68a
    live files (no pool here); and `tests/frozen/m66/test_htcondor_packaging_pins.py::test_coveragerc_htcondor_gates_exactly_the_backend`
    (disputed, see disputes/). Per-file min 96.86% (local/executors.py); diff-cover vs upstream/main 100% (404 lines).
  - htcondor-scoped (.coveragerc-htcondor, subprocess coverage, no pool): 288 passed, 9 failed (same set), 2 skipped;
    per-file min 96%; diff-cover (htcondor_backend + submit/services.py) 100% (473 lines).
  - dask-scoped (.coveragerc-dask, ci.yml's list + m68a): all passed; submit/ per-file >= 98.3%; diff-cover 100%.
  - precommit `PRECOMMIT-GATE: ok` before each commit; sphinx -W ok; `git diff 83ca76f -- tests/frozen` empty.
- Dispute filed: m66 `.coveragerc-htcondor` exact-source pin vs plan §6 / the m68a packaging pin.
- Not verifiable here: the live pool and Triton legs (egress), the parsl job (parsl not installed), non-Linux legs of the all-OS job.

## Implementer iteration 2 (review r1 REJECT: findings 3-11)
- 3/4: blockers.md records the release gate (no release before the graphed floor carries services + the resolve walk;
  floor unchanged) and the follow-up (delete `_resolved_value`, call `graphed.services.resolve_services` directly);
  the changelog says graphed.services is unreleased.
- 5: `recipes.http_server(root=)`: the default argv is the plan's exactly; `root != "."` appends `--directory root`
  (and stays the staged input). Ambiguity: the plan's argv has no directory and the frozen suite does not pin `root`;
  the previous reading (inputs only) served the driver's cwd, so a driver-hosted `root` had no effect. Extra test
  serves a file from `root`.
- 6: `_probe` docstring, the module docstring and htcondor.rst say one probe task per set, answers awaited up to the
  largest `timeout_s` among the set's services, at most max(2, n_workers()) tasks.
- 7: leg-1/leg-2 checks use the spec's own `timeout_s` (the 30 s cap is gone; no frozen test needs one).
- 8: driver.main's log handler setup is in try/finally: the handler is removed and closed and the package level
  restored (extra test).
- 9: one log-never-raise helper, `submit.services.release_quietly`; `launch.quietly` removed, htcondor imports it.
- 10: the managed readiness loop has its own `_READY_CHECK_S`.
- 11: `RunHandle.result()` reads `result.pkl` outside the load `try`: a missing file raises as itself (extra test).

## Implementer iteration 3 (review r2: APPROVE-conditional, three items)
- 1: `SiteProfile.__reduce__` rebuilds through the constructor with plain dicts (graphed `Launch`'s idiom), so every
  row pickles, deep-copies and replaces with `services` still a mappingproxy (extra test over every SITES row + a
  synthetic one). `dataclasses.asdict` still fails on the mappingproxy, exactly as it does for graphed's `Launch`;
  nothing calls it on a `SiteProfile`. `ServiceUnavailable` stores `dict(legs)`, so a mappingproxy `legs` pickles
  through `_result_blob` (extra test); `ServiceStatus` has no mapping field.
- 2: `CondorPilots.stop` waits through `release_quietly` (`_drain`); launch.py's own logger is gone.
- 3: dropping `SO_REUSEADDR` was tried and measured: the frozen
  `test_two_sets_entered_together_get_distinct_ports` fails (the children's closed connections leave TIME_WAIT on the
  6-port range, and a scan without REUSEADDR runs out of ports by trial 3). Kept it on POSIX and added a connect
  check to the scan, which rejects a port anything listens on: the macOS/BSD wildcard-listener case the reviewer
  raised, and Windows' non-exclusive wildcard listener. Extra test simulates the passing bind.
- Round-3 gates: main 1225 passed / 9 failed (the expected set) / 37 skipped, per-file min 97.03%, diff 100%;
  htcondor 297 / 9 (same set) / 2, min 96.0%, diff 100%; dask 358 passed, 1 failed, min 91.7%, diff 100%; sphinx ok.
  The dask failure, `tests/frozen/m43/test_dask_join_relational.py::test_salted_join_stays_co_partitioned_and_matches_local[numpy]`
  (an empty join result), passed 5/5 isolated reruns and in the round-2 full run. Its path (common/tasks_engine.py
  through `runner.backend.submit`) touches no m68a code, so it is recorded as an intermittent, not an m68a change.
- Coordinator check on the round-3 dask-scoped failure (`tests/frozen/m43/test_dask_join_relational.py::test_salted_join_stays_co_partitioned_and_matches_local[numpy]`, an empty join once): it passed 24/24 (12 reruns × 2 params) at b2f1a94. Its path calls `runner.backend.submit` directly (`common/tasks_engine.py:288,382,490`), and m68a's only dask-side change is the `services=` passthrough in `dask_runner` and `require_bound` in `transport_run_plan`. Not caused by m68a; CI's test-dask is the confirming run.
- m66 freeze amendment (owner ruling on the dispute): `freeze-m66-2` → `e2a2e4de750f51932d49453288eb9a43791a9474`.

## Implementer iteration 4 (graphed#63 resolve walk)
- venv graphed reinstalled at f69dec1 (graphed#63 head). `ci.yml` GRAPHED pinned there; the engine calls
  `graphed.services.resolve_services(bound, value)` directly; `_resolved_value` and its monkeypatch extra test deleted;
  blockers.md marks the resolve-walk blocker resolved (release-gate section kept). Changelog had no fallback mention.

## Final review (fresh reviewer, HEAD 0ae0c4e): APPROVE
- Local gates: 1228 passed, and the 6 failures are only the live-pool tests (m66 ×2, m67 ×3, m68a ×1; no pool here). The per-file coverage gate passes (minimum 97.03%, `local/executors.py`), diff-cover against upstream/main is 100% of 407 lines, precommit is ok, `sphinx -W` passes, and the frozen tree differs from 83ca76f only by the sanctioned m66 amendment.
- Nits left as follow-ups:
  - On Windows, the port scan's 1 s connect check costs about 1 s on each free port. Shorten the timeout, or use `SO_EXCLUSIVEADDRUSE` and skip the connect.
  - Probe-cancel callbacks keep completed probe futures on a warm set's stack until it closes. Use a pending set in the `_RunTasks` idiom.
  - `_check_grpc` honours proxy environment variables while `_check_http` bypasses them. Set `grpc.enable_http_proxy=0`, or document why they differ.
- Pending CI (graphed-org/graphed-executors#39): test-htcondor (pool + Triton), test-dask, test-parsl, and the all-OS matrix.
- Graphed side: graphed-org/graphed#63 (resolve walk). Reviewer APPROVE on f69dec1, with doc wording fixed in 1ff3479. CI was 36/36 green on f69dec1.

## Laptop iteration 5: the macOS stall (see ci-diagnosis.md, macOS)
- Root cause: on macOS 15+ runners, setup-python's builds stall 35-70 s in loopback reverse lookups
  (actions/setup-python#1223). `HTTPServer.server_bind` runs that lookup between bind and listen, so servers sat bound
  but not listening.
- Product: `LookupFreeHTTPServer`, the m41 override lifted into a base, now behind every product HTTP server.
- CI: macOS takes Python from uv, which measured ≤ 0.2 s on the same runner; the ineffective hosts step is removed.
- Gates on this Mac: frozen+extra 959 passed, 96 skipped; every file ≥ 90%; diff coverage 100% vs upstream/main; precommit ok.
