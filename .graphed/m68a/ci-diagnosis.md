# m68a CI diagnosis (graphed-org/graphed-executors#39, head bfabb04)

Logs were read through the GitHub Actions API (the MCP `get_job_logs`), so the blob-host egress block did
not matter. `main` (c2298d7) is green in every job, so each failure below is this PR's.

| job | result on bfabb04 | cause | fix |
|---|---|---|---|
| test-parsl py3.12 / py3.13 | cancelled at the 30-min timeout, after 72 tests passed (55%) | hang: the run scope cancels HTEX futures | 46f5802 |
| test-htcondor py3.12 | 1 failed (`test_services_live::test_a_driver_hosted_service_over_pool_pilots`) | `_ParslFuture` has no `cancelled()` | 46f5802 |
| test macos-latest py3.11–3.14 | 44 failed each, 1h36m | `socket.getfqdn()` blocks on macOS runners | 3c53335 (CI) |
| test-dask py3.12 | 1 failed (`m65/test_m65a3_control_dask.py::test_window_is_the_dask_task_slots[adaptive]`, `2 == 4`) | not reproduced; no m68a cause found, see below | none (the mirror push's run confirms) |
| ci required | failure | aggregates the above | — |

## test-parsl: hang (cancelling HTEX futures)

Evidence:
- CI: pytest printed 72 dots in 3m46s, then nothing until the 30-min cancel; orphaned `process_worker_pool` processes
  were terminated at cleanup.
- Local (CPython 3.12, parsl 2026.9.14 as CI resolved it, graphed d0ad16b): `pytest tests/frozen/m46` on this branch
  hangs deterministically in `test_parsl_m43_engine_on_tpe.py::test_m43_repartition_runs_on_tpe_with_the_full_m43_shape`,
  right after `test_parsl_failure_attribution.py`. On `origin/main`, the same command passes (51 tests).
  The faulthandler/py-spy dump shows a TPE worker thread stuck in `zmq.Context.__del__ -> destroy -> term`, fired by
  garbage collection during an `import numpy.ma`. It holds the import lock, and the other worker waits on that lock.
- Mechanism: m68a's `SubmitRunner.run` scope calls `_RunTasks.cancel()` on exit, which calls `backend.cancel()` on the
  run's pending plan tasks. When a plan raises (the failure-attribution tests), some of those tasks are already
  dispatched to HTEX. HTEX never marks a future running, so `Future.cancel()` succeeds. When the task's result lands,
  parsl's `_result_queue_worker` calls `task_fut.set_result(...)` without a guard (parsl
  `executors/high_throughput/executor.py:545`), which raises `InvalidStateError`, and the thread dies:
  - every later task of that executor never resolves (the CI hang);
  - its zmq socket is never closed, so collecting the context later blocks in `term()` (the local hang).
  `parsl_backend/transport_peer.py::_cleanup_after_barrier` documents the same hazard and avoids cancel for it. On
  main, the engine never cancelled parsl futures.
- Fix (46f5802): `ParslBackend.cancel` is a no-op on HTEX and still cancels on TPE, where it is safe. This also makes
  the stop-condition path's existing comment true ("cancel_running=False backend no-ops").
  `tests/extra/m47/test_parsl_cancel.py` pins both.
- Verified locally:
  - `pytest tests/frozen/m46` passes on the fixed branch.
  - The exact test-parsl step (py3.12, parsl 2026.9.14) passes: 133 passed in 5m22s.
  - Combined coverage is 93%, and the per-file gate passes.
  - diff-cover against main is 100% for parsl_backend.
  - py3.13 was not run locally.

## test-htcondor: `_ParslFuture.cancelled`

Evidence: the frozen `test_services_live.record_probe_answers` done-callback calls `f.cancelled()`. The HTCondor
backend hands out `parsl_backend.backend._ParslFuture`, which had `cancel` but no `cancelled`, so the callback
raised `AttributeError: '_ParslFuture' object has no attribute 'cancelled'` (logged by `concurrent.futures`), no
answer was recorded, and the test failed with `no probe answered`. Everything else in the job passed:
306 passed, 1 skipped, and per-file coverage was fine.
Fix (46f5802): `_ParslFuture.cancelled()` delegates to the raw future, with an extra test.
Not verified locally: there is no pool or Triton here. The live test needs the CI pool.

## macOS: `getfqdn` reverse lookup

Evidence: all 44 failures are m68a service tests, and every one is a timeout on a readiness check or a worker probe.
- **The 3c53335 `/etc/hosts` step changed nothing.** Its own timing lines, on run 36608363724: `getfqdn()` took 70.05 s
  before the step and 70.03 s after.
- **Cold timings on a macos-latest runner, one process per call, caches flushed first** (branch
  `diag/macos-fqdn` on the fork, run 36614709844):
  - setup-python's 3.12: `getfqdn()` >45 s; `getfqdn('127.0.0.1')`, `getfqdn('localhost')` and
    `http.server` bound on `127.0.0.1` 35 s each; `http.server` bound on `""` >45 s.
  - The same Python after `scutil --set HostName gha-runner` and a `127.0.0.1 gha-runner` hosts line (quarto-cli#14941's
    fix): 35 s for every one of those calls.
  - uv's python-build-standalone 3.12: every one of those calls took ≤ 0.20 s.
  - This matches actions/setup-python#1223: on macOS 15+ runners, loopback reverse lookups stall at the OS resolver
    for setup-python's builds.
- **Where the stall sits, from dumps on this Mac** with `socket.getfqdn` slowed to 70 s in every process (a
  sitecustomize) and a faulthandler dump per process: `test_the_driver_job_hosts_the_service_and_resolves_the_value`
  stalls in two places, both `HTTPServer.server_bind` → `getfqdn`:
  1. The driver job's task server (`htcondor_backend/server.py` `_Http`). The pilot then hangs in its POST. This is
     product code.
  2. The frozen `service_child.py` (`ThreadingHTTPServer(("", port))`). This is frozen code; so is the recipe's argv
     `python -m http.server`, which `test_services_sites.py` pins.

  After the product fix, only (2) remains in the dumps.
- On this Mac with a normal resolver the frozen suite passes: 140 passed, 4 skipped, 56 s.

Fixes:
- **Product:** `local/_transport.py`'s m41 `_InboxServer.server_bind` override becomes the base class
  `LookupFreeHTTPServer`. Every product HTTP server uses it: the transport inbox, `common/http_plane.py`'s
  `_DualRouteServer`, the htcondor task server, and both shuffle servers. `tests/extra/m68a/test_m68a_lookup_free_bind.py`
  covers them with `socket.getfqdn` patched to raise, a positive control, and a source check that no module builds a
  plain `ThreadingHTTPServer`.
- **CI:** frozen code and the tests themselves call `getfqdn` (`host_identity() == socket.getfqdn()` is pinned), so the
  macOS legs take Python from uv (`uv venv --seed --python-preference only-managed`) instead of setup-python. The
  3c53335 hosts step is removed.

## test-dask py3.12: `test_window_is_the_dask_task_slots[adaptive]`

CI measured `started_before_first_finished == 2`, expected 4: only 2 of the 4 window slots had started a 0.3 s probe
before the first finished. In the same run, the `[fixed]` case and the whole py3.14 leg passed.

Not reproduced locally (CPython 3.12, distributed 2026.8.0, 4 CPUs):
- the exact test-dask pytest command, twice: 359 passed each time, in about 3m10s (CI: 3m02s);
- the module's window tests in isolation, 3/3;
- 10 runs under a 4-process CPU burn: 0/10 failures on this branch and 0/10 on `origin/main`.

m68a's only change on this path is `_RunTasks`:
- one extra `add_done_callback` per dask future, which only schedules onto distributed's callback thread and does
  not delay submission;
- a cancel, at run exit, of futures not yet done. That runs after the plan's result, so it cannot delay the first
  four starts.

The window is `DaskBackend.task_slots()` (4), and all four leaves are submitted back to back. A count of 2 means
dask started two of them more than 0.3 s late, for example one worker busy or not yet ready. I found nothing in
this PR that causes that, and I am not calling it a flake without a reproduction. The run triggered by mirroring
m68a-ci is the confirming run. If it fails again, the next step is to record per-task worker and start times
from the recorder in an extra test.

## Verification

- `ruff check`, `ruff format --check` and `mypy` (strict, 289 files) are clean.
- `graphed_orchestrator.precommit` was NOT run: installing graphed-orchestrator from git was refused by this
  session's permission policy. The repo's `.pre-commit-config.yaml` hooks (ruff, ruff format, mypy) were run directly
  instead. The coordinating session should run `python -m graphed_orchestrator.precommit . --fast --no-coverage`
  on m68a-ci before mirroring.
