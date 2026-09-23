# m63 — implementer attempts log

Milestone **m63** (R19.6 throughput, part a): `submit(plan)` pipelining. Branch `lane/throughput`.
Frozen suite `tests/frozen/m63/` (tag `freeze-m63`). Plan: lane `plan-A.md` (A1–A4).

## Iteration 1 — A1: one run at a time per local executor

`_BaseExecutor.run` holds an instance `threading.Lock`. `test_m63_run_serialized.py`: the five
run-beside-run / persistent-pool legs pass; the submit leg waits on A3 (`submit` missing).

## Iteration 2 — A2: `SubmitRunner.monitor` public, read once per run

`self.monitor` replaces `self._monitor`; `run` reads it once and passes it to `_run_fixed`,
`_run_adaptive` and `_profiler_payload`. Probe: `r.monitor = m; r.run(fixed_plan(7))` on
`ThreadBackend(2)` gives 6 `SUBMITTED` (was 0 on main).

## Iteration 3 — A3: `submit(plan)` behind a bounded in-flight queue

`_plan_queue.PlanQueue`: stdlib one-thread `ThreadPoolExecutor` driver calling the host's `run`,
`BoundedSemaphore(max_in_flight)` released by the future's done-callback (fires on cancel too).
Hosts: `_BaseExecutor` (+ `_ProcessExecutorBase` forwards), `SubmitRunner`; `dask_runner` /
`parsl_runner` forward `max_in_flight`; `close()` drains first. CI: the m63 dask/parsl files join
the `test-dask`/`test-parsl` pytest lines. Whole frozen m63 suite (thread, process, pinned,
ThreadBackend, dask, parsl, serialized) passes, first attempt.

## Iteration 4 — A4: docs

design.rst "Recording the next plan while this one runs" (executed example, output matches the
comment), one sentence each in api.rst / dask.rst / parsl.rst; `sphinx-build -W` clean.

## Iteration 5 — gates (logs: lane `impl/exec-R*.log`, script `impl/exec_gates.sh`)

- Main-job stand-in (dask/distributed/parsl blocked in `sys.modules`): `tests/frozen tests/extra`
  pass; per-file gate ok (`_plan_queue.py` 100, `local/executors.py` 93.43, `submit/engine.py`
  93.17); diff gate from `tests/frozen` alone: 63 changed lines, 100 %.
- test-dask job (m63 dask file added): pass; per-file ok (`submit/engine.py` 97.48,
  `dask_backend/backend.py` 100); diff 18 lines, 100 %.
- test-parsl job (m63 parsl file added): pass; combined report ≥ 90; per-file ok
  (`parsl_backend/backend.py` 91.89); diff 2 lines, 100 %.
- Full `tests/frozen tests/extra` with extras installed: 832 passed (baseline 759 + 73 m63).
- `mypy` (repo strict config) over `src tests`: clean. `sphinx-build -W`: clean.

## Iteration 6 — review r1 F1: one monitor snapshot per local run

`_BaseExecutor.run` sets `self._run_monitor = self.monitor` under the run lock; every run-path
read (`_prepare`, `_combine_cb`, peer SUBMITTED/forwarding/profiler factory, thread submit, process
pool factory, the persistent collector's `_dispatch`) reads the snapshot. Class search: the only
other `run(self, plan` host, `SubmitRunner`, already snapshots. `tests/extra/m63/test_monitor_snapshot.py`
(thread peer/hub, process peer/hub-collector): 4 pass; with the fix stashed, 4 fail. design.rst
adds the interpreter-exit trap (L1). Gates (`impl/exec_gates.sh`, `impl/exec_gates.r1.out`): all
exit 0; `local/executors.py` 93.45 %; frozen-only diff 78/78 lines, dask 18/18, parsl 2/2.
