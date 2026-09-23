# m63 frozen suite: plan submit (graphed-executors)

Milestone **m63** (R19.6 throughput, part a). Frozen, read-only after `freeze-m63`. Spec: the
"Frozen-test contract" section of the lane's `plan-A.md`.

## New API this suite names

Every executor below is `ThreadExecutor`, `ProcessPoolExecutor`, `PinnedPoolExecutor`,
`SubmitRunner(ThreadBackend(...))`, `dask_runner(client)` or `parsl_runner(htex)`.

| API | Today's failure |
|---|---|
| `max_in_flight: int = 2` constructor keyword; `ex.max_in_flight`; `< 1` raises `ValueError("... max_in_flight must be at least 1 ...")` | `TypeError: ... unexpected keyword argument 'max_in_flight'` / `AttributeError: ... no attribute 'max_in_flight'` |
| `ex.submit(plan) -> concurrent.futures.Future[ExecResult]` | `AttributeError: ... object has no attribute 'submit'` |
| `SubmitRunner.monitor` public and assignable | an assigned monitor gets zero events (`_submitted(first) == N_LEAVES` fails) |
| one run at a time per local executor (contract 9) | overlap witness returns `1.0`; persistent pools return wrong bytes or raise |

## Files

| File | What it holds |
|---|---|
| `m63_harness.py` | module-level plan callables (`FloatLeaf`, `GatedLeaf`, `SleepyLeaf`, `OnePerCall`, `boom_on_three`, `flaky_once`, `overlap_a`/`overlap_b`), `RecordingMonitor`, `Call` (a daemon-thread call observed by join, never a hang), and the `check_*` scenarios every executor file runs |
| `test_m63_submit_local.py` | contracts 1-6, 8 on thread / process / pinned (process pools `persistent=True`, except the monitor legs) |
| `test_m63_submit_thread_backend.py` | contracts 1-6, 8 on `SubmitRunner(ThreadBackend(2))` |
| `test_m63_submit_dask.py` | contracts 1-8 on `dask_runner` over `LocalCluster(processes=True)`; `importorskip("distributed")` |
| `test_m63_submit_parsl.py` | contracts 1-6, 8 on `parsl_runner` over `start_htex(workers=2)`; `importorskip("parsl")`; the fixture puts the venv `bin` on `PATH` and this directory on `PYTHONPATH` |
| `test_m63_run_serialized.py` | contract 9 |

## Traceability

The same test name in several files runs the same `check_*` on that file's executors.

| Test | Contract | Witness | A wrong implementation it fails |
|---|---|---|---|
| `test_max_in_flight_is_a_validated_constructor_argument` | API | default 2, 5 read back, 0 and -1 raise with the fixed text | no knob; a knob not validated; a different message |
| `test_submit_equals_run_on_an_order_sensitive_fixed_plan` | 1 | `(value bytes, n_partitions, n_combines, stopped)` of `submit(plan).result()` == `run(plan)`, for one submit and three repeats; the fixture asserts its own order sensitivity (left fold and reversed fold differ in bytes) | a submit path that folds in completion order or uses another grouping; a non-`Future` return |
| `test_submit_equals_run_on_an_adaptive_plan` | 1 | `OnePerCall` serves one explicit 10-entry partition per call keyed by `ctx.n_done`; `target_events=150` of 400; both results `stopped is TARGET_EVENTS`, fingerprints equal over run and two submits | a submit path that batches `next_tasks`, ignores the stop, or keeps state across runs |
| `test_submit_returns_before_the_plan_finishes` | 2 | submit of a gated plan returns (in a helper thread) while `fut.done()` is False; opening the gate completes it with `run`'s bytes | a synchronous submit (mutant `sync` fails it) |
| `test_max_in_flight_bounds_outstanding_submits` | 3 | with `max_in_flight=2`, two gated submits return, a third stays blocked for 1 s, returns once gate 1 opens; values equal `run` | an unbounded queue (mutant `unbounded`) |
| `test_cancelling_a_queued_plan_frees_its_slot` | 3 | `f2.cancel() is True` on the queued plan; a following submit returns within 5 s | a slot released only when a plan body ends (mutant `cancel_keeps_slot`); plans started concurrently (mutant `concurrent`: cancel returns False) |
| `test_plans_complete_in_submit_order` | 4 | a one-leaf gated plan then a short plan: done-callback order `[10, 11]`; in process (thread, `ThreadBackend`) every leaf interval of plan 11 starts after the last of plan 10 ends | plans run side by side (mutant `concurrent` fails the thread, `ThreadBackend` and pinned legs); callbacks fired out of submit order |
| `test_errors_propagate_as_under_run` / `test_stage_error_from_a_worker_propagates_as_under_run` | 5 | leaf 3 of 6 raises a `StageError`; submit raises the same type, equal `str()`, equal `__dict__`, frames `[("user_analysis_m63.py", 63)]`; a plan queued behind it returns `run`'s bytes | an error wrapped or re-typed on the submit path; a failing plan that poisons the queue |
| `test_monitor_events_match_run` | 6 | `Counter` of `(phase, key, partition)`: exact equality and `n` of each phase on thread, `ThreadBackend`, dask, parsl; process pools: `SUBMITTED` multisets equal, `STARTED`/`FINISHED` at most once per key; each leg its own executor | events dropped, duplicated or re-labelled on the submit path |
| `test_assigned_monitor_reaches_the_next_plan` | 6 | `ex.monitor = first` then `run` gives `first` n `SUBMITTED`; `ex.monitor = second` then `submit` gives `second` n and `first` no more | `SubmitRunner` reading a private monitor (today); a monitor captured at construction |
| `test_retries_behave_as_under_run` (dask) | 7 | one marker file per leg, absent before and present after; `retries=1` gives the clean plan's bytes under run and submit; `retries=0` raises `ValueError` on both | retries not forwarded on the submit path |
| `test_close_drains_submitted_plans` / `..._and_a_closed_executor_still_accepts_submit` | 8 | `close()` in a helper thread still blocked after 1 s while two gated plans are submitted; after the gate opens every future is done, not cancelled, with `run`'s bytes; local: a submit after close completes | a close that cancels or abandons queued plans (mutant `close_nowait`); a local executor that refuses work after close |
| `test_run_beside_run_does_not_overlap` | 9 | `overlap_a` marks its start and waits 10 s for the gate `overlap_b` creates; A's value `0.0` (no overlap), B completes and the gate exists; thread / process / pinned, `max_workers=2`, not persistent | two `run`s executing side by side (today: 10 of 10 runs report 1.0) |
| `test_run_beside_a_submitted_plan_does_not_overlap` | 9 | as above with A submitted, B `run()` on a `ThreadExecutor` | a submit queue that does not serialize against `run` (mutant `concurrent`) |
| `test_concurrent_runs_on_a_persistent_pool_keep_their_values` | 9 | `ProcessPoolExecutor(4, persistent=True)` / `PinnedPoolExecutor(4, persistent=True)`, default comms, six distinct plans by three threads: each result's bytes equal that plan's serial run; driver-thread errors re-raised | cross-talk between concurrent runs on one kept pool (today: 10 of 10 runs wrong or raising) |

Narrowings: the in-process interval witness of contract 4 runs on thread and `ThreadBackend` only.
On dask a plan cannot finish beside a plan that blocks a worker (measured), so there the order leg
pins completion order only. The contract-9 overlap witness cannot fire on a persistent process
pool (a run's peer actors occupy every worker); the value leg covers that hazard.

## TEST_SANITY

The "mutant" names above are variants of a scratch reference `submit` (a bounded one-thread
queue in front of a serialized `run`), used only to show the suite passes a correct
implementation and fails plausible wrong ones; it is not shipped. Evidence and logs:
`graphed-workdir/lanes/throughput/test-sanity/` and the lane journal.
