# m65 frozen suite, unit A3: `SubmitRunner` honours the run control (`plan-A3.md`, tag `freeze-m65a3`)

Frozen and read-only after `freeze-m65a3`. This file sits beside the unit A2 `README.md` and does not
edit it: plan.md decision 4 says a later unit adds new files and never edits earlier ones.

Contract: plan-A1 A-2 (the runner contract), plan-A3 A3-1..A3-3 and §Frozen-test contract. The
inherited bodies are the frozen A2 tests at `freeze-m65a2`. They are transcribed into `m65a3_bodies.py`
with the route replaced by a `Leg` (a `SubmitRunner` factory plus fixed/adaptive) and nothing else
changed. The ThreadBackend legs have 2 slots; the dask cluster has 2 workers × 2 threads.

| File / test | Contract | What it witnesses |
|---|---|---|
| `m65a3_bodies.py` | helper | A2 bodies 13, 14, 14b, 15, 15b, 15c, 16 and the stop-drain body, over a `Leg`; `started_before_first_finished` (STARTED `t` before the earliest FINISHED `t`) |
| `test_m65a3_control_submit.py` 13, 14, 14b, 15, 15b, 15c, 16 (40-task, no-task) | A3 contract | `SubmitRunner(ThreadBackend(2))`, fixed and adaptive; 15c on the fixed leg takes the hub branch (float partials; the `cancel_key=39` run equals the uncontrolled value, EXHAUSTED) |
| `…submit::test_stop_condition_does_not_end_a_cancel_drain` | A3-2 adaptive | CANCELLED and every STARTED key folded when the stop condition can fire only in the drain |
| `…submit::test_submit_runner_control_attribute` | 17, A3-3 | `monitor`/`control` default `None`; `control=` lands on the attribute; an assigned control is honoured and reset |
| `…submit::test_window_floors_at_one_and_widens_on_reread` | 19, A3-1 | `task_slots()` 0 then 4: full value in 30 s, exactly 4 STARTED before the first FINISHED; adaptive: `next_tasks.calls == n_partitions + 1` (a timed wake calls no `next_tasks`) |
| `…submit::test_timed_wake_sees_resume_and_new_slots` | 19b, A3-1 | widen: slots 1 → 8 at a timer, `0 <= t2 - t_flag < 0.3`; resume: exactly 2 STARTED before the resume, one within 0.2 s after it while key 0 still runs |
| `…submit::test_combines_submit_after_inputs_complete` | 21, A3-2 | 7 submits carry a future (the combines), none unfinished at submit; value equals the uncontrolled run (float partials) |
| `…submit::test_cancel_after_last_leaf_changes_nothing` | 23, A3-2 | cancel from key 7 (last leaf): 7 combines, EXHAUSTED as uncontrolled, all ones, control reset |
| `test_m65a3_control_dask.py` 14, 15, 15b, 16, stop-drain | 18 | `dask_runner(client)` with the control assigned afterwards, fixed and adaptive (stop-drain adaptive only) |
| `…dask::test_window_is_the_dask_task_slots` | 18 slot leg | `replicate_broadcast=True`, 8 × 0.3 s: exactly 4 STARTED before the first FINISHED (a window of `n_workers()` gives 2) |
| `…dask::test_scale_from_zero_completes` | 20 | `LocalCluster(n_workers=0)` scaled to 2 after 2 s: full value within 60 s (a window of 0 never returns) |
| `test_m65a3_control_parsl.py::test_parsl_task_slots_never_wait` | 22 | TPE `task_slots() == 2`; an HTEX with no block returns 0 in under 1 s; per path, a block that arrives after 3 s and an `n_workers()` that raises: full value within 60 s |

The dask slot leg and test 20 pass on the pre-A3 tip, because an uncontrolled run has no window. They
guard against the window mutants named in plan-A3. The parsl file puts the interpreter's `bin` on `PATH`
and this directory on `PYTHONPATH` itself.
