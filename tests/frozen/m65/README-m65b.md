# m65 frozen suite, unit B: lean events and per-worker push (`plan-B.md`, tag `freeze-m65b`)

Frozen and read-only after `freeze-m65b`. A new file beside the A2 `README.md` and A3's
`README-m65a3.md`, which it does not edit (plan.md decision 4). No test imports `graphed.debug.dashboard`:
push is witnessed through `m65b_probe`, whose recording monitors each write one connection file
`<dir>/<pid>.<uuid>.jsonl` on their first event.

| File / test | Contract | Fails |
|---|---|---|
| `m65b_probe.py` | helper | `PushRecorder` (driver; events in memory and in its file), `worker_recorder` (a fresh monitor per call), `connections`, `LeanRecorder`, `fake_profiler`; `.profiles` counts `on_profile` |
| `test_m65b_lean_push.py::test_lean_routes_emit_submitted_and_terminal_only` (11) | B-2, B-3 on route set R | a STARTED in lean mode; a labelled terminal; no terminal event; terminal keys not `range(n)` (exact routes) |
| `…::test_per_worker_push_bypasses_the_driver` (12) | B-6 on proc-hub, proc-ipc, proc-http, pinned | worker events through the driver; a monitor per task instead of per worker process; a worker monitor in the driver process; routing broken with push off |
| `…::test_persistent_pool_pushes_each_run_to_its_own_monitor` (12b) | B-6, B r7 L2 | a persistent pool that keeps run 1's factory, so run 2's events land in run 1's directory or run 3 (no monitor) still pushes |
| `…::test_inprocess_worker_push_and_lean` (14) | B-3, B-6, B3 `_proc_init` | a STARTED or labelled terminal; `_proc_init()` leaving the drain thread alive; any `partition_label` call without a monitor; hub profile trees not pushed; an exit order that closes the worker monitor before `_proc_drain_final` sends the final profile |
| `…::test_peer_events_carry_task_keys` (19) | B-8 | leaf indices instead of task keys (keys `100 + 2i`) |
| `test_m65b_inprocess_actors.py::test_inprocess_peer_actors_push_and_lean` (17) | B-6, B-8 on the four actors | an actor that ships `("events", …)` or `("profile", …)` to the driver while pushing; a STARTED or labelled FINISHED in lean mode; no pushed profile |
| `test_m65b_push_submit.py::test_lean_submit_runner_emits_submitted_and_terminal_only` (11) | B-2 on SubmitRunner, B4's four drain sites | a STARTED; a drain waiting for two events per leaf (30 s timeout > 10 s bound) |
| `…::test_submit_runner_per_worker_push` (13) | B-6 `RunContext`, `_WORKER_MONITORS` | worker events through the driver; a 30 s drain wait; a worker cache that ignores the factory (leg 2 lands in leg 1's directory) |
| `test_m65b_push_dask.py::test_dask_workers_push_one_connection_per_worker` (15) | B-6 on dask | worker events through the driver; more than one monitor per worker process |

Decisions on the dispatch constraints:
- Test 17 passes `emit=True` with `monitor_factory`: a factory reroutes the streams `emit` would ship, so
  the "nothing reaches the driver" clause can fail (B r7 L1).
- Test 12b pins that each run of a persistent process pool pushes to that run's monitor (B r7 L2).
- Test 14 adds an exit-order leg: `atexit.register` is captured, the worker monitor registers its close
  when built, and the captured hooks run LIFO; the final profile must reach an open monitor (B r1 atexit).
- Test 14's two-positional leg asserts a drain thread is alive before the bare `_proc_init()` asserts none is.
- New keyword arguments go through `dict[str, Any]` splats so mypy has no call-arg error before they exist.
