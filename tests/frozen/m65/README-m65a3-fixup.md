# m65 frozen suite, unit A3 fixup: the controlled adaptive stop exit (tag `freeze-m65a3-fixup`)

Frozen and read-only after `freeze-m65a3-fixup`. Owner-ruled dispute: `.graphed/m65/disputes/test_m65a3_controlled_stop.md`.

| File / test | Contract | What it witnesses |
|---|---|---|
| `test_m65a3_control_stop.py::test_unused_control_ends_at_the_adaptive_stop_condition` | plan-A3 A3-2, stop exit (no check has seen CANCELLED) | `SubmitRunner(ThreadBackend(2))`, adaptive plan (40 one-entry tasks) with `StopCondition(target_events=5)`, bare and under an unused `RunControl()`: `(stopped, n_partitions)` equal each other and `(TARGET_EVENTS, 5)`, `5 < 40` leaves, control left `RUNNING` |
