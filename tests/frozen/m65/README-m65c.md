# m65 frozen suite, unit C: complete events per run (`plan-C.md` C-8, tag `freeze-m65c`)

Frozen and read-only after `freeze-m65c`. A new file beside the earlier m65 READMEs, which it does not edit
(plan.md decision 4). Plain-Python processes. The graphed C API (`RunRecorder`, `attach_run_report`) is read
as `graphed.debug` / `graphed.preserve` attributes inside the peer legs only, so on a graphed tree without C
the other legs fail on their own assertions (C r7 L1). Plan-C's one test 12 is split into one test per leg
family. **The failing plan**: 4 tasks, key 1 raises `ValueError("m65c boom")` at once, keys 0, 2, 3 sleep 0.2 s.

| Test (`test_m65c_hub_events.py`) | Leg (plan-C test 12) | Fails |
|---|---|---|
| `test_hub_failing_run_holds_its_events[proc-hub, thread-hub, thread-adaptive, proc-adaptive]` | hub failing legs; thread leg; adaptive legs | a run that raises before its leaves' terminals arrive; a leaked terminal in a good run started 0.5 s later; a wait gated on `comms` (adaptive legs) |
| `test_hub_wait_follows_the_leaf_futures` | (a): `_HUB_EVENT_DRAIN_S` = 0.3, slow keys 0.8 s | a count wait that runs before the leaf-future wait |
| `test_persistent_hub_runs_hold_their_events_on_a_kept_pool` | process persistent 5 runs | fewer than 4/4/4 at return; a kept pool released before every recorded run (> 2 distinct `STARTED` workers; C r3 L2) |
| `test_hub_counts_after_a_slow_monitor_returns` | (b) | counting before `on_task` returns |
| `test_hub_counts_when_the_monitor_raises` | (c) | counting inside `_dispatch`'s `suppress` (each run waits out the 2.0 s bound) |
| `test_switch_in_run_holds_only_its_own_events[dash, dash-none, failing-dash, adaptive-peer]` | switch-in legs (5/3/3/3 reps) | a kept pool's earlier tail delivered to the switched-in monitor |
| `test_peer_failing_run_reports_its_failing_task[proc-http, pinned, thread-ipc]` | peer failing legs (5 reps) | the failing actor's last batch lost (key 1 not `errored`); `inspect` without `errored=1` |
| `test_collect_peer_drains_the_failing_batch` | drain leg, in-process | no gated drain in `_collect_peer`; a drain without the gate |

Decisions on the dispatch constraints:
- Leg (c) warms the pool with one untimed run and times the next five (C r3 L1).
- The hub failing legs warm with a 2-task run and a 0.3 s sleep before the failing plan; the "0.5 s after the
  raise" check applies to them only; the failing-dash switch-in leg attaches `m` after the raise (C r3 N1 (i)).
- The default-comms switch-in leg warms with an adaptive run, so the hub pool exists (C r3 N1 (ii)).
- The drain leg waits up to 2 s for `("done",)` to reach `w0` and `w1`: `release_workers` sends each from its
  own thread, which on 3.14t can finish after `_collect_peer` raises (C r6 L1).
- The non-persistent five-run leg is the implementer's guard in `tests/extra/m65/`, not frozen (C r4 L2).
- Before graphed C lands, the thread-ipc peer leg fails on the missing `RunRecorder`; after it, that leg holds
  on today's executors.

Mutation check against the lane's C-8 prototype (`probes/cu1u3_cut.py`, `cu2u3_preclose.py`, the gated drain of
`cu5_drain_leg.py`, the actor exit wait patched into workers; not committed): all 16 pass in each of three runs;
an always-release hook, the prototype without the switch-in hook, the reversed wait order, counting before
`on_task`, no drain, and no actor wait each fail their row's test.
