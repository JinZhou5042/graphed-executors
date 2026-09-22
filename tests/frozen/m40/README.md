# M40 frozen suite — graphed-executors (distributed hash JOIN)

Milestone **M40**. This repo runs the JOIN: the co-partitioned two-phase map-write + gather-join
executor, the broadcast path, the deterministic-salt skew route, and the bounded-memory streaming/spill
join kernel. It is the integration home where the generic join kernel runs against BOTH real backends
(the a2 backend-independence witness by execution). Extends the M39 exchange (`run_repartition`) into a
two-input join (`run_join`). **Frozen — read-only after `freeze-M40-0`.**

## Files → theme (§8 M40)

| File | Themes | What it witnesses |
|---|---|---|
| `join_backends.py` | harness | real `AwkwardBackend`+`NumpyBackend` join adapters; scenario builders; the TEST-AUTHORED **duplicating** relational oracle (routes via the golden-pinned `partition`, never the join kernel) |
| `test_broadcast_join.py` | **(b)** | broadcast join == shuffle join == oracle; under broadcast `large_side_blocks==0` + `broadcast_puts==workers`; shuffle contrast `large_side_blocks>0` |
| `test_join_cost_model.py` | **(c)** | pinned rule at the exact crossover; executor HONOURS the plan-recorded `broadcast=` (not a runtime recompute); two auto runs → identical choice + bytes |
| `test_join_arrival_determinism.py` | **(e)** | join bit-identical under drop+dup announcements and a forced steal; content == oracle; faults engaged (non-vacuous) |
| `test_join_skew_salt.py` | **(f)** | skewed hot key: same salt → byte-identical + correct; salt is content-neutral yet re-places some key (live routing input) |
| `test_join_bounded_memory.py` | **(B5)** | `peak_join_bytes<=budget` joining a dest_pid whose duplicated output is 8× the budget; spill engaged; `join_output_rows==n_left*n_right` |
| `test_join_benchmark.py` | gates | block counts O(#producer-tasks·P) per side, not M×R; counts row-count-independent (no per-row messages); broadcast crossover measured from the rule |

## Pinned execution contract (test-author decisions — the implementer builds `graphed_exec_local.shuffle`)

```
run_join(backend, left_blocks, right_blocks, parts, *, on=("__joinkey__",), how="inner",
         workers=1, comms="ipc", store_root=None, broadcast=None, salt=0,
         mem_budget_bytes=None, steal=False, faults=ShuffleFaults()) -> ShuffleResult
# left_blocks = build side, right_blocks = probe side. broadcast: None=cost model chooses (recorded,
# deterministic); True/False = executor HONOURS a plan-recorded choice (E5). salt folds into the pinned
# sha256 route (never hash()). mem_budget_bytes bounds the kernel working set (covers OUTPUT duplication).

broadcast_join_choice(build_bytes, probe_bytes, n_nodes) -> bool   # broadcast IFF build*N < build+probe

# ShuffleResult.value: dict[part -> joined block]  (part = dest under shuffle; probe-partition index
#   under broadcast — the JOIN RESULT is the union over blocks, the partitioning differs by strategy).
# dest_block_hashes: dict[part -> sha256(joined wire)] — the cross-run determinism key.

# NEW ShuffleWitness join counters (test-author pins; implementer adds — mirror the M39 witness fields):
#   broadcast_chosen: bool          # the recorded/honoured strategy
#   build_side_blocks: int          # stage-1 blocks written for the build (left) side
#   large_side_blocks: int          # stage-1 blocks written for the probe (large) side; 0 under broadcast
#   broadcast_puts: int             # cluster.put replicating the build side (== #nodes broadcast, else 0)
#   peak_join_bytes: int            # kernel-measured peak live working set (the B5 gate)
#   join_spilled_partitions: int    # radix partitions spilled to the node-local Store (B5 non-vacuity)
#   join_output_rows: int           # total joined rows (relational duplication count)
# Reused from M39: n_producer_tasks, announcements_sent/dropped, stolen_tasks, block_holder,
#   manifest_owner, manifest_bytes, manifest_fetch_is_per_dest.
```

The correctness oracle is TEST-AUTHORED (`join_backends.expected_join_*`), a DUPLICATING relational
merge (§3.3 pin, trap 3): a probe row with k build matches ⇒ k output rows. It reuses only the
pre-existing M39 `partition` (golden-pinned routing) + plain field reads, so the executor cannot share
a join bug with the oracle. A grouped / list-of-matches / dedup impl produces a different multiset.

## Traceability — per test, the WRONG impl it discriminates

| Test | Mechanism witnessed | WRONG impl it FAILS |
|---|---|---|
| `test_broadcast_result_equals_shuffle_result_and_oracle` | broadcast result multiset == shuffle == duplicating oracle | broadcast that drops/regroups rows, or diverges from the shuffle join |
| `test_broadcast_does_not_shuffle_the_large_side` | `large_side_blocks==0` + `broadcast_puts==workers` under broadcast; `>0`/`==0` under shuffle (contrast) | an impl routing the large side through `_stage1_map_write` (blocks>0); one that never places the build side on nodes (puts==0) |
| `test_cost_rule_is_the_pinned_inequality_at_the_crossover` | `broadcast_join_choice` True/False straddling `build*N == build+probe` | a different comparison (`build<probe`, `<=` vs `<`) — fails a boundary vector |
| `test_executor_honours_the_recorded_choice_not_a_runtime_recompute` | forced `broadcast=True/False` recorded in `broadcast_chosen`, correct | a runtime-recompute model that overrides the recorded choice from observed sizes |
| `test_auto_choice_is_deterministic_across_two_runs` | two auto runs → identical `broadcast_chosen` + `dest_block_hashes`; == pinned rule on form sizes | a racy runtime cost model (choice/bytes drift) |
| `test_join_is_byte_identical_under_fuzzed_arrival_and_duplicates` | drop+dup every announcement → identical `dest_block_hashes` + oracle | a merge order that depends on block arrival order |
| `test_join_survives_a_stolen_producer_task_bit_for_bit` | stolen task's block on the thief, manifest at owner; bytes == clean run + oracle | a merge that depends on which node computed a stolen task |
| `test_skewed_join_is_deterministic_and_correct` | same salt → byte-identical + relationally correct under ~90% skew | `hash()`-based / nondeterministic routing (drifts across runs) |
| `test_salt_is_a_live_content_neutral_routing_input` | across salts: identical content, but some salt re-places a key | an impl that IGNORES salt (identical partitioning); one where salt corrupts the join |
| `test_join_spills_and_streams_within_a_budget_smaller_than_the_output` | `peak_join_bytes<=budget` while output is 8× budget; spill engaged; `join_output_rows==n_left*n_right` | a full-RAM `concat` (peak==output>budget); a dedup/grouped impl (fewer rows) |
| `test_block_counts_are_O_producer_tasks_times_P_per_side_not_MxR` | per-side blocks ≤ #producer-tasks·P and < #src·P; T ~ W | the O(#src_pid·P) tiny-fragment M×R blowup |
| `test_counts_do_not_scale_with_row_count` | 10× rows leaves block/announcement counts unchanged | a per-row-message / per-fragment impl (counts scale with rows) |
| `test_broadcast_crossover_is_measured_from_the_pinned_rule` | both regimes realised, consistent with the recorded rule | a hard-coded winner / unmeasured crossover |

## Runtime / flakiness discipline (R0.10a)

Every gate asserts **structural counters** or **content-addressed bytes**, never a clock. Stealing and
announcement faults are forced deterministically (`ShuffleFaults`), so (e) is not flaky. The (B5)
gate's primary discriminator is the **kernel working-set counter** `peak_join_bytes` — the direct
mirror of the frozen M39 `peak_writer_buffer_bytes` and M38 `max_frontier`; a whole-process RSS/wall
gate would be flaky AND cannot isolate the kernel because the joined result is materialised in `.value`.
Most themes
run in-process (`comms="ipc"`).

## Non-vacuity

Pre-implementation: `run_join` and `broadcast_join_choice` are absent from `graphed_exec_local.shuffle`,
and the join witness counters (`broadcast_chosen`, `large_side_blocks`, `peak_join_bytes`, …) do not
exist → right-reason `ImportError` / `AttributeError`. The relational oracle uses only the M39
`partition`, so it collects and computes today (proving the harness itself is not the failure). Every
theme has an explicit non-vacuity assertion (faults engaged, spill engaged, skew present, oracle
non-empty) so a silently-degenerate scenario cannot pass.
```
