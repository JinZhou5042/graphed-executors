# m68c executors TEST_SANITY, E freeze 1

**Verdict: PASS.** Suite `e41032a` (unchanged since), executors src `db8fb0a`, graphed branch `m68c` at
`95b1b7b` (unit G implemented, editable in `.venv-m68c`).

1. **Collects:** 21 items (thread 12, dask 2, parsl 3, htcondor 4); `pytest tests/frozen tests/extra
   --collect-only` has no collection errors and picks up all four m68c files.
2. **Failure reasons:** 19 failed, 1 passed (the unbound guard, as stated), 1 skipped (htcondor live, no
   bindings). Each failure is the §6 reason: `next_tasks` (11), `empty` (1), `UnboundService` (4),
   `process` (3). Two of the `next_tasks` rows (mid-run cancel and stage-boundary cancel) surface as
   `AssertionError: []` after a 30 s `wait_for`, because the run thread's exception is re-raised only
   from `run.result()`. A direct probe shows that exception is `AttributeError ... 'next_tasks'` with
   zero submits (`lanes/services-v2/probes/e_sanityE1_standin/bg_reason.py`). `sanity-evidence.md` has
   been corrected to this revision.
3. **Determinism:** two full runs gave identical sorted outcome lines. Under the stand-in (item 6), the
   control, edge and fault tests (6 tests) passed 8/8 repeats.
4. **Lint and types:** `ruff check` and `ruff format --check` are clean. `mypy --strict tests/frozen/m68c`
   (repo config) reports no issues in 5 source files, so the 3 `call-arg` errors from earlier rounds
   have cleared.
5. **Coverage:** the main matrix's `pytest tests/frozen tests/extra --cov=graphed_executors` collects
   the m68c files. The thread refusal and unbound tests alone record hits in `submit/engine.py`
   (117 lines), `local/executors.py` and `submit/services.py`. The ci-commit listing of the thread,
   dask, parsl and htcondor files is the implementer's `ci` commit (plan.md §7).
6. **Discrimination:** `probes/e_sanityE1_standin/apply_impl.py` builds a scratch §4 implementation
   (`_run_stages`, pick edge, refusal helper, HTCondor V2 pre-check) in a throwaway copy of the tree,
   running on real unit G. `good` passes all 20 runnable tests, including dask LocalCluster and parsl
   HTEX. Each mutant fails at least the test that pins its mechanism (`mutants.out`):

   | Mutant | Pinning test(s) that fail |
   |---|---|
   | `unbound_stages` (stages run the unbound plan) | given, managed, dask given |
   | `early_release` (services closed before stages) | managed |
   | `dispatch_first` (V2 before the ServiceSet prologue) | unbound guard (+7) |
   | `pick_per_map`, `pick_whole`, `driver_on_peer` | peer edge |
   | `pick_on_driver`, `driver_whole`, `driver_futures` | driver edge |
   | `drop_input` (a gather misses one map) | value tests (thread ×4, dask) |
   | `counts_with_picks` | no-service counts |
   | `v1_early` (`plan.empty()` early return) | pre-cancelled control |
   | `submit_only_check` | mid-run cancel |
   | `ignore_wait`, `picks_before_wait` | stage-boundary cancel |
   | `bypass` (`self.backend.submit`) | failed map submit, mid-run cancel |
   | `refuse_after`, `refuse_badmsg`, `no_refuse` | thread ×2, dask, parsl refusals |
   | `key_no_stage` | dask given (+4 thread) |
   | `v1_roles`, `bad_role`, `refuse_all`, `no_services` | htcondor refusal ×2 / derivation |
   | parsl `pick_on_driver` / `driver_on_peer` | HTEX driver edge / HTEX peer control |

   **Not constructed:** the live HTCondor leg (it needs the `htcondor2` bindings and a pool, and is
   skipped on macOS; it runs in the test-htcondor job).
7. **Tests unpassable under the plan as written:** none found. The stand-in follows §4 and the
   exit constraints (top-level positional futures, `plan.value`, SequentialRunner counts, picks after
   the stage wait), and it passes every runnable test.

Note, not an m68c defect: `stop_htex` leaves one `process_worker_pool` per parsl module run, as the
m63 suite does. Every pool this round started was killed.
