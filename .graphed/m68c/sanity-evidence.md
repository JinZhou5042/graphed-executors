# m68c executors: test-author sanity evidence

Base: executors `db8fb0a` (branch `m68c`) over graphed branch `m68c` at `95b1b7b` (unit G's
`feat(runners)` tip, editable in `.venv-m68c`), parsl added to that venv (`uv pip install
"parsl>=2026.7.20"`, the `[parsl]` extra).

## Observed failures at the sanity revision (executors db8fb0a + suite, graphed m68c 95b1b7b)

Command: `.venv-m68c/bin/python -m pytest tests/frozen/m68c -q -p no:cacheprovider -rfs --tb=short`.
19 failed, 1 passed, 1 skipped; two runs gave identical outcomes (sorted FAILED/SKIPPED lines, diffed).

| Test | Observed first failure |
|---|---|
| thread: given, managed, peer edge, driver edge, no-service value, failed map submit; dask given; parsl ×2 | `AttributeError: 'DurablePlanV2' object has no attribute 'next_tasks'` |
| thread: pre-cancelled control | `AttributeError: ... 'empty'` |
| thread: mid-run cancel, cancel at a stage boundary | `AssertionError: []` after `wait_for` (30 s): the run thread raised `AttributeError: ... 'next_tasks'` before any submit (`lanes/services-v2/probes/e_sanityE1_standin/bg_reason.py`), which the test only re-raises from `run.result()` |
| thread refusals ×2, dask and parsl peer-transport refusals | `graphed.services.UnboundService: service 'sf' has no endpoint` |
| htcondor: runner refusal, driverless refusal, driverless derivation | `AttributeError: ... 'process'` |
| thread: unbound with no launch | passes (guard, as the README states) |
| htcondor live | skipped: no `htcondor2` bindings (macOS) |

On graphed main `d0ad16b` (before unit G) every test failed at the fixture instead (`join_plan()` has no
`reduce`; `graphed.shuffle.split` missing).

## Harness pieces checked through V1 plans (G-independent)

Scratch probes (session scratchpad `v1_harness_probe.py`, `v1_dask_probe.py`, `v1_parsl_probe.py`)
ran the harness's `SCALE` External in an `aggregate_plan`:

- given leg on ThreadBackend: 4 `/sf` GETs, value == `SequentialRunner` over the bound plan, x scaled ×3;
- managed leg: every body read in-task == the child's reported pid, `pid_gone` after `run`;
- `HTCondorRunner(HTCondorBackend(_NoStart(), 1, port_range=(0, 0)), min_pilots=0)` refuses a lambda
  combine (`pilots import plan.combine ...`) with no pilots;
- `submit_driverless` under the suite's bindings recorder with a GPU spec: `dag=True`,
  `announce_only == {"sf": "svc0"}`, calls `_htcondor, Schedd, from_dag, submit`;
- dask process `LocalCluster`: workers import the harness, 4 GETs, value equal;
- parsl HTEX via `RecordingBackend(ParslBackend(htex), wrap=False)`: `peer_data_movement=False`, 4
  GETs, value equal, 2 broadcast handles recorded.

## Lint and types

`ruff check` and `ruff format --check` on `tests/frozen/m68c`: clean. `mypy --strict tests/frozen/m68c`
(repo config): no issues in 5 source files at graphed 95b1b7b.
