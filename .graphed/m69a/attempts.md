# m69a — the H→γγ inclusive processor on graphed (attempts log)

Frozen suite `tests/frozen/m69a` at `freeze-m69a` (3dc678b). Design: `lanes/htcondor/plan-services.md` §4.

## Iteration 1 — baseline
- `GRAPHED_HGG_REQUIRED=1 pytest tests/frozen/m69a`: 18 pass, 9 fail, every failure `ModuleNotFoundError: analysis`.

## Iteration 2 — `examples/hgg/analysis.py`
- The original's processor method for method on `NanoEventsFactory.from_root(..., mode="graphed", schemaclass=NanoAODSchema)`;
  the §4 rewrites: counters as plan outputs, `gak.with_field` for assignment, `gak.zip` for the flat record,
  `gak.where`, guards deleted, `gak.apply_correction` (template path; the gzip jet-ID JSON is decompressed first, the
  correctionlib plugin parses JSON bytes), the lumi mask as a `record_external` plugin (payload = the golden JSON's
  bytes, `sha256_bytes`, coffea `LumiMask` loaded through fsspec's memory filesystem).
- `HggProcess` wraps the `aggregate_plan` process: materialized record → the original's `dump_to_parquet` steps;
  returns `{part name: {dataset: counters}}`; plan `combine` = dict union.
- Gate: parts test failed `zip() argument 2 is shorter`: `aggregate_plan` returns one value per distinct node,
  and MC's `genWeightSum` and `sum_genw_presel` are the same hash-consed node (no lumi mask on MC).
  Fixed by passing distinct nodes and mapping each output name to its node's slot.
- Gate: m69a 27/27 green (both runners, both fixtures; lumimask plugin; comparator; oracle).

## Iteration 3 — `run_local.py`, `validate_real.py`, README; lint/types
- Entry counts via coffea virtual NanoEvents (the examples tree may not name uproot).
- `validate_real.py --parts 3 --mc/--data <fixtures>`: 6/6 parts IDENTICAL, `DIFFERENCES 0`.
- ruff/mypy: `examples` added to mypy `files`, `examples/hgg` to `mypy_path` and ruff `src`; coffea/higgs_dna/
  pyarrow/fsspec/hgg_harness `ignore_missing_imports` (only test-hgg installs coffea). mypy strict clean.

## Iteration 4 — gates and real-data validation
- Gates: m69a 27 passed; the same 27 in a clean venv installed with test-hgg's exact lines; rest of frozen 604 passed /
  78 skipped; precommit `--fast` ok; prek (ruff, ruff format, mypy strict) passed; sphinx -W ok.
- LPC login node, coffea image + graphed 0.0.6 + fork b2612ab + uproot ca3a8a2 + higgs_dna d179305, `validate_real.py
  --parts 8` on the whole first GluGluHto2G_M-125_amcatnlo_2024 file (4180 entries) and DataC_2024 file (704556):
  16/16 parts IDENTICAL, `DIFFERENCES 0` (transcript `lanes/htcondor/probes/site-lpc/m69a-validate-real.txt`).
  First attempt failed importing `hgg_harness`: it imports pytest, which the recipe's venv lacked.
