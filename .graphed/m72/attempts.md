# m72 — implementer iterations (graphed-executors, m69a re-spec)

Multiout lane (plans in `graphed-workdir/lanes/multiout/`: `plan.md`, `plan-C.md`). Frozen
`freeze-m72` = `4363da0` (`tests/frozen/m69a/test_hgg_conversion.py` refrozen for
`plan(fileset, *, year, out)`); graphed side at `graphed-org/graphed` branch
`feat/multi-output-plans`.

## C1 — CI installs graphed from the multi-output-plans commit

`GRAPHED` and `docs/requirements.txt` pin `graphed[awkward,numpy] @ git+…@4a3ae76b…`; `test-hgg`
gains `dtolnay/rust-toolchain@stable` (the other six jobs installing `GRAPHED` already had it).
Re-pin when the graphed PR's head moves or it merges.

## C2 — one plan over a fileset (refrozen `test_hgg_conversion.py` 7/7; m69a 26/26)

`process` returns `{"record", "counters", "metadata"}`; the record's fields are zipped in sorted
order (the original's writer sorts). `dataset_plan` = `aggregate_plan(*distinct counter nodes,
writes=[parquet_write(record, out/<ds>/nominal, name=part_name, metadata=…,
arrow_options={"extensionarray": False})], reduce=Counters(slots), combine=accumulate, empty=dict)`;
`plan(fileset)` = `collate` over datasets. `part_name` refuses a blind partition ("steps") and strips
`/…;cycle`. `run_local.py` builds a stepped fileset; `validate_real.py` runs ONE plan over both
files and judges each part by `compare_part` and each dataset's totals against the accumulated
oracle counters — on the committed fixtures: every part IDENTICAL, DIFFERENCES 0.

## C3 — docs

`examples/hgg/README.md` (fileset example, executed on the fixtures: 4 parts, both datasets'
counters), `docs/hgg.rst` (the one-plan shape; the table's counters, writer and part-name rows;
the MC fixture is now the 2024 GluGluH slice), `docs/changelog.rst`. `sphinx-build -W` clean.

## R1 repair — per-dataset union wording (review impl-r1 F3)

`examples/hgg/README.md` and `docs/hgg.rst` now say the union is over `plan({dataset: files})`
values; `dataset_plan`'s value is flat counters, whose union overwrites.
