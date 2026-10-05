# Test dispute: tests/frozen/m69a/test_hgg_conversion.py::test_one_plan_writes_every_part_and_returns_the_totals

## The test code (freeze-m72)
```python
value = runner.run(plan).value
expected = accumulate([counters for parts in oracle.values() for counters, _ in parts.values()])
assert sorted(value) == sorted(FIXTURES)
assert value == expected
for ds, leaves in expected.items():
    assert {k: type(v) for k, v in value[ds].items()} == {k: type(v) for k, v in leaves.items()}
```

## The clause it contradicts
plan-services.md §5.2 "Diagnostics": `examples/hgg/analysis.py` always fills the named diagnostic
histograms per dataset, and a dataset's value is m69a's counters plus `"diagnostics": {name: bh.Histogram}`
over `DIAGNOSTICS`, the named set. The test pins the counters-only value, so `value == expected` and the
leaf-type map fail on any implementation of that clause.

## Correction
Both assertions read `value[ds]` without its `"diagnostics"` key, and the test adds
`set(value[ds]["diagnostics"]) == set(analysis.DIAGNOSTICS)` with every entry a `bh.Histogram` (their
contents are m69b's `tests/frozen/m69b/test_hgg_diagnostics.py`). The README row names the
`"diagnostics"` key and the freeze sentence the `freeze-m69a-fixup` tag.
`test_each_dataset_run_on_its_own_collects_into_the_same_product` is unchanged: the collated and
per-dataset values stay equal with the histograms in them (`probes/m69b/probe_m69a_each_rv2.txt`).

## Owner ruling 2026-09-30: refreeze authorized
Committed with `python -m graphed_orchestrator.precommit --allow-refreeze tests/frozen/m69a`; tag
`freeze-m69a-fixup` (annotated).
