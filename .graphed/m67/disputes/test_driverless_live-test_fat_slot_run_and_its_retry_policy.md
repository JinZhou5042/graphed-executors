# Test dispute — `tests/frozen/m67/test_driverless_live.py::test_fat_slot_run_and_its_retry_policy`

## The test

```python
assert re.search(r"ExitCode\s*(==|=\?=)\s*3\b", str(row["OnExitRemove"])), row["OnExitRemove"]
```

## Failure

HTCondor 25 turns the driver's `retry_until = 3` into
`NumJobCompletions > JobMaxRetries || ExitCode is 3` in the schedd ad. The CI pool (PR #37,
run 36173424329) and LPC (`lanes/htcondor/probes/site-lpc/m67-driverless.txt`, 25.0.14) both print
that spelling, so the assertion fails on a correct retry policy.

## The clause

m67 §2 live (c): the driver's history row carries `JobMaxRetries == 2` and an `OnExitRemove` that
ends retries on exit code 3. `is` is the ClassAd meta-equality operator, a synonym of `=?=`.

## Proposed correction

`r"ExitCode\s*(==|=\?=|is)\s*3\b"`. The poison leg (`ExitCode 3` after exactly one start) keeps
witnessing the behaviour.

## Ruling (owner, 2026-09-25)

Accept `is`: the regex becomes `r"ExitCode\s*(==|=\?=|is)\s*3\b"`, nothing else in the file changes.
Amendment under `--allow-refreeze tests/frozen/m67`, re-tagged `freeze-m67-2`.
