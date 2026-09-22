# Test dispute — m40 `tracemalloc` corroboration is state-dependent

Test: `tests/frozen/m40/test_join_bounded_memory.py::
test_join_spills_and_streams_within_a_budget_smaller_than_the_output` — the last assertion only:

```python
assert peak <= output_bytes + 4 * budget
```

## The contradiction

The test's own docstring, and the M41 sibling it cites, pin the rule this assertion breaks: a
witness must be "a counter, so it is deterministic and CI-stable per R0.10a — a wall-clock/RSS gate
would be flaky". `peak` is not a counter: it is a whole-process `tracemalloc` reading that also sees
every allocation any other thread makes inside the window, so what it measures depends on the state
the session reached before the test ran.

## Measured

- The test in isolation is stable: 10 consecutive runs of the module alone (macOS, py3.11) measure
  1303950-1306496 ([numpy]) and 1323965-1325307 ([awkward]) against a 1920000 ceiling.
- In a whole-session run it is not. `pytest tests/frozen` (the command `main` itself is gated with,
  no `tests/extra` involved) measured **3247891** for [numpy] and failed; three other whole-session
  runs of the same suite on the same machine passed at ~1.23-1.33M.
- CI shows the same two-valued behaviour: `test macos-latest py3.11` measured 3152554 and 3152303
  on two runs of one branch while every other matrix leg, and `main` itself, stayed under the
  ceiling.

The other four assertions in the test — `peak_join_bytes <= budget`, spill engaged, the
`n_left*n_right` row count, and the oracle comparison — are counters and pass every time, including
in every failing run. They are the gate; this one is a corroboration that cannot hold its own
ceiling.

## Proposed correction

Drop the `tracemalloc` assertion, or raise it to a ceiling that bounds a full-materialisation
implementation rather than the observed happy path (the observed two states differ by ~1.9 MB while
the gap between the passing peak and the ceiling is ~0.6 MB). The kernel counter
`w.peak_join_bytes <= budget` already discriminates the full-RAM `concat` the corroboration was
added to catch.

## Status

Not routed around: the test is unmodified, its ceiling is unmodified, and no gate was relaxed. The
branch `ci/per-file-coverage-policy` leaves the failure visible in CI.

## Resolution (owner ruling, 2026-09-22)

Ruling: drop the `tracemalloc` assertion if the kernel counter already discriminates. Measured
against four kernel mutants in `_join_with_budget` (both adapters; the counters are the other four
assertions):

| mutant | `peak_join_bytes <= budget` | spill > 0 | rows + oracle | tracemalloc ceiling 1920000 |
|---|---|---|---|---|
| full-RAM concat (budget ignored) | FAIL (963200) | FAIL | pass | FAIL (3.45 M) |
| no `del` after each spill | pass | pass | pass | pass (1.35–1.48 M) |
| every joined chunk hoarded beside the read-back | pass | pass | pass | FAIL (2.27 M) |
| every wire hoarded beside the read-back | pass | pass | pass | FAIL (2.28 M) |

The counter refuses the full-RAM concat the test targets. The two hoarding mutants are refused only
by the tracemalloc line, and their excess (about 1.0 MB, one extra output copy) is smaller than the
session-state excess the line flakes on (about 1.9 MB), so no ceiling separates a hoarding kernel
from a noisy session. Dropped, not raised: the frozen gate no longer refuses a kernel that keeps an
extra copy of the output resident. The m41 sibling (`test_fetch_spill.py`, warm-up guarded) has not
flaked and is untouched. No `freeze-M40-*` tag exists on origin; the amendment is this commit.
