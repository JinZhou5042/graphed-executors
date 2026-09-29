# Dispute: tests/frozen/m66/test_htcondor_packaging_pins.py::test_coveragerc_htcondor_gates_exactly_the_backend

## The test
`sources == {"graphed_executors.htcondor_backend"}` for `.coveragerc-htcondor`'s `[run] source` (m66 freeze).

## The clause it contradicts
plan-services.md §6 (executors `test-htcondor`): "`.coveragerc-htcondor` sources add
`graphed_executors.submit.services`". The m68a freeze pins the same fact:
`tests/frozen/m68a/test_services_packaging.py::test_the_htcondor_coverage_sources_include_the_service_set`
(`"graphed_executors.submit.services" in sources`). Both frozen tests read the same file; no
`.coveragerc-htcondor` satisfies both.

## What the implementation does
Follows the plan (§6) and the newer m68a freeze: the sources are `graphed_executors.htcondor_backend`
and `graphed_executors.submit.services`. The m66 leg is red (in `pytest tests/frozen` on every job
that collects `tests/frozen/m66`), nothing else in that file is.

## Proposed fix
Amend the m66 pin (an `-2`-style amendment of the m66 freeze, as the plan's freeze-amendment
precedent does) to `{"graphed_executors.htcondor_backend"} <= sources` with the extra sources limited
to what later milestones' plans name, e.g. `sources - {"graphed_executors.htcondor_backend"} <=
{"graphed_executors.submit.services"}`; keep the branch/parallel/sigterm/fail_under asserts.

## Resolution (2026-09-28)
Owner ruling: amend the m66 pin as proposed. The backend must be a source, and only
`graphed_executors.submit.services` may join it. The branch/parallel/sigterm/fail_under asserts are unchanged. Committed as
`test(m66): allow the service set in the htcondor coverage sources`, sanctioned with `precommit --allow-refreeze
tests/frozen/m66`, and re-tagged `freeze-m66-2` (annotated, local; its sha is recorded in attempts.md if the proxy refuses the tag).
