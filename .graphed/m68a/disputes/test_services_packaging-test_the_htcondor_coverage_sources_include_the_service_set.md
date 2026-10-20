# Dispute: tests/frozen/m68a/test_services_packaging.py::test_the_htcondor_coverage_sources_include_the_service_set

Status: CLOSED — owner refreeze ruling 2026-10-20, refrozen in `test(frozen): .coveragerc-htcondor measures directories, not dotted names`, tag freeze-m68a-fixup3 (graphed-executors#71).

## The test
It required the dotted names `graphed_executors.submit.services` and `graphed_executors.htcondor_backend` in `.coveragerc-htcondor`'s `[run] source`.

## The clause it contradicts
Its subject is that the service set is measured with the backend. The dotted form makes coverage import and then evict `graphed_executors.submit`'s import graph (see the m66 dispute on the same file, #71).

## Correction
The test now requires `src/graphed_executors/submit` and `src/graphed_executors/htcondor_backend` among the directory sources (expanded with coverage's `substitute_variables`), and `submit/services.py` outside `[run] omit`.

## Ruling
Owner, 2026-10-20: the refreeze #71 needs is authorized.
