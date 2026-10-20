# Dispute: tests/frozen/m66/test_htcondor_packaging_pins.py::test_coveragerc_htcondor_gates_exactly_the_backend

Status: CLOSED — owner refreeze ruling 2026-10-20, refrozen in `test(frozen): .coveragerc-htcondor measures directories, not dotted names`, tag freeze-m66-fixup (graphed-executors#71).

## The test
It pinned `.coveragerc-htcondor`'s `[run] source` to the dotted names `graphed_executors.htcondor_backend` and `graphed_executors.submit.services`.

## The clause it contradicts
The test's subject is that the job gates exactly the backend and the service set. A dotted source is how the measurement goes wrong: coverage resolves `graphed_executors.submit.services` with `find_spec`, which imports `graphed_executors.submit` and everything it imports. Coverage does this inside `sys_modules_saved()` and then evicts those modules from `sys.modules`, so a later local process pool fails to pickle `_process_worker` ("not the same object", #71; upstream coveragepy#2324, closed as not planned).

## Correction
`[run] source` lists the backend and `submit/` directories under `${GRAPHED_EXECUTORS_SRC-src}` (CI sets an absolute path, so pilots and services in other directories still measure), and `[run] omit` drops the other `submit/` modules. The test expands the sources with coverage's own `substitute_variables`, requires each to be a directory, and asserts the measured file set is exactly `htcondor_backend/**` plus `submit/services.py`.

## Ruling
Owner, 2026-10-20: fix #71 with path-form sources; the pin keeps its intent of gating exactly the backend.
