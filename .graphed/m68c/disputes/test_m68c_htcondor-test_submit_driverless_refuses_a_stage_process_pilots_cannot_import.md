# Dispute: tests/frozen/m68c/test_m68c_htcondor.py::test_submit_driverless_refuses_a_stage_process_pilots_cannot_import

## The test
It requires the HTCondor path to refuse, before any submit, a join plan whose fold stage holds a
lambda `combine` (`_lambda_combine_plan`), with a ValueError naming `stages[i].process`.

## The clause it contradicts
Owner ruling 2026-10-01: "lambdas should be accepted for joins as a convenience (can't you use
cloudpickle for serialization?)". A join plan's stage process travels as its OpSpec, which is
cloudpickled by value; `SubmitRunner` broadcasts it with cloudpickle (executors 6c3fe3c), pilots load
it with `pickle.loads`, and `submit_driverless` writes the plan (its OpSpecs included) to
`plan.pkl`, which the driver job loads. Only the HTCondor pre-check refuses it.

## Proposed correction
The refusal becomes acceptance:
- `htcondor_runner` runs a lambda-`combine` join plan over pool pilots, and its value equals
  `SequentialRunner`'s.
- `submit_driverless` submits the same plan, and its `plan.pkl` loaded in a fresh process runs to
  the in-process value.

## Owner ruling 2026-10-01
Approved: lambdas are accepted for join plans on HTCondor ("lambdas should be accepted for joins as a
convenience"). Refreeze these tests to the acceptance form above under tag `freeze-m68c-fixup`.
