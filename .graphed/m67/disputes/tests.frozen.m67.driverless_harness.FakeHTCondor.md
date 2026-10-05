# Dispute: tests/frozen/m67/driverless_harness.py::FakeHTCondor

## The tests
`FakeHTCondor` has no `Credd`/`CredType`. These fail with the fix (`AttributeError`):
`test_driver_entry.py::test_local_pilots_run_the_plan_inside_the_slot` and
`test_driverless_payload.py::test_lxplus_description_ships_the_env_and_takes_extra_submit_last`.

## The clause it contradicts
The same as .graphed/m68b/disputes/tests.frozen.m68b.m68b_harness.FakeHTCondor.md. A `SendCredential`
job (the lxplus row, plan.md §2) submitted while the credd holds no Kerberos credential never starts.
The fix stores one before the submit, as `condor_submit` does, which needs a credd on the fake.

## Proposed fix
lanes/htcondor/probes/site-lxplus/submit-fixes/proposed-frozen-fixup.diff: `CredType.Kerberos` and a
`Credd()` whose `query_user_cred` returns a timestamp.

## Owner ruling 2026-09-30
Refreeze granted as proposed: the harness fakes gain `Credd`/`CredType` (and the m68b DAG fake `RemoteParam`), and the two `DAG_OPTIONS` pins require `{**DAG_OPTIONS, "dagman": "/usr/bin/condor_dagman"}`; tags freeze-m67-fixup2, freeze-m68b-fixup2.
