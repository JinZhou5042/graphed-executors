# Dispute: tests/frozen/m68c/test_m68c_htcondor.py::_FakeHTCondor

## The test
`_FakeHTCondor` has no `RemoteParam`. On main with #41 merged (8c299f7),
`test_submit_driverless_derives_a_join_plan_s_service_nodes` fails with
`AttributeError: '_FakeHTCondor' object has no attribute 'RemoteParam'` at `submit_driverless`'s DAG
branch; it is the only failure across tests/frozen/m66, m67, m68b and m68c (merge-group run 36870516270
ejected #42).

## The clause it contradicts
plan-services.md §3.3 (DAG driverless) as fixed by #41: the DAGMan executable is the schedd's, read
with `RemoteParam` on the chosen schedd's ad (the m68b_dag_harness dispute). m68c froze before #41
merged, so its fake predates that call. Its description sends no credential, so `Credd` is not reached.

## Proposed fix
The fake gains `RemoteParam(location) -> {"BIN": "/usr/bin"}`, as the m68b DAG fake did. With only
that change, a copy of the file passes and the frozen file fails, in one run.

## Owner ruling 2026-10-01
Refreeze approved as proposed: the fake gains `RemoteParam`; tag freeze-m68c-fixup2.
