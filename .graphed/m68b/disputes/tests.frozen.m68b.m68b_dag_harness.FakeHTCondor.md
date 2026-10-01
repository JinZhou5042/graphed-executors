# Dispute: tests/frozen/m68b/m68b_dag_harness.py::FakeHTCondor and two DAG_OPTIONS pins

## The tests
- `FakeHTCondor` (the DAG recorder) has no `RemoteParam` and no `Credd`/`CredType`.
- `test_driverless_dag.py::test_service_nodes_are_named_by_derived_ids_in_name_order` and
  `::test_an_lxplus_service_node_keeps_its_credential_and_env_and_the_dag_is_not_spooled` pin
  `from_dag`'s options to `DAG_OPTIONS` exactly.

Failing with the fix, each `AttributeError` on the fake:
`test_service_nodes_are_named_by_derived_ids_in_name_order`,
`test_an_lxplus_service_node_keeps_its_credential_and_env_and_the_dag_is_not_spooled`,
`test_a_user_module_path_holding_a_comma_is_refused[dag]`, `test_each_dag_submission_gets_a_new_run_directory`,
`test_paths_under_job_root_submit`, `test_the_job_root_check_is_lexical`, and in the test-htcondor job
`test_the_dag_description_is_the_probe_s` (`RemoteParam`);
`test_self_submission_reads_job_root_from_the_row` (`Credd`).

## The clause it contradicts
plan-services.md §3.3 (DAG driverless): the lxplus DAGMan executable is `/usr/bin/condor_dagman`, the
schedd's. Stock `Submit.from_dag` instead writes the `condor_dagman` on the caller's `PATH`
(`/usr/local/bin/condor_dagman` in the coffea image). bigbird26 holds that DAGMan (6/13), and
`submit_driverless` returns no error (lanes/htcondor/probes/site-lxplus/m68b-dag.txt,
submit-fixes-diagnosis.md §1). The fix passes `dagman=<schedd BIN>/condor_dagman`, reading `BIN` with
`RemoteParam` on the chosen schedd's ad, so `from_dag`'s options can no longer equal `DAG_OPTIONS`.
Storing the Kerberos credential before a `SendCredential` submit (see the m68b_harness dispute) needs
the fake's credd.

## Proposed fix
lanes/htcondor/probes/site-lxplus/submit-fixes/proposed-frozen-fixup.diff:
- the fake gains `RemoteParam(location) -> {"BIN": "/usr/bin"}` (the CI pool's layout, the fixture's
  `condor_dagman`), plus `CredType.Kerberos` and a `Credd()` whose `query_user_cred` returns a timestamp
  (a stored credential);
- the two pins become `{**DAG_OPTIONS, "dagman": "/usr/bin/condor_dagman"}`.

## Owner ruling 2026-09-30
Refreeze granted as proposed: the harness fakes gain `Credd`/`CredType` (and the m68b DAG fake `RemoteParam`), and the two `DAG_OPTIONS` pins require `{**DAG_OPTIONS, "dagman": "/usr/bin/condor_dagman"}`; tags freeze-m67-fixup2, freeze-m68b-fixup2.
