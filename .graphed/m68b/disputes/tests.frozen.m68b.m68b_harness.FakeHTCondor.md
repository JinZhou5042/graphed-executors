# Dispute: tests/frozen/m68b/m68b_harness.py::FakeHTCondor

## The tests
`FakeHTCondor` has no `Credd`/`CredType`. These fail with the fix (`AttributeError: 'FakeHTCondor'
object has no attribute 'Credd'`): `test_cluster_service_job.py::test_the_interpreter_image_and_env_per_recipe[lxplus-imageless]`,
`[lxplus-imaged]`, `::test_send_credential_is_dropped_on_an_attached_job_and_kept_in_watch_mode` and
`::test_stop_removes_at_once_and_unlinks_only_the_job_s_secret[spooled-completed]`.

## The clause it contradicts
plan.md §2, the `lxplus` row: submit keys include `MY.SendCredential=True`, and its pilots must start.
When the credd holds no Kerberos credential, a `SendCredential` job submitted through the bindings never
starts (`ReconnectFailed` ×8, `NumJobStarts` 0 over 10 min). `condor_submit` stores a credential
through `SEC_CREDENTIAL_PRODUCER`, and `schedd.submit` does not. After storing one, the same submit ran
at first match (submit-fixes-diagnosis.md §2.2). The fix asks the credd before each such submit and
stores a credential when it holds none. Any fake without a credd fails on that call.

## Proposed fix
lanes/htcondor/probes/site-lxplus/submit-fixes/proposed-frozen-fixup.diff: `CredType.Kerberos` and a
`Credd()` whose `query_user_cred` returns a timestamp, i.e. a stored credential. The submit path
then records exactly what it records today.

## Owner ruling 2026-09-30
Refreeze granted as proposed: the harness fakes gain `Credd`/`CredType` (and the m68b DAG fake `RemoteParam`), and the two `DAG_OPTIONS` pins require `{**DAG_OPTIONS, "dagman": "/usr/bin/condor_dagman"}`; tags freeze-m67-fixup2, freeze-m68b-fixup2.
