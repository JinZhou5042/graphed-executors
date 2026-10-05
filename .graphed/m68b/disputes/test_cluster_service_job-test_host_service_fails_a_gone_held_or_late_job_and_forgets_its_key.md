# Dispute: tests/frozen/m68b/test_cluster_service_job.py::test_host_service_fails_a_gone_held_or_late_job_and_forgets_its_key[idle,spooling]

## The test
The fake queue answers `JobStatus 1` (idle) or `5`/`HoldReasonCode 16` (spooling) forever, and the test
expects `host_service` to raise a `TimeoutError` after `timeout_s=1.5` naming that status.

## The clause it contradicts
Owner ruling 2026-10-01 on placement: "'available' has to do with the machines available to the
(underlying) scheduler. the question is 'can the histogram server be scheduled?' and, if so, the plan can
proceed and waits on that server to be available to run." A schedulable service job that is idle or
spooling is waited for; `timeout_s` counts from the job's first `JobStatus 2` (plan-services §5.2
"Schedulability"). Under that rule the case as frozen never times out.

## Correction
The queue answers idle or spooling, then running; the `TimeoutError` names `JobStatus 2`, and a spy on
`schedd.query` asserts it is raised at least `timeout_s` after the first running answer. On main's product
both cases fail that assertion (raised 0.5 s after the first running answer).

## Owner ruling 2026-10-01
Approved: "m68b refreeze approved" (scope: these two cases and the gpus=2 live leg); applied by the lead on
the owner's instruction "for fixup3 do (c)". Tag `freeze-m68b-fixup3`.
