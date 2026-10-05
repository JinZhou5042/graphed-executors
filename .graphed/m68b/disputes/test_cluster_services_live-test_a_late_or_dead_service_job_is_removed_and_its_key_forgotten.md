# Dispute: tests/frozen/m68b/test_cluster_services_live.py::test_a_late_or_dead_service_job_is_removed_and_its_key_forgotten (gpus=2 leg)

## The test
A service requesting `gpus=2` on a pool with no such slot is expected to raise a `TimeoutError` after
`timeout_s=20` naming `JobStatus 1`, the job removed afterwards.

## The clause it contradicts
Owner ruling 2026-10-01 (placement): a service is refused only when no machine available to the scheduler
can ever run it; a schedulable one is waited for. The owner-approved design (plan-services §5.2
"Schedulability", b458f5a) submits the job, matches its own ad against the pool's Machine ads with the
partitionable totals substituted, and removes it at once when none matches, raising `ServiceUnavailable`.

## Correction
The leg expects `ServiceUnavailable` naming `RequestGPUs 2` and the pool's largest slot memory, the job gone
from the queue (polled) and its history since the test began showing `NumJobStarts 0`; the dead-job leg is
unchanged.

## Owner ruling 2026-10-01
Approved: "m68b refreeze approved"; applied by the lead on the owner's instruction "for fixup3 do (c)".
Tag `freeze-m68b-fixup3`.
