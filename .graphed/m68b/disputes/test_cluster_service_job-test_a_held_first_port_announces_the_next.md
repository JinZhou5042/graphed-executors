# Dispute: tests/frozen/m68b/test_cluster_service_job.py::test_a_held_first_port_announces_the_next

Status: CLOSED — owner refreeze ruling 2026-10-17, refrozen in `test(frozen): m68b holds its first port without a free-port race`, tag freeze-m68b-fixup5 (graphed-executors#54).

## The test
It checks a range free with `free_range(3)`, starts the task server, then binds a `wildcard_listener` on the range's first port. On the advisory 3.14t job of PR #51 (job 113524008602) that bind raised `OSError: [Errno 98] Address already in use` on port 35865: the range is drawn from 20000-50000, which overlaps Linux's ephemeral range, and the task server's own port-0 bind (or any other socket) can take the port after the check.

## The clause it contradicts
The test's subject is `announce.py`'s next-free-port choice; whether another socket takes the port in the gap between the harness's check and its own bind is not part of it.

## Correction
`m68b_harness.held_range(n)` checks a range with `free_range(n)` and holds its first port with `wildcard_listener`, taking a fresh range when that bind fails. The test enters `task_server()` first, so the server's port is taken before the range is checked, then `held_range(3)`. The assertion is unchanged.

## Ruling
Owner, 2026-10-17: the frozen harness holds the probed port or retries with a fresh one; the assertion stays.
