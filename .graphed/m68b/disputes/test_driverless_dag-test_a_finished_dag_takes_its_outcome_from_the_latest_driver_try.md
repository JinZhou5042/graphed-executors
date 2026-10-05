# Dispute: tests/frozen/m68b/test_driverless_dag.py::test_a_finished_dag_takes_its_outcome_from_the_latest_driver_try

## The test
Every parametrization asserts that the driver-node `history` call is logged with `match=3`
(`e[3] == 3`), following plan-services.md's B2 row and its `RunHandle(dag=True)` clause
("highest `ClusterId` among `schedd.history(<driver-node constraint>, [...], match=3)`").

## The clause it contradicts
The rule `RunHandle._poll` states and the plan follows (`schedd.history(..., match=1)`): a history read
is bounded by `match`, since "without a match bound the schedd scans its whole history". On bigbird26 a
whole-history scan does not return.
`match=3` is reachable only when the driver ran three times. A DAG whose driver succeeded on its first
try has one driver ad, so `status()`, `wait()` and `result()` never return on lxplus.

Measured on lxplus (lanes/htcondor/probes/site-lxplus/m68b-dag.txt, "attempt 2"): DAG 12790659 finished
DAG_STATUS_OK at 00:24:56Z with one driver try (12790661, ExitCode 0). `RunHandle.wait()` was still blocked
12 min later. `status()` as shipped: the DAGMan history read (`match=1`) returned in 0.2 s, then the
driver-node read (`match=3`) was killed by `timeout 120` (rc 124). The same handle with only that `match`
rewritten to 1: the driver-node read returned 1 ad in 0.2 s, `wait()` gave `done`, and `result()` was
bit-for-bit.

## Proposed correction
- Product: `match=1` in `RunHandle._dag_status`. History is read newest first, and DAGMan submits a
  retry only after the previous try has left the queue, so the first driver ad found is the latest try.
- Frozen pin: `e[3] == 1`. The recorded-order cases still pass, because the fake returns every ad
  whatever the `match`.
- Plan: change the B2/`RunHandle(dag=True)` text from `match=3` to `match=1`, with that reason.

## Owner ruling 2026-10-01
Approved as proposed (owner, asked what lxplus still needed: "you can do this yourself"): the
product reads the driver node's history with `match=1` and the frozen pin becomes `e[3] == 1`.
