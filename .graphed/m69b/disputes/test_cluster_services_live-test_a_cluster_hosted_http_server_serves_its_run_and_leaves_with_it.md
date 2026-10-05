# Dispute: tests/frozen/m68b/test_cluster_services_live.py::test_a_cluster_hosted_http_server_serves_its_run_and_leaves_with_it

Status: WITHDRAWN — the contradicting plan sentence (71a5deb, an `HTCondorRunner` runs one plan at a time) was the
driver's, not B1's; plan/m68 ba30c9c replaced it with the correction below (a backend starts one service set at a
time, through its probe). No frozen change is needed.

## The test
On the pool, `held = runner.submit(<gated plan>)` keeps its task waiting on a gate, then
`direct = runner.run(...)` must finish while `held` is still in flight, and `queued(schedd, batch(scope_held))`
must still be non-empty after it ("the direct run's end removed the submitted run's service"); only then is the
gate opened. The runner is `htcondor_runner(...)`, an `HTCondorRunner`.

## The clauses
- It implements plan-services B1 (`test_cluster_services_live.py`, minicondor): "a queued `submit` overlapping a
  direct `run` gets two service clusters, and each run's end removes only its own".
- It contradicts §5.2 as amended in 71a5deb: "An `HTCondorRunner` runs one plan at a time (`run`, which `submit`'s
  driver thread also calls, takes a runner lock …)". Under that lock, `direct` waits for `held`, which waits for a
  gate the test opens only after `direct` returns.

## Measured
In the pool container (`m68b-minicondor`, one 15973 MiB slot), 7e6e9c5's tree plus `cut_one_run_exec_rv7.py`:
the row fails after 350.5 s with `AssertionError: the direct run's end removed the submitted run's service`
(`GatedGet`'s 300 s deadline released `held` first). The queue was empty afterwards. Without the cut the row
passes (the full test-htcondor line at 1b3ba0f, the same src: 540 passed, 10 skipped).

## Proposed correction (no frozen change)
Serialize each set's start through its probe, not the whole run: an `HTCondorBackend` lock that
`ServiceSet.start` takes (duck-typed, as `starting_services`) around the resolve phase and `_probe`. A set
holding it waits only for slots and workers that already-started plans release without it, so no cycle forms,
and a direct `run` still overlaps a submitted plan whose services have started.
Prototype (`cut_one_start_probe.py`: one lock attribute and four indented lines in `ServiceSet.start`), same pool:
- r7's `probe_cross_plan_exec_rv7.py`, concurrent leg: plan x ends at 27.2 s, plan y at 37.7 s, with values; the
  queue is empty.
- frozen m68b `test_cluster_services_live.py`, frozen m68a `test_services_protocol.py`, frozen m69b
  `test_service_order.py` and `tests/extra/m69b`: 113 passed, 3 skipped, exit 0. The queue is empty.

The other way out is to keep 71a5deb's run lock and refreeze this row's overlap leg, amending B1.
