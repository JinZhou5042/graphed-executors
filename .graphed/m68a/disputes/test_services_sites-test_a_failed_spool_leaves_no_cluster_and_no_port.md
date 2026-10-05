# Dispute: tests/frozen/m68a/test_services_sites.py::test_a_failed_spool_leaves_no_cluster_and_no_port

Status: CLOSED — owner ruling 2026-10-02: "m69b refreeze authorized" (freeze-m68a-fixup2)

## The test
`HTCondorBackend(pilots, 2, host="127.0.0.1", port_range=(port, port))` over a spooling `CondorPilots` whose
spool raises is expected to raise `RuntimeError` matching `spool` at construction, its cluster removed and the
port freed.

## The clause it contradicts
plan-services §5.2 "Ordering" (owner ruling 2026-10-01: a run's service jobs are submitted before its pilots):
a backend whose pilots and services are both jobs (`CondorPilots`, `"cluster"` among the narrowed hosts) only
prepares at construction and submits its pilots at its first need of a worker. The generic profile offers
`"cluster"`, so construction no longer submits and the spool never runs there: the `pytest.raises` sees
nothing raised.

## Correction
The constructor call gains `service_hosts=("driver",)`, so this backend's services are not jobs and its pilots
are still submitted (and spooled) at construction:

```diff
-            lambda: backend_api().HTCondorBackend(pilots, 2, host="127.0.0.1", port_range=(port, port)),
+            lambda: backend_api().HTCondorBackend(
+                pilots, 2, host="127.0.0.1", port_range=(port, port), service_hosts=("driver",)
+            ),
```

The deferred spool failure is the m69b row
`tests/frozen/m69b/test_service_order.py::test_a_deferred_spool_failure_removes_its_cluster_and_close_drops_the_secret`.
