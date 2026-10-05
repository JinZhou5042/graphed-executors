# Dispute: tests/frozen/m68b/test_cluster_service_job.py::test_host_service_returns_the_announced_endpoint_and_releases_in_order[http:/-http,tcp-tcp,grpc:-grpc]

Status: CLOSED — owner ruling 2026-10-02: "m69b refreeze authorized" (freeze-m68b-fixup4)

## The test
It unpacks `pilot_desc, service_desc = submits(schedd.log)[:2]`, the first submit being the pilots' at the
backend's construction, and asserts `cfg["url"] == pilot_desc["arguments"].split()[0]`.

## The clause it contradicts
plan-services §5.2 "Ordering" (owner ruling 2026-10-01: a run's service jobs are submitted before its pilots):
the backend under test (`CondorPilots` on the generic profile, which offers `"cluster"`) submits no pilots at
construction, only at its first need of a worker. A direct `host_service` call needs no worker, so the log
holds the service job's submit alone and the two-value unpack fails.

## Correction
The service job's submit is the only one; the url check goes, since the 200 announce the test posts to
`cfg["url"]` already shows it is the task server's. The leak check still reads `service_desc` against both
secrets (`prepare` writes the pilots' secret at construction):

```diff
-        pilot_desc, service_desc = submits(schedd.log)[:2]
-        assert cfg["url"] == pilot_desc["arguments"].split()[0]
+        (service_desc,) = submits(schedd.log)
```
