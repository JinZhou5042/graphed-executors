# Dispute: tests/frozen/m68b/test_cluster_service_job.py::test_host_service_forgets_its_announce_secret_when_construction_refuses

Status: CLOSED — owner ruling 2026-10-02: "m69b refreeze authorized" (freeze-m68b-fixup4)

## The test
After `host_service` refuses a spec whose input is missing, it asserts `len(submits(schedd.log)) == 1`: the
one submit being the pilots' at the backend's construction, so no service job was submitted.

## The clause it contradicts
plan-services §5.2 "Ordering" (owner ruling 2026-10-01: a run's service jobs are submitted before its pilots):
the backend under test (`CondorPilots` on the generic profile, which offers `"cluster"`) submits no pilots at
construction, only at its first need of a worker. The log then holds no submit at all, and the count of one
fails although no service job was submitted.

## Correction
Assert the property the message names, independent of when the pilots are submitted:

```diff
-        assert len(submits(schedd.log)) == 1, "a service job was submitted for a refused input"
+        services = [d for d in submits(schedd.log) if str(d["JobBatchName"]).startswith("graphed-service-")]
+        assert services == [], "a service job was submitted for a refused input"
```
