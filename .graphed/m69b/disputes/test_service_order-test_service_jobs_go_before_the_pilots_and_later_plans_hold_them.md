# Dispute: tests/frozen/m69b/test_service_order.py::test_service_jobs_go_before_the_pilots_and_later_plans_hold_them

Status: CLOSED — owner ruling 2026-10-02: "approve the freeze-m69b-fixup2 refreeze, go" (freeze-m69b-fixup2)

## The test
For each later plan it asserts `hold.t < services[0].t` and `release.t > of_kind(later, "announce")[-1].t`, where
`t` is the `time.monotonic()` stamp `OrderSchedd._stamp` takes under its lock as each action happens.

## The clause it contradicts
plan-services §5.2 "Ordering" asks that the hold come before the plan's first service submit and the release after
its last announce: an order, which the harness already records as the position in `schedd.events` (appended under
the same lock). On Windows `time.monotonic()` advances in ~15.6 ms steps, so two actions in one step get equal
stamps and the strict comparison fails although the order is right. CI run 37030897685 (head 67ba8f2), both
Windows legs: `Hold` at t=1295.671 then `submit-service` at t=1295.671 (py3.12), 1184.671 / 1184.671 (py3.11);
the release and the announce also tie (1297.671). macOS and Linux pass.

## Correction
Compare positions in `later`, the order the harness recorded:

```diff
-            assert hold.t < services[0].t, f"plan {i} held its pilots after its service submit: {later}"
-            assert release.t > of_kind(later, "announce")[-1].t, (
+            assert later.index(hold) < later.index(services[0]), f"plan {i} held its pilots after its service submit: {later}"
+            assert later.index(release) > later.index(of_kind(later, "announce")[-1]), (
```

No other frozen comparison of these stamps is strict (`test_the_first_need_waits_for_pilots_past_a_service_s_timeout`
uses `>=` and a duration).
