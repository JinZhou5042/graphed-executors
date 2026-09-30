# Test dispute: tests/frozen/m68a/services_harness.py::run_bounded (also tests/frozen/m67/driverless_harness.py::run_bounded)

## The test code
```python
def run_bounded(fn, timeout_s=RUN_TIMEOUT_S):
    pool = ThreadPool(1)
    try:
        return pool.apply_async(fn).get(timeout_s)
    ...
    finally:
        pool.close()          # never joined; the pool is dropped with its handler threads still exiting
```

## The clause it contradicts
Its own contract ("a hang is a failure, never a wedged CI job") and the TEST_SANITY / §B.3 determinism
requirement: on CPython 3.14t the harness itself deadlocks the test process, independent of the code under test.

## Mechanism (measured)
A dropped ThreadPool's `multiprocessing.util.Finalize` (-> `Pool._terminate_pool`, which joins the pool's
handler threads) runs, on 3.14t, at a deferred point on the main thread. When that point is inside the next
`ThreadPool(1)`'s `threading.Thread.start` (`with _active_limbo_lock: _limbo[self] = self`), the main thread
holds `_active_limbo_lock` and joins a handler thread that is blocked in `Thread._delete` acquiring that
same lock. The constructor sits outside the `.get(timeout_s)` bound, so nothing ends the wait.
Stdlib-only reproduction: 8 of 8 runs wedge within 447-14410 calls on macOS 3.14.6t. On Linux 3.14.7t with
--cpus 2, 2 of 6 runs wedge. GIL 3.14 and 3.13t do not wedge in 50k calls.

## Proposed correction
Drop the ThreadPool and bound the call the way tests/frozen/m44/transport_harness.py and
tests/frozen/m66/htcondor_harness.py already do: a daemon `threading.Thread` plus `join(timeout_s)`.
Alternatively, `pool.join()` after `close()` on the non-timeout path. The same change applies to
tests/frozen/m67/driverless_harness.py::run_bounded. Both corrections, swapped into the same stdlib loop on
macOS 3.14.6t: 0 of 3 runs wedge in 50k calls each (the unmodified pattern: 3 of 3 wedge, at calls
20883, 2507 and 736). The thread form is the one m46/m47/m66 already use.

## Why not fixed outside tests/frozen
Nothing in `src/` creates these pools, and patching the harness from a non-frozen conftest would be
routing around a frozen file. Resolution needs an owner-sanctioned frozen fixup (as `freeze-m47-fixup`).

## Owner ruling 2026-09-30
Sanctioned fixup: every frozen `run_bounded` that pools uses the daemon-thread + `join(timeout)` form of `tests/frozen/m66/htcondor_harness.py::run_bounded`, in PR #40; tags freeze-m67-fixup, freeze-m68a-fixup, freeze-m68b-fixup.
Sites: m67 `driverless_harness.run_bounded`, m68a `services_harness.run_bounded`, m68b `m68b_harness.run_bounded` and `m68b_harness.in_background` (the same per-call pool), and m68b `m68b_dag_harness.run_bounded`.
