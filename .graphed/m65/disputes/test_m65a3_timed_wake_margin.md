# Dispute record: the resume-refill timing bound flakes on loaded CI runners

**Artifact:** `tests/frozen/m65/test_m65a3_control_submit.py::test_timed_wake_sees_resume_and_new_slots`,
its second phase: after `ctl.resume()` the first refilled slot's `STARTED` must arrive within 0.2 s
(`min(later) - resumed < 0.2`). The A2 extra test `tests/extra/m65/test_a2_hub_window.py::
test_resume_refills_while_a_task_is_still_running` carries the same bound on the thread hub.

**Gap:** the refill comes from the `_PAUSED_WAKE_S` (50 ms) timed wake, so the bound is 4× the
cadence. On macOS runners it measured 0.2106 s and 0.2104 s (graphed-executors run 35956197050, py3.12
extra / py3.13 frozen) and 0.21 s again on run 35982523140 (py3.11 extra); main's own dispatch
35951784489 flaked the extra test the same way. Those suites took 700 s where ubuntu takes 320 s.

**Why not routed around:** the bound is the test's discriminator: the alternative refill, key 0's
finish, lands about 0.7 s after the resume (key 0 sleeps 1 s from its start; the resume follows the
first finish by 0.2 s), so any bound below that still proves the timed wake did the refill. The
mechanism is unchanged; only the margin against runner load is.

**Proposed correction (`--allow-refreeze tests/frozen/m65`):** `< 0.2` becomes `< 0.5` in the frozen
test and in the extra test. Tag `freeze-m65a3-fixup2` (precedent: `freeze-m65a3-fixup`).

**Disposition:** RULED — owner, 2026-09-24: "Refreeze the flakey windows tests with looser bounds".
Recorded by the lane lead (team-lead), 2026-09-24.
