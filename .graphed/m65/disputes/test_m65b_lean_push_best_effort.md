# Dispute record: the best-effort hub routes can deliver no terminal at all

**Artifact:** `tests/frozen/m65/test_m65b_lean_push.py::test_lean_routes_emit_submitted_and_terminal_only`,
the `BEST_EFFORT` routes (`proc-hub`, `proc-pooled`, `proc-adaptive`): `assert 1 <= len(keys) <= mp.N`
(the m37 form the r10 owner decision (1) chose for the hub route's non-push leg).

**Gap:** on Windows py3.13 (graphed-executors run 35982523140, `[proc-pooled]`) the run delivered no
terminal event: `assert 1 <= 0`. A non-persistent hub pool ships events on the `_PROC_DRAIN_INTERVAL`
tick and can lose the last batch in the worker's exit flush (plan-C C-8's measured
`probes/cu2_trailing.out`), so on a slow spawn the whole 16-task run is one lost batch. The other
Windows and every other OS leg passed on the same run; the loss is the route's documented best-effort
delivery, not B's push or lean paths.

**Why not routed around:** the lean witnesses stay: no `STARTED` on any route, empty `partition` on
every terminal, a label on every `SUBMITTED`, and the exact key set on the `EXACT` routes. Only the
lower bound on a best-effort count is dropped; completeness on the hub route is plan-C C-8's opt-in.

**Proposed correction (`--allow-refreeze tests/frozen/m65`):** `1 <= len(keys) <= mp.N` becomes
`len(keys) <= mp.N`; the subset check on the next line stays. Tag `freeze-m65b-fixup`.

**Disposition:** RULED — owner, 2026-09-24: "Refreeze the flakey windows tests with looser bounds".
Recorded by the lane lead (team-lead), 2026-09-24.
