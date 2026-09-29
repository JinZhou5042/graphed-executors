# m68a blockers (upstream, not disputes)

## graphed's resolve walk is not merged — RESOLVED
- **Resolved (2026-09-28):** graphed-org/graphed PR #63 (branch `resolve-walk`, head `f69dec1`) adds
  `graphed.services.resolve_services`/`Resolvable` and the composites' forwarding. `ci.yml`'s `GRAPHED` pin is at
  that head (to move to its merge commit, then the release). The engine calls `graphed.services.resolve_services(bound,
  value)` directly; the fallback `_resolved_value` and its extra test are deleted. The two frozen legs
  (`test_resolve_reaches_a_collated_part`, `test_resolve_reaches_an_aggregate_plan_reduce`) pass.

History (as recorded before #63):
- Plan: `plan-services.md` §3.2 "The resolve walk" (a graphed PR "stacked on main before the release executors floor
  at") and §7 ("graphed m68 PR and the resolve walk before executors m68a").
- Observed (2026-09-28): `graphed-org/graphed` main `6e9e55e` has `services.py` with `ServiceSpec`, `Launch`,
  `UnboundService`, `split_endpoint`, `Bindable`, `referenced_services`, `bind_externals`, `require_bound`,
  `bind_services`, but no `resolve_services` or `Resolvable`. No branch carries `def resolve_services`, and there is
  no `freeze-preserve-m68-3` tag.
- Effect on m68a: in `test_services_protocol.py`, the legs where a `Resolvable` spy sits under `collate({...})` or as
  `aggregate_plan`'s `reduce` need graphed's composites to forward `resolve_services`. The engine cannot reach
  those spies without reaching into graphed's private composites, which the plan's design excludes
  (`probes/class/probe_resolve_traversal_r16.txt`: the engine's `getattr(plan.process, "resolve_services")` finds no
  hook on either).
- Unblocks when: graphed merges the resolve walk and `ci.yml`'s `GRAPHED` pin moves to that commit. Then one executors
  change follows: delete the fallback `_resolved_value` in `submit/engine.py` (its `getattr` of
  `graphed.services.resolve_services` and of the process's hook) and call
  `graphed.services.resolve_services(bound, value)` directly in `SubmitRunner._run_scoped`, with its extra test.

## Release gate: the graphed floor
- `pyproject.toml` floors `graphed>=0.0.6`, and released 0.0.6 has no `graphed.services`; m68a hard-imports it from
  `local/executors.py`, `submit/engine.py`, `submit/services.py`, `submit/recipes.py`, `htcondor_backend/` and the dask
  and parsl `transport_peer.py`. CI installs graphed by ref (`env.GRAPHED`, now graphed#63's merge commit d0ad16b on main), so the suite runs.
- Gate: **no graphed-executors release before the floor moves to the graphed release that carries `graphed.services`
  and the resolve walk** (plan §7: "floored at the release that holds both"). The floor is deliberately unchanged
  until that release exists.
