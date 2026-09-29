# m68a frozen suite — the engine service set, driver-hosted and site legs, driverless endpoints

This suite freezes the acceptance tests for executors m68a: `SubmitRunner` resolves each spec of
`plan.services` by three legs (a given endpoint, the site's, a managed start), checks it where it runs
and probes it from a worker, binds the endpoints into the plan with graphed's `bind_services`, resolves
the run's value while the services are up, and releases everything the submission acquired when `run`
returns or raises. The source of truth is `plan-services.md` on the plan branch (§3.1 is the m68a unit;
D1, D2, D4, D6, D7 and D10 are the binding decisions; §6 the CI lines), with the implementer constraints
of `reviews/m68a-exit-items-r8-r10.md` and r7's E1–E7. **Frozen — read-only after the m68a freeze tag.**

The positive leg: the generic `http_server` recipe is the third recipe every hosting test runs, beside Triton and histserv, and the one that runs the whole path in the frozen suites: `test_services_protocol.py` and `test_driverless_endpoints.py` here, `test_cluster_services_live.py` in m68b, and `test_dask_hosted_service.py` and `test_parsl_hosted_service.py` in m70.

## How to run it

`pytest tests/frozen/m68a`. Everything except the two gated files runs on every OS with the base
`.[dev]` install; the gRPC cases of `test_service_checks.py` and the `grpc:` case of
`test_services_sites.py` also need `grpcio` and `grpcio-health-checking` (they skip only on a
free-threaded interpreter, which has no grpcio wheel; wherever the GIL is enabled a missing package is a
failure). `test_scope_dask_memory.py` needs `distributed` (`importorskip`; the `test-dask` job lists it).
`test_services_live.py` needs, for its pool leg, the `htcondor2` bindings and a personal HTCondor (gated
exactly as `tests/frozen/m67/test_driverless_live.py`: skipped without the bindings, a failure without a
schedd), and, for its Triton legs, a Triton container serving `data/triton_models` on gRPC with
`recipes.triton`'s flags: those legs are skipped unless `GRAPHED_TRITON_GRPC=host:port` is set, and fail
when it is set and the server is not ready. **The `test-htcondor` CI step must export
`GRAPHED_TRITON_GRPC=localhost:8001`** (`test_services_packaging.py` checks that it does).

`data/triton_models/graphed_identity/` is a python-backend identity model with P9's name and I/O
(`INPUT0` → `OUTPUT0`, FP32). Its dims are `[-1, -1]` (P9's), not `[-1]`: graphed's Triton plugin sends
an `(n_events, n_features)` matrix, which a rank-1 input would reject.

## Files

| File | What it is |
|---|---|
| `services_harness.py` | Accessors for the planned names (`services_api()`, `recipes_api()`, `driver_api()`, …, looked up inside test bodies, so the suite collects before the implementation exists); `RecordingBackend` (submits, placement hints, the `RunContext` nonce of plan tasks, cancels by key, task starts, probe answers; an optional gate that holds its single worker; an optional submit fault); `HostFake` (a two-host `SubmitBackend`: driver on host A, `host_service` reporting host B, each probe task run as a worker on a chosen host through `$_CONDOR_MACHINE_AD`; `site_services`, `service_hosts`, `advertise_host`, `host_identity`; injectable faults); the spies `SpyProcess`/`SpyReduce`/`GetProcess` (`bind_services`/`resolve_services` recorded into one ordered trace); in-process HTTP/TCP/gRPC servers with accept counters and a first-bytes sniffing proxy; `pid_gone` (POSIX `os.kill(pid, 0)` → `ProcessLookupError`; Windows `GetExitCodeProcess`) and `port_free` (binds on loopback and all interfaces, and a connect is refused); the m67 bindings recorder and job-dir pieces, copied |
| `service_child.py` | The managed child the recipes in this suite run (`{python} service_child.py <mode> {port} <report>`): writes its pid and rendered `{python}` first, then serves HTTP answering its pid, a gRPC health server, or nothing |
| `m68a_triton.py` | graphed's `tests/frozen/preserve/m26/fake_triton.py` copied, with a connect log; an in-memory partitioned awkward source; the Triton `service=`/`url=` plans; the §3 `graphed_identity` plan for the live legs; an `aggregate_plan` whose `reduce` is a spy |
| `conftest.py` | A fresh trace per test; a teardown backstop that kills any managed child a failing implementation left running (the tests assert it is gone before this runs) |
| `data/triton_models/graphed_identity/` | The model repository the CI container serves on 8001 |

## Traceability

| Test | Plan clause |
|---|---|
| `test_services_protocol.py::test_http_server_recipe_runs_the_whole_path_without_a_service_client` | §3.1 row 1: the generic `recipes.http_server` requirement (`http:/`) runs resolve, bind, probe, a task GET, close; neither Triton nor histserv imported (`sys.modules` of a fresh interpreter) |
| `…::test_a_given_endpoint_wins_over_a_site_endpoint` | D2 order; row 1 "a given endpoint wins over a site endpoint" |
| `…::test_a_dead_given_endpoint_refuses_naming_user_with_zero_plan_submits` | D2 leg 1 "never silently replaced"; row 1 |
| `…::test_a_dead_site_endpoint_falls_through_to_managed` | D2 leg 2 "falls through and the status records it" |
| `…::test_no_launch_and_no_endpoint_names_all_three_legs` | D2 "Nothing left → `ServiceUnavailable(name)` naming the three legs" |
| `…::test_a_gpu_recipe_without_host_service_is_refused_naming_the_attribute` | D2/D10; §3.1 ServiceSet bullet (cluster-hosted iff `host_service`) |
| `…::test_an_injected_fault_surfaces_and_leaves_nothing[*]` | row 1 fault matrix (nine faults × `run`/`ServiceSet.start`): the injected exception itself, every managed pid reaped and port free with the runner open, `release_service` exactly for the returned keys; r20: the unanswered probe's `backend.cancel`; the raising release logged and a later release still run |
| `…::test_a_managed_child_lives_exactly_as_long_as_its_run` | row 1 "per run … gone when `run` returns, two runs start two pids" |
| `…::test_two_sequential_runs_on_the_fake_each_release_only_their_own_key` | D10 per-call key; row 1 |
| `…::test_overlapping_submissions_keep_their_own_scope` | §3.1 engine "a submission's scope is its run's `RunContext`"; row 1 overlap leg; r22 (nonces differ); r20 (A's probe cancelled, never runs) |
| `…::test_a_failed_leaf_submit_cancels_the_run_s_queued_leaves` | §3.1 engine (plan-task cancel on the run's stack); row 1; `probes/scope/probe_submission_scope.txt` P |
| `…::test_two_sets_entered_together_get_distinct_ports` | §3.1 port lock; row 1 Barrier leg (10 trials) |
| `…::test_a_warm_user_held_set_serves_two_plans_with_one_child` | §3.1 "Warm across plans"; row 1 warm leg; `probes/lifetime/probe_lifetime.txt` B |
| `…::test_bind_comes_after_the_probe_and_before_the_first_plan_task` | D2 "bind before the first plan-task submit"; row 1 |
| `…::test_resolve_services_runs_once_on_the_run_value_while_the_child_is_up` | §3.1 engine (`resolve_services(bound, value)` while up); row 1; r16 exit (the bound plan) |
| `…::test_resolve_reaches_a_collated_part` | row 1 `collate` leg; §3.2 resolve walk; `probes/class/probe_resolve_traversal_r16.txt` |
| `…::test_resolve_reaches_an_aggregate_plan_reduce` | row 1 `aggregate_plan` leg; §3.2 resolve walk |
| `…::test_an_unbound_plan_is_refused_before_any_process_call` | §3.1 engine (`require_bound` in the other runners); row 1 |
| `…::test_on_close_runs_before_teardown`, `…::test_a_raising_on_close_is_logged_and_the_value_stands` | §3.1 ServiceSet (`on_close` on the stack; a release logs and never raises); row 1 |
| `…::test_statuses_are_logged_with_leg_and_ready_at` | §3.1 "logs each status at INFO on `graphed_executors.services`"; r17–r20 "status witnesses read the log record" |
| `…::test_on_thread_backend_every_probe_answer_carries_the_driver_identity` | D2 identity rule (one machine); row 1 |
| `…::test_the_two_host_probe_passes_only_off_the_service_host[*]` | D2 identity rule; §3.1 probe (fresh key, `max(2, n_workers())`, no placement kwarg); row 1 two-host leg |
| `…::test_a_user_or_site_service_passes_on_any_answer[*]` | §3.1 "user and site → `None`, where any passing answer passes" |
| `…::test_the_probe_task_returns_host_identity` | D2 `host_identity()`; E1; row 1 |
| `test_service_checks.py::*` | §3.1 `check_ready`; D1 wire rule; row 2 (SERVING/NOT_SERVING/unknown, `grpcs` attempts TLS, no health, the P8 gateway shape, `http.server` and `https`, mismatches undialled, `tcp` on every scheme, unknown forms, `_probe_services` reason, `ServiceSet` endpoint form) |
| `test_services_sites.py::test_the_measured_rows`, `::test_site_services_are_endpoints` | D4 rows (P6, P7, m66); `services` values unpinned, each an endpoint |
| `…::test_service_hosts_derive_from_the_port_ranges`, `…::test_an_attached_backend_without_a_driver_host_refuses_a_managed_start` | D4 rule for any profile; §3.1 htcondor bullet ("leg 3 on an attached `("cluster",)` profile refuses here naming the attribute") |
| `…::test_a_services_value_that_is_not_an_endpoint_fails_construction_naming_the_row` | D4 "a `services` value `split_endpoint` refuses fails construction naming the row" |
| `…::test_the_triton_recipe_is_grpc_only`, `…::test_the_http_server_recipe` | §3.1 `submit/recipes.py` |
| `…::test_a_managed_endpoint_is_minted_with_the_check_scheme[*]`, `…::test_python_is_rendered_as_the_driver_interpreter` | D2 minted scheme; §3.1 `{python}` = `sys.executable` |
| `…::test_an_attached_backend_names_the_host_condor_writes_as_machine` | D2 `backend.host_identity` = `FULL_HOSTNAME` |
| `…::test_the_lpc_driver_job_backend_reads_its_own_row`, `…::test_the_lxplus_driver_job_hosts_a_service_beside_the_driver` | §3.1 htcondor bullet `in_job=`; row 3 (through `driver._runner`, no bindings) |
| `…::test_a_failed_second_pilot_spawn_leaves_no_pilot_and_no_port`, `…::test_a_failed_spool_leaves_no_cluster_and_no_port`, `…::test_a_failed_runner_in_the_driver_leaves_no_pilot_and_no_port` | §3.1 htcondor bullet (the m66 acquirers); `probes/class/probe_acquire_record.py` |
| `…::test_close_frees_the_task_server_port_when_the_launcher_stop_raises` | `probes/class/probe_phase_and_release_r14.py` B |
| `test_triton_service_ref.py::*` | D7 through the engine; row 4; E2 (a TCP listener passing the spec's `tcp` check) |
| `test_driverless_endpoints.py::test_the_driver_job_hosts_the_service_and_resolves_the_value` | §3.1 Driverless endpoints; row 5 main leg |
| `…::test_a_plan_without_services_starts_nothing` | r24 ("a plan with no services: the driver's `ServiceSet` submits no probe and starts nothing") |
| `…::test_given_endpoints_short_circuit_the_launch`, `…::test_a_bare_endpoint_is_refused_before_any_bindings_call[*]` | row 5 `submit_driverless(services=)` |
| `…::test_a_dead_given_endpoint_exits_1_with_service_unavailable`, `…::test_a_held_port_range_exits_1_with_os_error`, `…::test_a_refusal_of_the_in_run_recheck_exits_1` | D6; row 5; `probes/lifetime/probe_inner_worker_loss_r17.txt` |
| `…::test_exit_code_classifies_the_in_run_types[*]`, `…::test_a_plan_error_with_a_live_service_exits_3_intact[*]` | D6; `probes/services-code/probe_exit_code_r8.txt` F; E10 |
| `…::test_a_service_unreachable_round_trips_through_the_result_blob`, `…::test_a_service_unavailable_round_trips_through_the_result_blob`, `…::test_an_exception_that_does_not_reload_arrives_as_text` | §3.1 exception pickling; `probes/services-code/probe_result_blob_r9.txt`; E12 |
| `…::test_a_result_the_submitter_cannot_load_names_the_error_and_the_log` | §3.1 `RunHandle.result()`; E15, E16 |
| `test_services_live.py::*` | row 6 (a) pool, (b) given endpoint, (c) site leg |
| `test_scope_dask_memory.py::test_at_most_task_slots_leaf_results_are_held` | row 7; r23 K2 |
| `test_services_packaging.py::*` | row 8; §6 CI; E4, E6 |

## Blocked upstream: graphed's resolve walk

graphed at the pinned commit (cf4520d) has `bind_services` and the composites' `bind_services`
forwarding, but not `resolve_services`/`Resolvable` or the composites' forwarding of `resolve_services`
(plan §3.2 "The resolve walk", unmerged). These legs are written to the plan and stay red until graphed
lands it; nothing in executors can make them pass without reaching into graphed's private composites:

- `test_services_protocol.py::test_resolve_reaches_a_collated_part`
- `test_services_protocol.py::test_resolve_reaches_an_aggregate_plan_reduce`

Every other leg with a spy calls the hook on `plan.process` itself, which the engine reaches directly.

## Plan §3.1 "Fails on", and the test that catches each

| Fails on | Caught by |
|---|---|
| bind after submit | `test_bind_comes_after_the_probe_and_before_the_first_plan_task`, `test_http_server_recipe_runs_the_whole_path_without_a_service_client` |
| a same-host probe pass off the driver's machine | `test_the_two_host_probe_passes_only_off_the_service_host[only-the-service-host]` |
| a driverless bare endpoint reaching the job | `test_a_bare_endpoint_is_refused_before_any_bindings_call` |
| a service, record or pending task outliving its submission or a warm set's `with` | `test_a_managed_child_lives_exactly_as_long_as_its_run`, `test_two_sequential_runs_on_the_fake_each_release_only_their_own_key`, `test_overlapping_submissions_keep_their_own_scope`, `test_a_failed_leaf_submit_cancels_the_run_s_queued_leaves`, `test_a_warm_user_held_set_serves_two_plans_with_one_child`, `test_an_injected_fault_surfaces_and_leaves_nothing`, `test_the_driver_job_hosts_the_service_and_resolves_the_value` |
| a consumed result held until the run ends | `test_scope_dask_memory.py` |
| a cleanup failure replacing the refusal | `test_an_injected_fault_surfaces_and_leaves_nothing[release-raises-then-refusal-*]`, `test_a_raising_on_close_is_logged_and_the_value_stands` |
| a raising release step skipping a later one | `test_an_injected_fault_surfaces_and_leaves_nothing[release-raises-then-refusal-*]` (the earlier child is still reaped), `test_a_raising_on_close_is_logged_and_the_value_stands`, `test_close_frees_the_task_server_port_when_the_launcher_stop_raises` |
| a service-phase exception exiting 3 | `test_a_dead_given_endpoint_exits_1_with_service_unavailable`, `test_a_held_port_range_exits_1_with_os_error`, `test_a_refusal_of_the_in_run_recheck_exits_1`, `test_exit_code_classifies_the_in_run_types` |
| a returned value that needs a released service (a composed plan's too) | `test_resolve_services_runs_once_on_the_run_value_while_the_child_is_up`, `test_the_driver_job_hosts_the_service_and_resolves_the_value`, `test_resolve_reaches_a_collated_part`, `test_resolve_reaches_an_aggregate_plan_reduce` |
| an unanswered probe that hangs | `test_an_injected_fault_surfaces_and_leaves_nothing[probe-unanswered-*]`, `test_overlapping_submissions_keep_their_own_scope` |
| a URL-blind cache | `test_two_nodes_on_two_services_bind_two_endpoints` |
| a replaced given endpoint | `test_a_dead_given_endpoint_refuses_naming_user_with_zero_plan_submits`, `test_given_endpoints_short_circuit_the_launch` |
| a service name in the engine | `test_no_service_is_named_in_the_engine`, `test_http_server_recipe_runs_the_whole_path_without_a_service_client`, `test_the_literal_url_plan_starts_no_service_set` |
| an HTTP check passing a gRPC gateway | `test_an_http_check_fails_on_a_grpc_gateway_answering_200_everywhere` |
| a scheme-blind TLS choice | `test_grpcs_on_a_plaintext_server_attempts_tls_and_fails`, `test_http_check_passes_on_http_server_and_fails_as_https` |
| a connect-only probe | `test_the_probe_reports_not_serving_where_a_connect_would_pass`, `test_an_injected_fault_surfaces_and_leaves_nothing[probe-refuses-*]` (the hosted endpoint accepts TCP and answers every GET 503, so only the spec's own `http:/` check refuses it) |
| a driver job that ignores its site row | `test_the_lpc_driver_job_backend_reads_its_own_row`, `test_the_lxplus_driver_job_hosts_a_service_beside_the_driver` |
| a service refusal that is not retried or a plan error that is | `test_exit_code_classifies_the_in_run_types`, `test_a_plan_error_with_a_live_service_exits_3_intact`, the exit-1 tests above |
| a `result.pkl` the submitter cannot load | `test_a_result_the_submitter_cannot_load_names_the_error_and_the_log`, `test_an_exception_that_does_not_reload_arrives_as_text` |

## The contract these tests pin

Beyond the names in plan §3.1, the tests rely on these readings of it:

- `_probe_services(checks)` takes a sequence of `(endpoint, check)` pairs and returns the pair
  `(host_identity(), reasons)`, `reasons` one entry per check (a sequence or a mapping), `None` = ready.
  A probe task's key is `svc-<scope>-probe-<i>`; a plan task's first argument is its `RunContext`.
- The status log record carries `status` (a `ServiceStatus`) and is emitted at INFO by the
  `graphed_executors.services` logger, once per spec per `start()` (a user-held set's own start
  included). `detail` of a managed status that fell through leg 2 contains the dead site endpoint.
- `ServiceUnavailable.legs` has a key per leg tried (all three when none applied); a leg-1 failure
  names `user` and the endpoint. A check that never passes names the check (`http:/`) in
  `legs["managed"]`; a GPU recipe without `host_service` names `host_service` there.
- `ServiceSet.start()` returns an `Endpoints` whose values are the minted or given endpoints;
  `Endpoints.on_close(cb)` is reachable from the `endpoints` a plan part's `bind_services` receives
  (graphed's `bind_services` passes the mapping through).
- A managed service is started on `backend.advertise_host`; the fake and `ThreadBackend` advertise
  `127.0.0.1`, so the minted endpoint is `scheme://127.0.0.1:<port>` with `<port>` in the spec's
  `ports` (no `service_ports` attribute on either).
- Driver-job `run.json` carries `endpoints` (`{}` when none were given); `driver._runner(run, job, log)`
  keeps m67's signature.
- `HTCondorBackend.close` over a raising `launcher.stop` may raise that error after freeing the port.
- A held port range surfaces as the raw `OSError` from the bind: from `run` and `ServiceSet.start`
  (`test_an_injected_fault_surfaces_and_leaves_nothing[port-range-held-*]`), and as exit 1 with that
  `OSError` in `result.pkl` from the driver (`test_a_held_port_range_exits_1_with_os_error`). This is the
  literal text of the table's rows 1 and 5 (r15: the table governs over D2's "nothing left →
  `ServiceUnavailable`").
- D4's "a `None` range refuses the matching host at construction naming the row" is read as: building a
  `SiteProfile` derives `service_hosts` without that host, and a managed start on it is then refused in
  `ServiceUnavailable.legs["managed"]`, which names `host_service`. "Naming the row" is not asserted
  there, because no planned surface of that refusal carries the row's name; it is asserted for a
  `services` value that is not an endpoint, which fails `SiteProfile` construction naming the row.
- `test_a_failed_runner_in_the_driver_leaves_no_pilot_and_no_port` injects its fault by patching the
  module global `driver.HTCondorRunner` (the name `driver._runner` constructs its runner through), and
  asserts the patched constructor was reached.
- Every call into the planned API is bounded (`run_bounded`, `bounded_set`, `closing_bounded`): the
  suite runs without pytest-timeout, so a hang fails the test instead of wedging CI.
- `test_given_endpoints_short_circuit_the_launch` pins the given server's GET count at 7, the count
  the plan implies: the driver's own set (one leg-1 check, one probe answer, identity `None` so the
  first passing answer passes), the run's set (the same two), one GET per task (two partitions) and one
  by `resolve_services` in the driver.
- `recipes.http_server` children write no pid report; the fresh-interpreter whole-path leg and the
  lxplus driver-job leg run under `popen_backstop()`, which records what `subprocess.Popen` starts and
  kills anything still alive at the end (a correct implementation has already stopped it).
- The in-run re-check leg counts on the plan's call order: the driver's own set checks the given
  endpoint once and the probe asks once (identity `None`: the first passing answer passes); the server
  answers those two and refuses the rest.
- `recipes.http_server(name, *, root=".")`: the suite does not pin what `root` does (the plan's argv
  carries no directory); it replaces `ports`/`timeout_s` (and, in the driverless file, the argv) of the
  recipe's spec, keeping its kind and check.
- E3 reading: `test_driverless_endpoints.py` needs no histserv (the in-job histserv leg moved to m69b,
  and the row's service is `recipes.http_server`), so it does not `importorskip("histserv")`, and no
  m68a file imports histserv.
- Every status witness reads the `graphed_executors.services` log record (r17–r20), never a
  `statuses()` accessor.
- The packaging search for service names is case-insensitive (`triton|histserv`), so a docstring's
  "Triton" counts (E4).
