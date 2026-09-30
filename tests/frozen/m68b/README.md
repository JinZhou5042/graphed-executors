# m68b frozen suite, part B1: attached cluster hosting

This suite freezes the acceptance tests for executors m68b part B1: a `ServiceJob` is a second HTCondor
cluster beside `CondorPilots`, running `service.sh` + the stdlib-only `announce.py`, which starts the
recipe's child in `service/`, self-checks it on an in-job port, and announces `key host:port identity`
to the task server's `/announce` route, signed with a per-call announce secret. `HTCondorBackend`
gains `host_service`/`release_service` when its launcher is `CondorPilots` and `"cluster"` is in
`service_hosts`. The source of truth is `plan-services.md` on the plan branch: §3.3 B1 (the unit),
the three B1 rows of the §6 frozen-rows table, and "Fails on (B1)". **Frozen: read-only after the
m68b B1 freeze tag.**

## How to run it

`pytest tests/frozen/m68b`. `test_announce_route.py` runs on every OS against a real `TaskServer` on
loopback; the in-process legs of `test_cluster_service_job.py` run on every OS under the bindings
recorder; its subprocess legs
(`announce.py` run as `[sys.executable, "-m", "graphed_executors.htcondor_backend.announce",
"service.json"]` so coverage records it) are `skipif(sys.platform == "win32")`.
`test_cluster_services_live.py` needs the `htcondor2` bindings and a personal HTCondor: skipped
without the bindings, a failure naming the missing pool when they are installed and no schedd
answers. B1 commit 1 adds `tests/frozen/m68b` to the `test-htcondor` job; that job's per-file
coverage gate is what measures `announce.py`.

## Files

| File | What it is |
|---|---|
| `m68b_harness.py` | Accessors for the planned names (looked up inside test bodies, so the suite collects before the implementation exists); `run_bounded`/`get_within`/`wait_for`, and `wait_announce` (a call overrunning its own timeout by `MARGIN_S` fails); `pid_gone`, `port_free`, `free_range`, `wildcard_listener`; announce signing and posting; the m66 `MarkerBomb`; a real `TaskServer` on loopback; `spy_method`; the m67 bindings recorder (`RecordingSchedd`, `record_bindings`, `FAKE_CLUSTER`); a `Machine = localhost` machine ad; `job_config`/`write_job`/`AnnounceRun` (the job dir and a bounded `announce.py` process that is SIGTERMed, then killed, with its children, on exit); the live legs' plan parts (`TimedGet`, `GatedGet`, `service_plan`) |
| `m68b_child.py` | The managed child (`<python> m68b_child.py <mode> <port> <report> [code]`, stdlib, python ≥ 3.9): appends a start line to `<report>.starts`, writes pid, `sys.executable`, cwd entries, whether `../graphed-secret` exists, whether SIGTERM is blocked and two env vars; modes `serve`, `ignore` (SIGTERM ignored), `grpcish` (200 `application/grpc`), `exit`, `once` (one 503, then exit) |
| `test_announce_route.py` | Row 1: the `/announce` route and the announce secret |
| `test_cluster_service_job.py` | Row 2: `ServiceJob` keys and files, `host_service`/`release_service` under the recorder, `announce.py` in process and as a subprocess |
| `test_cluster_services_live.py` | Row 3: the minicondor legs |

## Traceability

"FoN" cites the "Fails on (B1)" entry a test fails on.

| Test | Plan clause / FoN |
|---|---|
| `test_announce_route.py::test_a_signed_announce_is_recorded_once_and_wakes_the_waiter` | row 1 signed → 200, `wait_announce` returns `(host:port, identity)`, second call `None` after `t`; `announce_secret` 32 bytes ≠ pilot secret |
| `…::test_an_unsigned_or_pilot_signed_announce_is_refused_and_records_nothing` | row 1 unsigned / pilots' secret → 403, nothing recorded |
| `…::test_another_key_s_announce_secret_does_not_sign_this_key` | row 1 another key's announce secret → 403 |
| `…::test_a_signed_body_that_is_not_three_fields_with_an_integer_port_is_a_bad_request[two,four,non-integer-port]` | row 1 → 400, nothing recorded |
| `…::test_a_body_without_a_readable_key_is_forbidden[not-utf8,empty]` | row 1 not UTF-8 / empty → 403 |
| `…::test_a_forgotten_key_is_refused` | row 1 `forget_announce` → 403; FoN announce secret left registered |
| `…::test_an_announce_secret_signs_no_pickle_and_the_pilot_secret_still_settles` | row 1 `/result` pickle; FoN signing a pickle with an announce secret (m66 marker) |
| `…::test_a_waiting_announce_takes_no_wake_up_meant_for_a_leasing_pilot` | §3.3 TaskServer bullet (announces wait on their own condition) |
| `test_cluster_service_job.py::test_host_service_exists_iff_condor_pilots_and_a_cluster_host[*]` | row 2 `hasattr(backend, "host_service")` iff |
| `…::test_files_mirror_each_input_under_service_by_basename` | row 2 `files()` mirror (file symlink absolute; `models/` real tree of file links); FoN directory input landed without its name |
| `…::test_inputs_named_like_job_files_land_only_under_service` | row 2 `service.json`/`tmp`/`.job.ad`/`.machine.ad`/`a,b`; FoN input that replaces a job file |
| `…::test_every_transfer_entry_is_relative_so_a_comma_in_log_dir_splits_nothing` | row 2 every entry relative, `,` in `log_dir`; FoN absolute `transfer_input_files` entry |
| `…::test_the_submit_keys[*]` | row 2 keys: absolute `executable` in fresh `service-<key>/`, `initialdir`, `arguments`, `request_*` (float → int, no `request_gpus` at 0), `job_max_vacate_time`, `service` named once, no secret/url in any value; FoN relative job executable, secret in a job ad |
| `…::test_service_json_carries_the_plan_fields_and_no_secret` | row 2 `ports == worker_ports`, `lease_s`/`beat_s` the server's; `graphed-secret` 0o600 |
| `…::test_the_interpreter_image_and_env_per_recipe[*]` | row 2 `./env/bin/python` / `sys.executable` / `python3`; `MY.SingularityImage` over the profile's; `env.tgz` link iff image-less `ship_env`; FoN dropped or wrong env transfer |
| `…::test_send_credential_is_dropped_on_an_attached_job_and_kept_in_watch_mode` | row 2 `MY.SendCredential` (control: pilots keep it) |
| `…::test_a_relative_log_dir_and_a_relative_input_resolve_where_they_were_written` | row 2 cwd A/B leg; FoN link that does not resolve or reaches another runner's `env.tgz`, `stop()` unlinking another runner's secret |
| `…::test_construction_refuses_a_bad_input_naming_it[missing,shared-basename,dir-symlink,holds-dir-symlink]` | row 2 refusals; FoN input holding a directory symlink |
| `…::test_construction_accepts_a_directory_holding_a_file_symlink` | row 2 control (`models/` accepted) |
| `…::test_stop_removes_at_once_and_unlinks_only_the_job_s_secret[*]` | §3.3 `stop()` (act Remove at once, no drain; spool retrieve on JobStatus 4); row 2 `stop()` unlinks the job's `graphed-secret` |
| `…::test_announce_py_is_stdlib_only_python_3_9_and_the_job_runs_the_module_s_code` | row 2 stdlib only, `feature_version=(3, 9)`; the transferred copy is the module's bytes |
| `…::test_service_sh_exits_3_naming_an_interpreter_it_cannot_find` | §3.3 `service.sh` (POSIX) |
| `…::test_the_sigterm_handler_never_waits_and_ignores_further_sigterms` | row 2 `on_sigterm` in process; FoN SIGTERM handler that waits |
| `…::test_the_stop_path_signals_no_child_popen_already_reaped` | row 2 `main()` → 143, no `os.kill`; FoN reaped child's pid signalled |
| `…::test_the_stop_path_signals_an_unreaped_child_once` | row 2 control (POSIX): exactly one SIGTERM |
| `…::test_host_service_forgets_its_announce_secret_when_construction_refuses` | row 2 missing input after the mint; FoN announce secret left registered after a failed call |
| `…::test_a_failed_submit_or_spool_leaves_no_cluster_and_no_registered_secret[spool,submit]` | row 2 m68a failed-spool shape; FoN failed-spool `ServiceJob` left queued |
| `…::test_host_service_returns_the_announced_endpoint_and_releases_in_order[*]` | §3.3 `host_service` (key `<scope>-<hex16>`, endpoint scheme per check, identity) and `release_service` (forget, then stop); FoN announce secret left registered after a released call, service outliving its run |
| `…::test_host_service_fails_a_gone_held_or_late_job_and_forgets_its_key[gone,held,idle,spooling]` | §3.3 host_service failure paths; FoN timed-out/dead `ServiceJob` left queued, secret left registered |
| `…::test_an_attached_service_announces_once_ready_from_a_clean_start` | row 2 `../graphed-secret` False, beats 200, env merge, SIGTERM unblocked, port in range, identity = `host_identity()`; FoN announce before readiness or without identity, pilot secret readable through the service, recipe `env` replacing the job's |
| `…::test_the_child_s_cwd_holds_exactly_the_inputs` | row 2 `user.cc` 404, `models/<file>` 200; FoN user credential or other file in the cwd |
| `…::test_a_relative_python_is_resolved_in_the_job_dir` | row 2 `./env/bin/python` wrapper marker; FoN relative `{python}` left relative or resolved against `service/` |
| `…::test_a_relative_argv0_resolves_among_the_inputs` | row 2 `./serve.sh`; FoN argv[0] resolved against the job dir |
| `…::test_a_missing_argv0_exits_3_naming_it_without_a_traceback` | row 2; FoN raising `Popen` exiting with a traceback |
| `…::test_a_bare_python_without_path_is_resolved_on_the_default_path` | row 2 no-`PATH` leg; FoN child started through `announce.py`'s own `sys.executable` |
| `…::test_sigterm_kills_a_child_that_ignores_it_within_the_bound` | row 2 5 s + margin; FoN reap that waits on a SIGTERM-ignoring child |
| `…::test_the_orphan_reap_kills_a_child_that_ignores_sigterm` | row 2 closed-port url → exit 0 within `lease_s` + 5 s + margin; FoN orphan-path reap that waits, orphaned job left running |
| `…::test_sigterm_during_the_orphan_reap_ends_within_the_bound` | row 2 SIGTERM ~1 s after "orphaned" |
| `…::test_a_held_first_port_announces_the_next` | row 2 `("", port)` listener; FoN port chosen off the node that binds it |
| `…::test_a_tcp_requirement_announces_through_a_connect` | row 2 `tcp` self-check |
| `…::test_a_grpc_gateway_never_passes_http_and_the_budget_is_the_whole_start` | row 2 exit 3 within `timeout_s` + margin, one start; FoN `timeout_s` per port |
| `…::test_a_child_that_exits_at_once_is_not_restarted_per_port` | row 2 ≥ 10 ports, exit 3 naming the returncode; FoN restarted per port |
| `…::test_a_child_that_fails_its_check_then_exits_is_started_once` | row 2 ×10, one start each; FoN restarted per port |
| `…::test_an_orphaned_service_is_reaped_and_exits_0[no-200-ever,another-secret,server-gone]` | row 2 orphan rule; FoN orphaned before its first announce |
| `…::test_watch_mode_announces_each_new_url_and_secret` | row 2 watch mode, SIGTERM reaps the child |
| `test_cluster_services_live.py::test_a_cluster_hosted_http_server_serves_its_run_and_leaves_with_it` | row 3 (a): `Machine` identity in the status and every probe answer, task GET, `/service.json` 404, removal polled, `forget_announce` before `stop`, per-run clusters, overlap removes only its own, history ordering against `t`; FoN service outliving its run, run releasing another run's service |
| `…::test_a_late_or_dead_service_job_is_removed_and_its_key_forgotten` | row 3 (b): `gpus=2` `TimeoutError` naming 20 and JobStatus 1; exit-at-once `RuntimeError` naming JobStatus 4 before `timeout_s`; both gone (polled) and forgotten |

Not a test: "`announce.py` passes `test-htcondor`'s per-file gate (≥ 90%) with these legs executing
it" and FoN "an `announce.py` the frozen suite does not measure" are CI's per-file coverage gate over
the `-m` subprocess legs above.

## Deviations from the row wording

- `serve.sh` `exec`s `sys.executable m68b_child.py` rather than `python3 -m http.server`, so the leg
  does not depend on a `python3` on `PATH` and its start is witnessed by the child's report.
- The construction refusal is asserted as `ValueError` or `OSError` naming the input; the plan
  pins neither type.
- `exit`/`once` children exit 97/98, so "naming its returncode" cannot match a clock field.
