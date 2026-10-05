# m68b frozen suite, part B2: DAG driverless, `job_root`, lxplus

This part freezes the acceptance tests for executors m68b part B2. `SiteProfile.job_root` becomes the
one statement of which tree a site's schedd and jobs read and write directly, and m67's self-submit
reads it. A driverless plan whose services need a SERVICE node (a recipe with an image or GPUs, no
`services=` endpoint, a kind the site row does not serve) becomes a DAG in a new
`<log_dir>/graphed-<nonce>/`. The driver job publishes its url and an announce secret for the node
ids. Its in-job backend resolves those names from the SERVICE nodes' announces.
`RunHandle(dag=True)` tracks the DAGMan cluster and its `driver` node. The source of truth is
`plan-services.md` on the plan branch: §3.3 B2, the two B2 rows of the §3.3 frozen-rows table, and
"Fails on (B2)". **Frozen: read-only after the m68b B2 freeze tag.**

## How to run it

Run `pytest tests/frozen/m68b/test_driverless_dag.py tests/frozen/m68b/test_driverless_dag_live.py`.

- `test_driverless_dag.py` runs on every OS under the bindings recorder.
  - The in-job legs build `driver._runner(...)` with `sys.modules["htcondor2"] = None` and a
    `Machine = localhost` machine ad.
  - `test_the_dag_description_is_the_probe_s` needs real bindings and `condor_dagman` on `PATH`
    (the `test-htcondor` job). It is skipped elsewhere.
  - The placeholder `sh` leg is `skipif(shutil.which("sh") is None)`.
- `test_driverless_dag_live.py` needs the `htcondor2` bindings and a personal HTCondor with one
  simulated GPU (`probes/m68b/sim_gpu.config`, B1's commit 1 adds it to `test-htcondor`).
  - Without the bindings it is skipped.
  - With the bindings, a pool that lists no schedd, or no GPU, is a failure naming what is missing.

## Files

| File | What it is |
|---|---|
| `m68b_dag_harness.py` | The accessors for the planned names, looked up inside test bodies. `run_bounded`, `wait_for`, `free_range`. HMAC signing and `post`. The m66 `MarkerBomb` and a lookup-free `ok_server`. `fake_venv`, `write_machine_ad` and `site_copy` (`dataclasses.replace` of a row, registered in `SITES` for one test). The bindings recorder: `DagSchedd` answers a constraint naming `DAGNodeName` from the driver node's ads and every other constraint from the DAGMan cluster's ads, and logs `history`'s `match`. `FakeHTCondor.Submit.from_dag` is logged, and delegated to real bindings when it is given them. `read_sub`, `dag_statements` and `tree_bytes` read back what was submitted. The spec builders. The live plan parts `BodyGet`/`DagResolved`/`kill_driver`. The job imports this file as a `user_modules` entry, so it imports only the stdlib, graphed and graphed_executors. |
| `data/from_dag-generic.txt` | The `Submit.from_dag` description that `probes/m68b/probe_dag_service.txt` printed under `{"UseDagDir": True, "AddToEnv": "_CONDOR_DAGMAN_USE_STRICT=0"}`, verbatim (`sed -n 3,14p`, the indent stripped). |
| `test_driverless_dag.py` | Row 1 (every OS). |
| `test_driverless_dag_live.py` | Row 2 (minicondor with one simulated GPU). |

## Traceability

"FoN" cites the "Fails on (B2)" entry a test fails on.

| Test | Plan clause / FoN |
|---|---|
| `test_driverless_dag.py::test_sites_carry_job_root_and_no_name_keyed_root_is_left` | Row 1: `SITES` `job_root` values and no `_SELF_SUBMIT_ROOT`. FoN: a site root read from two places. |
| `…::test_self_submission_reads_job_root_from_the_row` | Row 1: `pilots="condor"` with `job_root=None` is refused naming `job_root` (control: the same row runs a fat slot). lxplus outside `/afs` is refused naming `/afs`. §3.3 "applied to the directory actually used": with `log_dir=None`, a root other than `/` is refused naming it. An lxplus copy whose `job_root` holds `log_dir` self-submits. FoN: a site root read from two places. |
| `…::test_a_kind_the_site_serves_is_not_a_service_node` | Row 1: on the lpc copy (`sandbox_root=<tmp>`, `image=`, a fake venv), a `recipes.triton` plan submits one plain job with no `announce_only` entry. Control: the row's `services={}` is refused naming `job_root` with the recorder empty. FoN: a SERVICE node for a kind the site serves. |
| `…::test_service_nodes_are_named_by_derived_ids_in_name_order` | Row 1: the ids leg (`driver` declared before `a b`, `0web` sorts first and is image-less CPU, `given` has a `services=` endpoint). Node names, `.sub` stems, `service-svc<i>/` and the `service.json` keys. No `graphed-secret` anywhere in the run dir. `driver.sub` keeps the driver's keys. `announce_only == {"a b": "svc0", "driver": "svc1"}`. The DAG text. No `max_retries`/`retry_until` in any node `.sub`. `periodic_remove = JobStatus == 5` in each `svc<i>.sub` and not in `driver.sub`. `from_dag` on the absolute DAG path with exactly the two options, and its description submitted as it came, unspooled. FoN: a DAG without SERVICE; a node name, file or key taken raw from a spec name; two retry owners; a SERVICE node given the pilots' secret; a DAG that runs only from its own dir; a SERVICE node failing a DAG whose driver succeeded. |
| `…::test_an_lxplus_service_node_keeps_its_credential_and_env_and_the_dag_is_not_spooled` | Row 1: the lxplus copy (`job_root=<tmp>`). `svc0.sub` has `MY.SendCredential = True`. `service-svc0/env.tgz` resolves (`files()` ran after `_stage`). `submit` is logged with `spool=False` and no `spool` call, although the profile spools. FoN: a SERVICE node without the credential its `dag_dir` needs. |
| `…::test_a_user_module_path_holding_a_comma_is_refused[plain,dag]` | Row 1: a `,` in a `user_modules` path is refused before any bindings call. Control: the same module without the comma submits. |
| `…::test_driver_sh_writes_the_placeholder_before_it_execs` | Row 1: the `driver.sh` text writes `result.pkl` and `driver.log` before its `exec` line. FoN: a killed driver held for ever. |
| `…::test_the_placeholder_loads_as_a_runtime_error_naming_the_log` | Row 1: the pre-exec lines, run through `sh` in a scratch dir, leave a `result.pkl` that loads as `(False, RuntimeError)` naming `driver.log`, and leave a `driver.log` (S-15). FoN: a killed driver held for ever. |
| `…::test_each_dag_submission_gets_a_new_run_directory` | Row 1: each submission's files lie in a new `<log_dir>/graphed-<nonce>/` (the handle's `log_dir`, the nonce of `JobBatchName`). A second submission gets another directory and leaves the first byte-identical. FoN: a DAG that reads another run's files. |
| `…::test_a_dag_is_refused_before_any_bindings_call[no-worker-ports,lxplus-outside-afs,module-outside-root,input-outside-root]` | Row 1: refusals with the recorder empty. A generic copy without `worker_ports` names it. lxplus outside `/afs` names `/afs` and `log_dir`. A `user_modules` path, or an `announce_only` recipe input, outside `job_root` names the path, `job_root` and the root (§3.3: "each refusal naming its field, the path and the root"). FoN: a DAG input the schedd cannot read. |
| `…::test_paths_under_job_root_submit` | Row 1 control: the same module and input under the root submit. |
| `…::test_the_job_root_check_is_lexical` | Row 1: an input under the root that is a symlink to a file outside it is accepted. |
| `…::test_the_driver_job_publishes_an_announce_secret_then_its_url` | Row 1: with `announce_only`, `_runner` `os.replace`s `graphed-secret` (0600 on POSIX) before `driver.url`. The url names the ad's `Machine` and a `worker_ports` port. A pickle signed with the announce secret to `/result` gets 403 and is not loaded (control: the pilots' secret's is loaded). A run dict without the two keys builds as m67's (`advertise_host` `127.0.0.1`, no `host_service`, no `driver.url`). FoN: a SERVICE node given the pilots' secret; a retried driver the SERVICE node never re-announces to (the pair is written secret first). |
| `…::test_the_in_job_backend_resolves_a_service_node_by_its_announce` | Row 1: after a signed announce under `svc0`, `host_service` returns `(endpoint, the announced identity, "svc0")`. It refuses another name naming `announce_only`, with no bindings importable. A `ServiceSet` over it logs `("web", "managed", "cluster", <identity>)`. FoN: a second `ServiceJob` from the driver job. |
| `…::test_a_finished_dag_takes_its_outcome_from_the_latest_driver_try[*]` | Row 1: the DAGMan cluster is gone with its `ExitCode 1`, and the outcome is the highest-`ClusterId` driver try in recorded order. A latest try with `ExitCode 0` gives `done`, whether it comes last or between the others. A latest try with `ExitCode 1` coming first, a latest try killed by a signal, or no driver ad gives `failed`. `history` is logged on the driver node with `match=1` (history is newest first), and no constraint or projection names `DAG_*`. FoN: a status or outcome read from DAGMan's exit; a `DAG_*` counter or `DAG_Status`. |
| `…::test_a_held_driver_node_reads_held_and_wait_times_out_naming_it` | Row 1: a running DAGMan (`DAG_JobsHeld: 0`) with its driver node at `JobStatus 5` gives `held`. The query is constrained on `DAGManJobId` and `DAGNodeName == "driver"`. `wait(timeout=0.3)` raises `TimeoutError` naming `held`. Control: m67's mapping gives `running`. FoN: a held driver node reported as running. |
| `…::test_every_dagman_job_status_is_mapped[*]` | Row 1: DAGMan 1 with no driver ad gives `queued`, 2 with the driver running gives `running`, 3 gives `removed`, 5 gives `held`, and 6 and 7 give `running`. FoN: a DAGMan `JobStatus` left unmapped. |
| `…::test_a_finished_dag_still_queued_on_a_spooled_site_is_read_without_a_retrieve` | Row 1: on lxplus (`spool=True`), a queued DAGMan at `JobStatus` 4, with its own `ExitCode 1`, and a driver try at `ExitCode 0` gives `done`. `result()` returns the run dir's value with no `retrieve` logged. Control: `dag=False` retrieves. FoN: a queued `4` read as running. |
| `…::test_a_failed_dag_without_a_result_names_the_dagman_log` | Row 1: `RuntimeError` naming `run.dag.dagman.out`. |
| `…::test_an_m67_saved_handle_loads_as_a_plain_job` | Row 1: an m67 handle file loads with `dag=False`, and a `dag=True` handle round-trips. |
| `…::test_the_dag_description_is_the_probe_s` | Row 1 fixture (`test-htcondor`): the submitted description equals `data/from_dag-generic.txt` after substituting the DAG dir, the bindings' own `htcondor2.version()` (as `-CsdVersion` escapes it), and the `condor_dagman` path. |
| `test_driverless_dag_live.py::test_a_gpu_service_runs_as_a_service_node_of_the_run_s_dag` | Row 2 (a), from a cwd other than `log_dir`. It finds a universe-7 DAGMan and exactly one SERVICE node, `svc0`, with `NumJobStarts 1` and `AssignedGPUs` set. `result()` holds the body that `resolve_services` fetched. `RemoveReason` names `DAGManJobId`, and the queue empties (polled). FoN: a DAG without SERVICE; a DAG that runs only from its own dir; a SERVICE node failing a DAG whose driver succeeded. |
| `…::test_a_killed_driver_is_retried_then_fails_with_the_placeholder` | Row 2 (b): the plan's task SIGKILLs the driver. The DAG ends `failed` (never held, or `wait` would time out) with three driver clusters in history, and `result()` raises the placeholder `RuntimeError`. FoN: a killed driver held for ever; a retried driver the SERVICE node never re-announces to (the third try reaches the plan only after its announce). |

Not tested here: the lxplus GPU/Triton run and site checks (1)–(3), which are owner-run evidence
transcripts (§3.3 "Site checks", §9). The docs commit's statements are not tested either: the lexical
check's consequence for a held node, and the cost of a SERVICE node that never announces
(3 × `timeout_s`).

## Deviations from the row wording

- **The driver-try orders are a superset of the row's.** The row returns the highest `ClusterId`
  last. The tests also put it between the others (`done`) and first (`failed`), so that neither a
  first-ad reading nor a last-ad reading passes (Q-03: history returns recorded order). A try killed
  by a signal, with no `ExitCode`, is added as a `failed` case.
- **The lpc plain job's `announce_only == {}` is read as `run.get("announce_only", {}) == {}`.** The
  driver reads `run.get("announce_only") or {}`. That the job is plain is witnessed by no `from_dag`,
  `executable` `driver.sh` and `max_retries 2`.
- **The row's "lpc naming `job_root`" refusal is the lpc test's control.** It is not repeated in the
  refusal parametrization.
- **The comma refusal's message is asserted to name the path or `user_modules`.** The row pins no text.
- **The lexical-symlink leg skips on a host that cannot create a symlink.**
- **The in-job legs run on a generic copy whose `worker_ports` is a free range apart from
  `driver_ports`.** Generic's two ranges are equal, so there a url on `driver_ports` would pass.
- **Beyond the row, from §3.3 text:**
  - the DAG refusal messages name their field, path and root;
  - `log_dir=None` is refused under a root other than `/`;
  - the run dir's nonce is `JobBatchName`'s;
  - the placeholder lines also create `driver.log`;
  - `run.json.dag_dir` is absolute;
  - each node's `initialdir`, and its `service.json` `watch`/`url`;
  - which spec each node was built from;
  - a `dag=True` handle round-trips through save/load;
  - the live file fails naming `sim_gpu.config` when the pool has no GPU.
