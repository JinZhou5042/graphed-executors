# m66 frozen suite — the direct HTCondor backend

This suite freezes the acceptance tests for `graphed_executors.htcondor_backend`: pilot jobs that
pull tasks from a driver-side HTTP task server, launched through the `htcondor2` bindings or as
local subprocesses. The source of truth is `lanes/htcondor/plan.md` (§2 is the public API, §3 the
test plan, D1–D7 the binding decisions). **Frozen — read-only after the m66 freeze tag.**

Run it with `pytest tests/frozen/m66`. Everything except `test_htcondor_live_pool.py` runs on every
OS in the main matrix. The live-pool file needs the `htcondor2` bindings (Linux wheels only) and a
personal HTCondor; the `test-htcondor` CI job provides both.

## Files

| File | Plan item | What it shows | A wrong implementation it fails |
|---|---|---|---|
| `htcondor_harness.py` | §3 harness | The m42 helpers the copied bodies use (D1, no cross-directory import), deferred `*_api()` accessors, `local_backend(n)` (waits for `n` pilots), `RecordingLauncher`, `PoisonUriProcess`, `MarkerBomb`, `run_bounded` | — |
| `data/lpc-schedd-ads.json` | §3 test 2 | The recorded LPC collector answer, `MyAddress` dropped | — |
| `test_htcondor_submit_conformance.py` | test 1, D1 | The m42 bodies over two local pilots: fixed plans bit-for-bit (n = 0, 1, 2, 5, 16), key-order folding, adaptive exhaustion, an intact `StageError`, the direct seam, exact monitor phases, the all-False capability vector, and a composed task running on a pilot pid | driver-side compute, unresolved future args, a non-bytes broadcast, a stringified `StageError`, lost events |
| `test_htcondor_sites.py` | test 2 | `schedd_weight` is the wrapper formula; `choose_schedd` picks `lpcschedd4.fnal.gov` in both orders; each weight term decides a derived two-ad set in both orders; `counts_as_alive`; the three profiles; `submit_description` keys; `start` refusals before any bindings call; a non-editable env ships as `env.tgz`, the secret file is mode 0600 | a formula missing any term, the proxy-default trap, site keys in `generic`, a missing secret/env/module transfer, refusals that are late or absent |
| `test_htcondor_auth.py` | test 3, D2 | Every route answers 403 to an unsigned or wrongly signed body and never unpickles it; the same body correctly signed is unpickled (the live control); a pilot with the wrong secret exits 2 and gets no task | unpickle before the check, a check skipped on one route |
| `test_htcondor_lost_pilot.py` | test 4, D5 | `LEASE_S = 2`: a die-once leaf is re-run on another pilot; a twice-lost leaf becomes a `StageError` naming its partition and the pilot, after exactly two attempts; the last pilot dying fails the run with "no pilots left"; `describe_failure` | requeue-forever, fail-on-first-loss, a second `set_running_or_notify_cancel`, a raw `WorkerLost` from a blocking dependency resolution, key-instead-of-partition attribution, a hang |
| `test_htcondor_refusals.py` | test 5, D4 | A lambda `process`, a `__main__` `process` and a `__main__` `combine` instance raise `ValueError` naming `user_modules` with zero submits; the importable control runs with five submits | the engine's `PicklingError` passing through, a lambda-only refusal, a refusal after submitting |
| `test_htcondor_no_import.py` | test 6, D3 | The package and its nine public names import while `htcondor`, `htcondor2` and `classad2` are blocked, and nothing tries to import them; `submit/` never names htcondor | an eager bindings import (wrapped in `try` or not), a missing public name |
| `test_htcondor_packaging_pins.py` | test 7 | The extra is `["htcondor>=25.13"]`; main omit has `*/htcondor_backend/*`; `.coveragerc-htcondor` measures only the backend with branch, parallel, sigterm and `fail_under = 90`; the `test-htcondor` job installs from `get.htcondor.org` and runs `tests/frozen/m66` with a bare `--cov` and `--cov-config=.coveragerc-htcondor`; `ci-required` needs it | a drifted floor, an unmeasured or mis-scoped coverage gate, a CI job that never runs the suite on a pool |
| `test_htcondor_live_pool.py` | test 8, D3 | (a) generic profile, two pilots: bit-for-bit, two pilot pids, the harness imported from the job scratch dir with no `PYTHONPATH`, the full job ads never carry the secret, and after `close()` the queue is empty, history has two rows and `pilot.0.out` is in `log_dir`. (b) a spooled profile shipping a venv the test builds: the pilot's `sys.prefix` is under its scratch dir, and `close()` retrieves `pilot.0.out` and empties the queue | a launcher that never submits, a missing retrieve or remove, a secret in the job ad, an ignored shipped env, dead `user_modules` transfer |

The live-pool file is skipped with its reason where `htcondor2` is not installed. Where it is
installed, each test first requires a schedd in the collector within 30 s and fails with the reason
when there is none.

## The contract these tests pin

Beyond the names in plan §2, the tests rely on these readings of it:

- `X-Graphed-Sig` is `hmac.new(secret, body, hashlib.sha256).hexdigest()`.
- `HTCondorBackend(launcher, n, host=...)` calls `launcher.start(url, secret, n)` during
  construction; the tests learn the url and secret from that call.
- `CondorPilots(...)` and `submit_description` touch no file system; `start` makes every refusal
  (image, sandbox, not a venv, editable dist) before its first `_htcondor()` call, then writes
  `graphed-secret` and `env.tgz` into `log_dir`. The sandbox check compares paths, so a `log_dir`
  under `/uscmst1b_scratch/lpc1/3DayLifetime/<getpass.getuser()>` passes it without existing.
- `start`'s bindings calls go through the module-level `launch._htcondor()`.
- `x509userproxy` is `f"{os.path.expanduser('~')}/x509up_u{os.getuid()}"` where `os.getuid` exists.
- A pilot's identity is `hostname:pid`, and `StageError.cause_message` carries it.
- `WorkerLost(key, pilot)` takes both positionally.
- The backend instance accepts attribute assignment, so a test can wrap `backend.submit`.
- A pilot started by hand as `python -m graphed_executors.htcondor_backend.pilot <url> <secret_path>`
  with a wrong secret exits 2 and prints "wrong secret file".
