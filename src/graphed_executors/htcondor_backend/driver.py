"""The driverless entry: ``python -m graphed_executors.htcondor_backend.driver <dir>``, run inside ONE job.

``<dir>`` (the job's scratch dir, its cwd) holds ``plan.pkl`` (a stdlib-pickled runtime ``Plan``) and
``run.json``. The driver runs the plan with :class:`HTCondorRunner` over :class:`LocalPilots` in its own
slot (``pilots="local"``) or over pilot jobs it submits itself (``pilots="condor"``, to the schedd named
by ``run.json["schedd_locate"]``), and on every exit writes ``result.pkl`` = ``(ok, ExecResult |
exception)`` and ``driver.log`` for the output transfer (``driver.sh`` writes a placeholder of both first).

The plan's services are resolved here, in the driver job, by the engine's three legs: the
``run.json["endpoints"]`` the submitter gave (leg 1), the job's own site row (leg 2), or a managed
start (leg 3), beside the driver or, for a name in ``run.json["announce_only"]``, by the announce of
the run's DAG SERVICE node, to which the driver publishes its url and an announce secret in
``run.json["dag_dir"]``; the run then re-checks and probes them as leg-1 endpoints inside
``runner.run`` and resolves its value while they are still up.

``main`` classifies an exit by phase, and only inside ``runner.run(plan)`` by type:

- 0: done.
- 1: environment, which the job's retries may get past. Anything raised before or after
  ``runner.run(plan)``: reading ``run.json`` or the plan, starting the pilots, and the driver's own
  service set (its endpoints and placement are environment; *this plan's decision*, plan-services D6).
  Inside ``runner.run``: a run whose workers were lost (a ``KilledWorker`` ``StageError``, e.g. every
  pilot preempted; *owner ruling 2026-09-25*), and the run's own service phase (``ServiceUnavailable``,
  ``ServiceUnreachable`` and a probe's raw ``WorkerLost``; *this plan's decision*).
- 3: every other exception from ``runner.run(plan)``: the plan's own error, deterministic, so the job's
  ``retry_until`` (a DAG's ``RETRY driver 2 UNLESS-EXIT 3``) stops retrying it. A ``StageError``
  (*owner ruling 2026-09-25*) or a task's exception re-raised intact, such as a ``ValueError`` (*this
  plan's decision*).
"""

from __future__ import annotations

import json
import logging
import os
import pickle
import sys
import time
import traceback
from contextlib import ExitStack
from pathlib import Path
from typing import Any, TextIO

from graphed.debug import StageError

from graphed_executors.submit.services import (
    ServiceSet,
    ServiceUnavailable,
    ServiceUnreachable,
    host_identity,
    release_quietly,
)

from .announce import URL_FILE
from .backend import HTCondorBackend, HTCondorRunner
from .launch import (
    LOG_FILE,
    PLAN_FILE,
    RESULT_FILE,
    RUN_FILE,
    SECRET_FILE,
    CondorPilots,
    LocalPilots,
    publish,
)
from .server import WorkerLost
from .sites import SITES

EXIT_DONE, EXIT_FAILED, EXIT_PLAN_ERROR = 0, 1, 3


def _runner(run: dict[str, Any], job: Path, log: TextIO) -> HTCondorRunner:
    """The job's runner over its pilots; the backend is released if anything after it fails."""
    profile = SITES[run["site"]]
    n = int(run["n_pilots"])
    announced: dict[str, str] = run.get("announce_only") or {}
    if run["pilots"] == "condor":
        launcher: Any = CondorPilots(
            profile,
            image=run["image"],
            request_memory_mb=int(run["request_memory_mb"]),
            log_dir=run["log_dir"],
            user_modules=[job / name for name in run["user_modules"]],
            schedd_locate=tuple(run["schedd_locate"]),
            extra_submit=run.get("extra_submit"),
        )
    else:
        launcher = LocalPilots(python=sys.executable, pythonpath=[job])
    if run["pilots"] == "condor" or announced:  # dialled from other nodes: pilot jobs, SERVICE nodes
        host, ports = host_identity(), profile.worker_ports
    else:  # the slot's own pilots dial loopback; any free port serves them
        host, ports = "127.0.0.1", profile.worker_ports or (0, 0)
    with ExitStack() as on_error:  # held until the runner exists: a failure after the pilots stops them
        backend = HTCondorBackend(
            launcher, n, host=host, port_range=ports, in_job=profile, announced=announced
        )
        on_error.callback(release_quietly, "the driver job's backend", backend.close)
        if announced:
            dag_dir = Path(run["dag_dir"])
            # the secret first: a SERVICE node that reads the new url reads the new secret with it
            secret = backend._server.announce_secret(sorted(announced.values()))
            publish(dag_dir / SECRET_FILE, secret.hex())
            publish(dag_dir / URL_FILE, backend._server.url)
        if run["pilots"] == "condor":
            where = f"cluster={launcher.cluster}"
        else:
            where = f"pids={[p.pid for p in launcher._procs]}"
        print(f"{n} {run['pilots']} pilots on {backend._server.url} {where}", file=log, flush=True)
        runner = HTCondorRunner(
            backend,
            min_pilots=int(run["min_pilots"]),
            retries=int(run["retries"]),
            max_in_flight=int(run["max_in_flight"]),
        )
        on_error.pop_all()
    return runner


def _result_blob(ok: bool, payload: object) -> bytes:
    """``pickle.dumps((ok, payload))``, returned only once it loads again: an outcome that does not
    round-trip (unpicklable, or an exception whose constructor differs from its ``args``) reaches the
    submitter as a ``RuntimeError`` carrying its text."""
    try:
        blob = pickle.dumps((ok, payload))
        pickle.loads(blob)
        return blob
    except Exception as exc:
        text = f"{type(payload).__name__} did not round-trip through pickle ({type(exc).__name__}: {exc})"
        return pickle.dumps((False, RuntimeError(f"{text}: {payload}")))


def _exit_code(exc: Exception) -> int:
    """The exit of an exception from inside ``runner.run(plan)``: 1 (retried) for lost workers and for
    the run's service phase, 3 (not retried) for everything else, the plan's own error."""
    lost = isinstance(exc, StageError) and exc.cause_type == "KilledWorker"
    service = isinstance(exc, ServiceUnavailable | ServiceUnreachable | WorkerLost)
    return EXIT_FAILED if lost or service else EXIT_PLAN_ERROR


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    job = Path(args[0] if args else ".").resolve()
    if str(job) not in sys.path:
        sys.path.insert(0, str(job))  # user_modules arrive in the scratch dir
    start = time.monotonic()
    result: object = None
    error: BaseException | None = None
    code = EXIT_FAILED
    with open(job / LOG_FILE, "a") as log:
        print(f"driver pid={os.getpid()} dir={job}", file=log, flush=True)
        handler = logging.StreamHandler(log)
        handler.setFormatter(logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s"))
        package = logging.getLogger("graphed_executors")
        level = package.level
        package.addHandler(handler)  # service statuses and failed releases, in driver.log
        package.setLevel(logging.INFO)
        try:
            run = json.loads((job / RUN_FILE).read_text())
            with open(job / PLAN_FILE, "rb") as f:
                plan = pickle.load(f)
            runner = _runner(run, job, log)
            try:
                live = runner.wait_for_pilots()
                print(f"{live} pilots live after {time.monotonic() - start:.1f}s", file=log, flush=True)
                # the driver job's own set: its failures are environment (exit 1) whatever their type
                given = run.get("endpoints") or {}
                with ServiceSet(plan.services, runner.backend, endpoints=given) as endpoints:
                    runner.services = endpoints
                    try:
                        result, code = runner.run(plan), EXIT_DONE
                    except Exception as exc:
                        error, code = exc, _exit_code(exc)
            finally:
                try:
                    runner.close()
                except Exception:  # the run's outcome stands; the close failure is only logged
                    print("runner.close() failed:", file=log)
                    traceback.print_exc(file=log)
        except Exception as exc:
            error, code = exc, EXIT_FAILED
        finally:
            package.removeHandler(handler)
            handler.close()
            package.setLevel(level)
        blob = _result_blob(True, result) if error is None else _result_blob(False, error)
        if error is not None:
            traceback.print_exception(error, file=log)
        print(f"exit {code} after {time.monotonic() - start:.1f}s", file=log, flush=True)
    (job / RESULT_FILE).write_bytes(blob)
    return code


if __name__ == "__main__":
    sys.exit(main())
