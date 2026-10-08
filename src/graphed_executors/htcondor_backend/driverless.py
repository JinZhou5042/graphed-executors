"""Driverless runs: :func:`submit_driverless` ships a plan in ONE HTCondor job whose driver
(:mod:`graphed_executors.htcondor_backend.driver`) runs it, so the run needs no login session;
:class:`RunHandle` tracks that job and collects its ``result.pkl``, from this session or a later one.

A plan with a service the driver job can host neither beside itself nor through its site (an image or
GPUs) is submitted as a DAG instead: ``JOB driver`` plus one ``SERVICE`` node per such service, each a
watch-mode :class:`~graphed_executors.htcondor_backend.services.ServiceJob` announcing to the driver of
the moment, all in a new run directory under ``log_dir`` that must lie under the site's ``job_root``.
"""

from __future__ import annotations

import json
import os
import pickle
import tempfile
import time
import uuid
from collections.abc import Mapping, Sequence
from contextlib import ExitStack
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, cast

from graphed.core.execution import ExecResult, Plan
from graphed.core.plan import DurablePlanV2
from graphed.services import ServiceSpec, split_endpoint

from . import launch
from .backend import _require_plan_importable
from .launch import (
    ENV_FILE,
    LOG_FILE,
    PLAN_FILE,
    RESULT_FILE,
    RUN_FILE,
    CondorPilots,
    CondorReason,
    collectors,
)
from .services import ServiceJob
from .sites import SITES, SiteProfile, counts_as_alive

DRIVER_MODULE = "graphed_executors.htcondor_backend.driver"
PILOTS_SUBDIR = "pilots"
TERMINAL = ("done", "failed", "removed")
STATUS_ATTRS = ["JobStatus", "HoldReasonCode", "ExitCode"]
LOG_NAMES = (LOG_FILE, "driver.out", "driver.err", "driver.condor.log")
DAG_FILE = "run.dag"
# with the site's default strictness an idle or held SERVICE node at the DAG's end fails DAGMan
DAG_OPTIONS = {"UseDagDir": True, "AddToEnv": "_CONDOR_DAGMAN_USE_STRICT=0"}
PLACEHOLDER = pickle.dumps(
    (False, RuntimeError(f"the driver exited before writing a result; see {LOG_FILE}"))
)


@dataclass(frozen=True)
class RunHandle:
    """A submitted driverless job: ``cluster`` on the schedd named ``schedd`` of ``site``, its outputs
    returning to ``log_dir``. Every call locates that schedd by name, never the local default. With
    ``dag``, ``cluster`` is the run's DAGMan job, and the outcome is its ``driver`` node's."""

    site: str
    schedd: str
    cluster: int
    log_dir: str | Path
    submitted_at: float
    dag: bool = False

    def _located(self) -> tuple[Any, Any]:
        htc = launch._htcondor()  # looked up per call, so a test can stand in for the bindings
        query = SITES[self.site].schedd_query
        pools: list[str | None] = [None] if query is None else [*collectors(htc, query[0])]
        errors = []
        for pool in pools:
            try:
                return htc, htc.Schedd(htc.Collector(pool).locate(htc.DaemonType.Schedd, self.schedd))
            except Exception as exc:  # the next collector in the list may know it
                errors.append(f"{pool}: {exc}")
        raise RuntimeError(f"schedd {self.schedd} not found: {'; '.join(errors)}")

    def _poll(self, schedd: Any) -> tuple[str, bool]:
        """(status, whether the job is still in the queue)."""
        constraint = f"ClusterId == {self.cluster}"
        ads = list(schedd.query(constraint=constraint, projection=STATUS_ATTRS))
        in_queue = bool(ads)
        if not in_queue:
            # without a match bound the schedd scans its whole history; a driver cluster has one proc
            ads = list(schedd.history(constraint, STATUS_ATTRS, match=1))
        if not ads:
            raise RuntimeError(
                f"cluster {self.cluster} is neither in the queue of {self.schedd} nor its history"
            )
        ad = ads[0]
        if self.dag:
            return self._dag_status(schedd, ad.get("JobStatus")), in_queue
        if counts_as_alive(ad):
            return ("running" if ad.get("JobStatus") == 2 else "queued"), in_queue
        status = ad.get("JobStatus")
        if status == 4:
            return ("done" if ad.get("ExitCode") == 0 else "failed"), in_queue
        return {3: "removed", 5: "held"}.get(status, "running"), in_queue

    def _dag_status(self, schedd: Any, status: Any) -> str:
        """DAGMan's own ``JobStatus`` for removed and held; the driver node's state while DAGMan runs,
        and its latest try's exit once DAGMan ended (a try is a new cluster; ``RETRY driver 2``: three)."""
        if status in (3, 5):
            return "removed" if status == 3 else "held"
        node = f'DAGManJobId == {self.cluster} && DAGNodeName == "driver"'
        if status in (1, 2):
            ads = list(schedd.query(constraint=node, projection=["ClusterId", "JobStatus", "HoldReasonCode"]))
            if any(ad.get("JobStatus") == 5 and not counts_as_alive(ad) for ad in ads):
                return "held"
            return "running" if any(ad.get("JobStatus") == 2 for ad in ads) else "queued"
        if status == 4:
            # newest first: the first driver ad is the latest try, and a bound past the tries scans all history
            tries = list(schedd.history(node, ["ClusterId", "ExitCode"], match=1))
            latest: Mapping[str, Any] = max(tries, key=lambda ad: int(ad["ClusterId"]), default={})
            return "done" if latest.get("ExitCode") == 0 else "failed"
        return "running"

    def status(self) -> str:
        """``queued``, ``running``, ``held``, ``done``, ``removed`` or ``failed`` (a nonzero exit)."""
        return self._poll(self._located()[1])[0]

    def wait(self, timeout: float | None = None, poll_s: float = 15.0) -> str:
        """Poll until done, failed or removed; ``TimeoutError`` after ``timeout`` seconds."""
        deadline = None if timeout is None else time.monotonic() + timeout
        _, schedd = self._located()
        while (status := self._poll(schedd)[0]) not in TERMINAL:
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(f"cluster {self.cluster} is still {status} after {timeout}s")
            time.sleep(poll_s if deadline is None else min(poll_s, max(0.0, deadline - time.monotonic())))
        return status

    def result(self) -> ExecResult[Any]:
        """The run's ``ExecResult``; a failed run re-raises the driver's exception."""
        _, schedd = self._located()
        status, in_queue = self._poll(schedd)
        if status not in ("done", "failed"):
            raise RuntimeError(f"cluster {self.cluster} is {status}: it has no result yet")
        path = Path(self.log_dir) / RESULT_FILE
        if self.dag:  # never spooled: the driver node's output transfer lands it here
            if not path.is_file():
                raise RuntimeError(
                    f"DAG {self.cluster} is {status} and no driver try returned {RESULT_FILE}; "
                    f"see {DAG_FILE}.dagman.out in {self.log_dir}"
                )
        elif in_queue and SITES[self.site].spool:
            schedd.retrieve(f"ClusterId == {self.cluster}")
        # result.pkl crossed from the job's environment to this one, whose load is the authority
        blob = path.read_bytes()  # a missing file raises as itself
        try:
            ok, payload = pickle.loads(blob)
        except Exception as exc:
            raise RuntimeError(
                f"cluster {self.cluster}'s {RESULT_FILE} does not load here ({type(exc).__name__}: {exc}); "
                f"see {LOG_FILE} in {self.log_dir}"
            ) from exc
        if not ok:
            raise payload
        return cast("ExecResult[Any]", payload)

    def remove(self) -> None:
        htc, schedd = self._located()
        schedd.act(
            htc.JobAction.Remove,
            f"ClusterId == {self.cluster}",
            reason=CondorReason("graphed: driverless run removed"),
        )

    def logs(self) -> dict[str, str]:
        """The driver's logs that have come back to ``log_dir`` (a spooled job's after :meth:`result`)."""
        root = Path(self.log_dir)
        return {name: (root / name).read_text() for name in LOG_NAMES if (root / name).is_file()}

    def save(self, path: str | Path) -> None:
        Path(path).write_text(json.dumps({**asdict(self), "log_dir": str(self.log_dir)}))

    @classmethod
    def load(cls, path: str | Path) -> RunHandle:
        return cls(**json.loads(Path(path).read_text()))


def _service_nodes(
    specs: Sequence[ServiceSpec], given: Mapping[str, str], profile: SiteProfile
) -> dict[str, str]:
    """``{name: node id}`` for each spec only a SERVICE node can host: launched, given no endpoint, of a
    kind the site does not serve, and needing an image or GPUs (a driver job hosts the rest beside
    itself). Ids, not names, reach DAGMan and the three-field announce: a name is a free string."""
    names = sorted(
        spec.name
        for spec in specs
        if spec.launch is not None
        and spec.name not in given
        and spec.kind not in profile.services
        and (spec.launch.image is not None or bool(spec.launch.resources.get("gpus", 0)))
    )
    return {name: f"svc{i}" for i, name in enumerate(names)}


def _require_under(profile: SiteProfile, what: str, path: str | Path) -> None:
    """Refuse ``path`` outside the site's ``job_root``; lexical (``abspath``), so a symlink under the root
    passes whatever it names."""
    root = profile.job_root
    if root is None:
        raise ValueError(
            f"site {profile.name!r} has no job_root, a tree its jobs read directly, so {what} {path} "
            "cannot be read from a job there"
        )
    if root != "/" and not Path(os.path.abspath(path)).is_relative_to(root):
        raise ValueError(
            f"{what} {path} lies outside job_root={root} of site {profile.name!r}, the only tree its "
            f"jobs read directly: pass a {what} under {root}"
        )


def _write_sub(path: Path, desc: Mapping[str, str]) -> None:
    path.write_text("".join(f"{key} = {value}\n" for key, value in desc.items()) + "queue\n")


def submit_driverless(
    plan: Plan[Any] | DurablePlanV2,
    *,
    site: str = "generic",
    image: str | None = None,
    n_pilots: int = 2,
    pilots: str = "local",
    request_memory_mb: int,
    log_dir: str | Path | None = None,
    user_modules: Sequence[str | Path] = (),
    env: str | Path | None = None,
    extra_submit: Mapping[str, str] | None = None,
    min_pilots: int = 1,
    retries: int = 3,
    max_in_flight: int = 2,
    services: Mapping[str, str] | None = None,
) -> RunHandle:
    """Submit ``plan`` as ONE job on ``site`` whose driver runs it over ``n_pilots`` pilots:
    ``pilots="local"`` starts them in the job's own slot (``request_cpus=n_pilots``), ``pilots="condor"``
    submits them as jobs from inside the driver job (sites whose jobs can submit, with ``worker_ports``,
    from a ``log_dir`` under ``job_root``). ``log_dir`` receives the submit files and the returned
    ``result.pkl`` and ``driver.log``; ``user_modules`` and ``env`` are shipped as for
    :class:`CondorPilots`. ``services`` (service name -> ``scheme://host:port``) are the run's given
    endpoints, which the driver job checks instead of starting those services. A service only a SERVICE
    node can host makes the run a DAG in a new ``<log_dir>/graphed-<nonce>/`` (the handle's
    ``log_dir``), whose ``log_dir``, ``user_modules`` and service inputs must lie under ``job_root``.
    Everything is refused before the bindings are touched."""
    if pilots not in ("local", "condor"):
        raise ValueError(f"pilots={pilots!r}: 'local' (in the driver's slot) or 'condor' (jobs it submits)")
    endpoints = dict(services or {})
    for name, endpoint in endpoints.items():
        try:
            split_endpoint(endpoint)
        except ValueError as exc:
            raise ValueError(f"services[{name!r}]: {exc}") from None
    profile = SITES[site]
    if pilots == "condor" and not profile.jobs_can_submit:
        raise ValueError(
            f"site {site!r} does not let a job submit jobs, so its driver cannot: use pilots='local'"
        )
    _require_plan_importable(plan, ("process", "combine", "empty", "next_tasks", "stop"))
    launcher = CondorPilots(
        profile,
        image=image,
        request_cpus=n_pilots if pilots == "local" else 1,
        request_memory_mb=request_memory_mb,
        log_dir=log_dir,
        user_modules=user_modules,
        env=env,
        extra_submit=extra_submit,
    )
    launcher._refuse()
    for module in launcher.user_modules:
        if "," in module:
            raise ValueError(f"user_modules: {module} holds ',', which splits the job's input list")
    announce_only = _service_nodes(plan.services, endpoints, profile)
    if profile.worker_ports is None and (pilots == "condor" or announce_only):
        who = (
            "pilots a driver job submits"
            if pilots == "condor"
            else f"the SERVICE nodes of {sorted(announce_only)}"
        )
        raise ValueError(
            f"site {site!r} has no worker_ports: {who} cannot reach the driver job's task server"
        )
    out = Path(
        os.path.abspath(log_dir or tempfile.mkdtemp(prefix="graphed-driverless-", dir=launcher._sandbox()))
    )
    with ExitStack() as refused:  # a refusal leaves no temporary log_dir behind
        if not log_dir:
            refused.callback(out.rmdir)
        if pilots == "condor" or announce_only:
            _require_under(profile, "log_dir", out)
        nonce = uuid.uuid4().hex[:8]
        run_dir = out / f"graphed-{nonce}" if announce_only else out
        by_name = {spec.name: spec for spec in plan.services}
        nodes = {
            node: ServiceJob(by_name[name], launcher, key=node, watch=str(run_dir))
            for name, node in announce_only.items()
        }
        if nodes:  # unspooled: the schedd reads each of these where it lies
            for module in launcher.user_modules:
                _require_under(profile, "user_modules", module)
            for job in nodes.values():
                for path in job.inputs:
                    _require_under(profile, f"service {job.spec.name!r} input", path)
        refused.pop_all()
    out.mkdir(parents=True, exist_ok=True)
    if nodes:
        run_dir.mkdir()  # new per run: no DAG reads another run's files
    launcher.log_dir = run_dir
    (run_dir / PLAN_FILE).write_bytes(pickle.dumps(plan))
    script = launcher._stage(run_dir, "driver.sh", DRIVER_MODULE, PLACEHOLDER)
    inputs = [PLAN_FILE, RUN_FILE, *([ENV_FILE] if profile.ship_env else []), *launcher.user_modules]
    base = {
        "executable": str(script),
        "arguments": ".",
        "output": "driver.out",
        "error": "driver.err",
        "log": "driver.condor.log",
        "transfer_input_files": ",".join(inputs),
        "transfer_output_files": f"{RESULT_FILE},{LOG_FILE}",
        "JobBatchName": f"graphed-driverless-{nonce}",
    }
    if not nodes:  # a DAG's RETRY is its driver node's one retry owner
        # exit 3 is a plan error: deterministic, so it must not be retried
        base.update(max_retries="2", retry_until="3")
    desc = launcher.submit_description("", 1, base)
    htc = launch._htcondor()
    name, schedd = launcher._choose(htc)
    run = {
        "pilots": pilots,
        "n_pilots": n_pilots,
        "site": site,
        "image": image,
        "log_dir": str(run_dir / PILOTS_SUBDIR),
        "request_memory_mb": request_memory_mb,
        "min_pilots": min_pilots,
        "retries": retries,
        "max_in_flight": max_in_flight,
        # a DAG's driver also reads its SERVICE nodes' queue ads there
        "schedd_locate": [str(htc.param["COLLECTOR_HOST"]), name]
        if pilots == "condor" or (nodes and profile.jobs_can_submit)
        else None,
        "user_modules": [Path(m).name for m in launcher.user_modules],
        "endpoints": endpoints,
        "announce_only": announce_only,
        "dag_dir": str(run_dir) if nodes else None,
        "extra_submit": launcher.extra_submit,
    }
    (run_dir / RUN_FILE).write_text(json.dumps(run, indent=1))
    if not nodes:
        with ExitStack() as on_error:  # a failed spool must not leave the job queued
            result = launcher._submit(htc, schedd, desc, 1, on_error)
            on_error.pop_all()  # submitted: the job outlives this session
    else:
        _write_sub(run_dir / "driver.sub", desc)
        lines = ["JOB driver driver.sub"]
        for node, job in nodes.items():
            (run_dir / f"service-{node}").mkdir()
            keys = job.files(run_dir / f"service-{node}")
            # a held node frees its slot (a GPU, say) at once; the user's extra_submit still wins
            _write_sub(
                run_dir / f"{node}.sub",
                {**keys, "periodic_remove": "JobStatus == 5", **launcher.extra_submit},
            )
            lines.append(f"SERVICE {node} {node}.sub")
        lines.append("RETRY driver 2 UNLESS-EXIT 3")
        (run_dir / DAG_FILE).write_text("\n".join(lines) + "\n")
        # never spooled: the run dir lies under job_root, which the schedd reads
        # from_dag names the condor_dagman on this host's PATH; the scheduler universe runs the schedd's
        bindir = htc.RemoteParam(htc.Collector().locate(htc.DaemonType.Schedd, name))["BIN"]
        options = {**DAG_OPTIONS, "dagman": f"{bindir}/condor_dagman"}
        launch.ensure_credential(htc, desc)  # the DAG's own description sends none; its nodes do
        result = schedd.submit(htc.Submit.from_dag(str(run_dir / DAG_FILE), options))
    return RunHandle(
        site=site,
        schedd=name,
        cluster=int(result.cluster()),
        log_dir=run_dir,
        submitted_at=time.time(),
        dag=bool(nodes),
    )
