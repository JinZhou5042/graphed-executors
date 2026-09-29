"""Driverless runs: :func:`submit_driverless` ships a plan in ONE HTCondor job whose driver
(:mod:`graphed_executors.htcondor_backend.driver`) runs it, so the run needs no login session;
:class:`RunHandle` tracks that job and collects its ``result.pkl``, from this session or a later one.
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
from graphed.services import split_endpoint

from . import launch
from .backend import _require_importable
from .launch import ENV_FILE, LOG_FILE, PLAN_FILE, RESULT_FILE, RUN_FILE, CondorPilots, collectors
from .sites import SITES, counts_as_alive

DRIVER_MODULE = "graphed_executors.htcondor_backend.driver"
PILOTS_SUBDIR = "pilots"
TERMINAL = ("done", "failed", "removed")
STATUS_ATTRS = ["JobStatus", "HoldReasonCode", "ExitCode"]
LOG_NAMES = (LOG_FILE, "driver.out", "driver.err", "driver.condor.log")

# where a site's jobs can submit from: their inner pilots need an initialdir the schedd reads
_SELF_SUBMIT_ROOT = {"lxplus": "/afs"}


@dataclass(frozen=True)
class RunHandle:
    """A submitted driverless job: ``cluster`` on the schedd named ``schedd`` of ``site``, its outputs
    returning to ``log_dir``. Every call locates that schedd by name, never the local default."""

    site: str
    schedd: str
    cluster: int
    log_dir: str | Path
    submitted_at: float

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
        if counts_as_alive(ad):
            return ("running" if ad.get("JobStatus") == 2 else "queued"), in_queue
        status = ad.get("JobStatus")
        if status == 4:
            return ("done" if ad.get("ExitCode") == 0 else "failed"), in_queue
        return {3: "removed", 5: "held"}.get(status, "running"), in_queue

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
        if in_queue and SITES[self.site].spool:
            schedd.retrieve(f"ClusterId == {self.cluster}")
        # result.pkl crossed from the job's environment to this one, whose load is the authority
        blob = (Path(self.log_dir) / RESULT_FILE).read_bytes()  # a missing file raises as itself
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
            htc.JobAction.Remove, f"ClusterId == {self.cluster}", reason="graphed: driverless run removed"
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


def submit_driverless(
    plan: Plan[Any],
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
    submits them as jobs from inside the driver job (sites whose jobs can submit, with ``worker_ports``).
    ``log_dir`` receives the submit files and the returned ``result.pkl`` and ``driver.log``;
    ``user_modules`` and ``env`` are shipped as for :class:`CondorPilots`. ``services`` (service name ->
    ``scheme://host:port``) are the run's given endpoints, which the driver job checks instead of
    starting those services. Everything is refused before the bindings are touched."""
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
    for role in ("process", "combine", "empty", "next_tasks", "stop"):
        if (part := getattr(plan, role)) is not None:
            _require_importable(part, role)
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
    if pilots == "condor":
        if profile.worker_ports is None:
            raise ValueError(
                f"site {site!r} has no worker_ports: pilots a driver job submits cannot reach its task "
                "server; use pilots='local'"
            )
        root = _SELF_SUBMIT_ROOT.get(profile.name)
        if root is not None and (log_dir is None or not Path(os.path.abspath(log_dir)).is_relative_to(root)):
            raise ValueError(
                f"site {site!r} submits pilots from a job only with an initialdir under {root}: "
                f"pass log_dir=<a directory under {root}> (got {log_dir})"
            )
    out = Path(
        os.path.abspath(log_dir or tempfile.mkdtemp(prefix="graphed-driverless-", dir=launcher._sandbox()))
    )
    out.mkdir(parents=True, exist_ok=True)
    launcher.log_dir = out
    (out / PLAN_FILE).write_bytes(pickle.dumps(plan))
    script = launcher._stage(out, "driver.sh", DRIVER_MODULE)
    inputs = [PLAN_FILE, RUN_FILE, *([ENV_FILE] if profile.ship_env else []), *launcher.user_modules]
    desc = launcher.submit_description(
        "",
        1,
        {
            "executable": str(script),
            "arguments": ".",
            "output": "driver.out",
            "error": "driver.err",
            "log": "driver.condor.log",
            "transfer_input_files": ",".join(inputs),
            "transfer_output_files": f"{RESULT_FILE},{LOG_FILE}",
            # exit 3 is a plan error: deterministic, so it must not be retried
            "max_retries": "2",
            "retry_until": "3",
            "JobBatchName": f"graphed-driverless-{uuid.uuid4().hex[:8]}",
        },
    )
    htc = launch._htcondor()
    name, schedd = launcher._choose(htc)
    run = {
        "pilots": pilots,
        "n_pilots": n_pilots,
        "site": site,
        "image": image,
        "log_dir": str(out / PILOTS_SUBDIR),
        "request_memory_mb": request_memory_mb,
        "min_pilots": min_pilots,
        "retries": retries,
        "max_in_flight": max_in_flight,
        "schedd_locate": [str(htc.param["COLLECTOR_HOST"]), name] if pilots == "condor" else None,
        "user_modules": [Path(m).name for m in launcher.user_modules],
        "endpoints": endpoints,
    }
    (out / RUN_FILE).write_text(json.dumps(run, indent=1))
    with ExitStack() as on_error:  # a failed spool must not leave the job queued
        result = launcher._submit(htc, schedd, desc, 1, on_error)
        on_error.pop_all()  # submitted: the job outlives this session
    return RunHandle(
        site=site, schedd=name, cluster=int(result.cluster()), log_dir=out, submitted_at=time.time()
    )
