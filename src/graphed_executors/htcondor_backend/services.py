"""Cluster-hosted services: a :class:`ServiceJob` is one HTCondor job, beside the pilots' cluster, that runs
a service recipe and announces where it listens.

The job runs ``service.sh``, which execs the stdlib-only ``announce.py`` (a copy of
:mod:`graphed_executors.htcondor_backend.announce`) on ``service.json``. The recipe's child starts in
``service/``, a directory built on the submit side with one entry per recipe input, named by its
basename (a file as a symlink to it, a directory as a real tree of file symlinks), and transferred as
that one directory, so the child's cwd holds exactly its inputs whatever else lands in the scratch dir.
An attached job carries a per-call announce secret, never the pilots' secret; a watch-mode job (a DAG's
SERVICE node) carries none and reads the driver's url and secret from the watched directory instead.
"""

from __future__ import annotations

import json
import os
import shlex
import shutil
import threading
from collections.abc import Mapping, Sequence
from contextlib import ExitStack
from pathlib import Path
from typing import Any

from graphed.services import ServiceSpec

from . import launch
from . import server as _server
from .launch import ENV_FILE, SECRET_FILE, SLOT_RESOURCES, CondorPilots, write_secret

ANNOUNCE_SOURCE = Path(__file__).with_name("announce.py")
RUN_DIR = "service"
AD_ATTRS = ["JobStatus", "ExitCode", "HoldReasonCode", "HoldReason"]
_REQUESTS = ("RequestMemory", "RequestCpus", "RequestGPUs")
VACATE_S = 30  # a removed service frees its slot (and GPU) this soon, whatever the EP's own vacate time


class ServiceJob:
    """``spec``'s recipe as one job submitted through ``launcher``'s schedd, announcing under ``key``.

    Attached: it announces to ``url`` (the task server) signed with ``secret``. Watch mode (``watch`` a
    directory, ``url`` and ``secret`` unset): it announces to what that directory names. Inputs are
    resolved against this process's cwd; one that is missing, shares another's basename, or is or holds
    a symlink to a directory (which HTCondor holds a job on) is refused here, naming it."""

    def __init__(
        self,
        spec: ServiceSpec,
        launcher: CondorPilots,
        *,
        key: str,
        url: str | None = None,
        secret: bytes | None = None,
        watch: str | None = None,
    ) -> None:
        if spec.launch is None:
            raise ValueError(f"service {spec.name!r} has no launch recipe to run on the cluster")
        if (watch is None) != (url is not None and secret is not None):
            raise ValueError("a ServiceJob announces either to url with secret, or to what watch names")
        if launcher.profile.worker_ports is None:
            raise ValueError(f"site {launcher.profile.name!r} has no worker_ports for a service to bind")
        self.spec = spec
        self.launch = spec.launch
        self.launcher = launcher
        self.key = key
        self.url = url
        self.secret = secret
        self.watch = watch
        self.inputs = _checked_inputs(self.launch.inputs)
        self.dir: Path | None = None
        self.cluster: int | None = None
        self._stack = ExitStack()
        self._lock = threading.Lock()  # one submit or stop at a time: a stop waits for a submit in flight
        self._stopped = False

    def files(self, dir: Path) -> dict[str, str]:
        """Write the job's files into ``dir`` and return its submit keys; submits nothing."""
        launcher, profile, image = self.launcher, self.launcher.profile, self.launch.image
        python = "python3" if image is not None else launcher._job_python
        quoted = shlex.quote(python)
        (dir / "service.sh").write_text(
            f"#!/bin/sh\n[ -f {ENV_FILE} ] && tar xzf {ENV_FILE}\n"
            f"command -v {quoted} >/dev/null 2>&1 || {{ echo service.sh: no interpreter {quoted} >&2; exit 3; }}\n"
            f'exec {quoted} announce.py "$@"\n'
        )
        (dir / "service.sh").chmod(0o755)
        shutil.copyfile(ANNOUNCE_SOURCE, dir / "announce.py")
        config = {
            "argv": list(self.launch.argv),
            "env": dict(self.launch.env),
            "check": self.spec.check,
            "ports": list(profile.worker_ports or ()),
            "key": self.key,
            "url": self.url,
            "watch": self.watch,
            "python": python,
            "timeout_s": self.spec.timeout_s,
            "lease_s": _server.LEASE_S,
            "beat_s": _server.POLL_S,
        }
        (dir / "service.json").write_text(json.dumps(config))
        inputs = ["announce.py", "service.json"]
        if self.secret is not None:
            write_secret(dir / SECRET_FILE, self.secret)
            inputs.append(SECRET_FILE)
        if profile.ship_env and image is None:
            assert launcher.log_dir is not None, "the pilots' env.tgz is staged when they start"
            # the pilots' own tarball, linked: condor follows a file symlink, spooled or not
            os.symlink(launcher.log_dir / ENV_FILE, dir / ENV_FILE)
            inputs.append(ENV_FILE)
        if self.inputs:
            _mirror(self.inputs, dir / RUN_DIR)
            inputs.append(RUN_DIR)  # relative to initialdir, so a ',' in dir splits nothing
        resources = self.launch.resources
        base = {
            "executable": str(dir / "service.sh"),
            "arguments": "service.json",  # arbitrary argv would not survive condor's quoting
            "output": "service.out",
            "error": "service.err",
            "log": "service.log",
            "transfer_input_files": ",".join(inputs),
            "request_cpus": str(int(resources.get("cpus", 1))),
            "request_memory": str(int(resources.get("memory_mb", launcher.request_memory_mb))),
            **({"request_gpus": str(int(resources["gpus"]))} if resources.get("gpus", 0) > 0 else {}),
            "JobBatchName": f"graphed-service-{self.key}",
            "job_max_vacate_time": str(VACATE_S),
        }
        desc = launcher.submit_description(self.url or "", 1, base)
        desc["initialdir"] = str(dir)
        if self.watch is None:  # attached: its inputs arrive by transfer, so it needs no credential
            desc.pop("MY.SendCredential", None)
        if image is not None:
            desc["MY.SingularityImage"] = f'"{image}"'
        desc.update(launcher.extra_submit)  # the user's keys still have the last word
        return desc

    def submit(self) -> None:
        """Write the files into a new ``service-<key>/`` under the launcher's ``log_dir`` and submit one
        job; its removal is registered the moment ``schedd.submit`` returns, so a failed spool leaves
        none. A job already stopped is not submitted."""
        launcher = self.launcher
        assert launcher._schedd is not None and launcher.log_dir is not None, (
            "start the launcher before a service job: it submits to the pilots' schedd"
        )
        self.dir = launcher.log_dir / f"service-{self.key}"
        self.dir.mkdir()  # the key is per call: no call reuses another's directory
        desc = self.files(self.dir)
        htc = launch._htcondor()
        with self._lock, ExitStack() as stack:
            if self._stopped:
                raise RuntimeError(f"service job {self.key} was stopped before it was submitted")
            result = launcher._submit(htc, launcher._schedd, desc, 1, stack)
            self.cluster = int(result.cluster())
            self._stack = stack.pop_all()

    def ad(self) -> Any:
        """The job's queue ad, else its history ad once it left the queue, else ``{}``."""
        schedd, constraint = self.launcher._schedd, f"ClusterId == {self.cluster}"
        ads = list(schedd.query(constraint=constraint, projection=AD_ATTRS))
        if not ads:
            ads = list(schedd.history(constraint, AD_ATTRS, match=1))
        return ads[0] if ads else {}

    def match_refusal(
        self, machines: list[Any], claims: Mapping[str, Sequence[Any]] | None = None
    ) -> str | None:
        """:func:`queued_refusal` of this queued job (its claims, by holder, the running ads of the runner's
        pilots, which keep their slots until the runner closes, and of the set's earlier servers, which
        keep theirs until the run ends); ``None`` when ``machines`` is empty or the job left the queue."""
        if not machines:  # a collector that lists no slot says nothing about the pool: submit and wait
            return None
        ads = list(self.launcher._schedd.query(constraint=f"ClusterId == {self.cluster}"))
        if not ads:  # it already left the queue: the announce wait reports how
            return None
        return queued_refusal(f"service job {self.key}", ads[0], machines, claims)

    def stop(self) -> None:
        """Remove the job at once (a service never exits by itself; a spooled job that completed is
        retrieved first, so its ``service.out``/``.err`` come back), then drop its secret file. Calls
        from any threads are serialized and only the first does that: each returns once it has run."""
        with self._lock:
            if self._stopped:  # Windows refuses an unlink racing another one: access denied
                return
            self._stopped = True
            self._stack.close()
            if self.dir is not None:
                (self.dir / SECRET_FILE).unlink(missing_ok=True)


def queued_refusal(
    what: str, ad: Any, machines: list[Any], claims: Mapping[str, Sequence[Any]] | None = None
) -> str | None:
    """Why no slot of ``machines`` could ever run the queued job whose whole ad is ``ad`` (``what`` names
    it), else ``None``: the ad (the request, the site's and the user's submit keys) must
    ``symmetricMatch`` a slot's ad whose free ``Memory``/``Cpus``/``GPUs``/``Disk`` are a partitionable
    slot's totals, so a busy pool still matches, less what ``claims`` (running ads, by holder) hold there."""
    import classad2  # noqa: PLC0415  (ships with the htcondor2 bindings)

    slots = [_as_whole(classad2.ClassAd(str(machine))) for machine in machines]
    held = {who: running for who, running in (claims or {}).items() if running}
    for running in held.values():
        for claim in running:
            _less_claim(slots, claim)
    if any(ad.symmetricMatch(slot) for slot in slots):
        return None
    asked = ", ".join(f"{a}={ad.eval(a) if a in ad else 0}" for a in _REQUESTS)
    largest = max(int(slot.get("Memory", 0)) for slot in slots)
    if held:
        return (
            f"{what} matches no slot of the pool beside {' and '.join(held)}: {asked}; "
            f"the largest slot memory beside them is {largest} MiB"
        )
    return (
        f"{what} matches no slot of the pool, busy or not: {asked}; the largest slot memory is {largest} MiB"
    )


def machine_ads(locate: tuple[str, str] | None) -> list[Any]:
    """The pool's slot ads, dynamic slots dropped, from the collector of the schedd ``locate = (pool,
    name)`` names (``None``: the default collector)."""
    htc = launch._htcondor()
    collector = htc.Collector(locate[0]) if locate is not None else htc.Collector()
    # in the collector, where dynamic slots can be most of a pool; `=!=` keeps an ad with no SlotType
    slots = 'MyType == "Machine" && SlotType =!= "Dynamic"'
    return list(collector.query(constraint=slots))  # whole ads: the job's Requirements may read any attribute


def _as_whole(slot: Any) -> Any:
    """``slot`` (a copy) as it would be with nothing running: a partitionable slot's totals as its free
    resources."""
    if slot.get("PartitionableSlot"):
        for total, free in (
            ("TotalSlotMemory", "Memory"),
            ("TotalSlotCpus", "Cpus"),
            ("TotalSlotGPUs", "GPUs"),
            ("TotalSlotDisk", "Disk"),
        ):
            if total in slot:
                slot[free] = slot[total]
    return slot


def _less_claim(slots: list[Any], claim: Any) -> None:
    """Take a running job's share (``<r>Provisioned``, else ``Request<r>`` evaluated in its ad) out of
    the slot it runs in: ``RemoteHost`` names its dynamic slot ``slotN_M@host``, whose parent is
    ``slotN@host``, or a static slot itself."""
    remote = str(claim.get("RemoteHost", ""))
    name, at, host = remote.partition("@")
    parent = name.rsplit("_", 1)[0] + at + host
    for slot in slots:
        if slot.get("Name") in (remote, parent):
            for r in SLOT_RESOURCES:
                held = next((a for a in (f"{r}Provisioned", f"Request{r}") if a in claim), None)
                if r in slot and held is not None:
                    slot[r] = int(slot[r]) - int(claim.eval(held))  # RequestDisk is an expression


def _checked_inputs(inputs: tuple[str, ...]) -> list[str]:
    paths = [os.path.abspath(p) for p in inputs]
    seen: dict[str, str] = {}
    for path in paths:
        if not os.path.exists(path):
            raise ValueError(f"service input {path} does not exist")
        name = os.path.basename(path)
        if name in seen:
            raise ValueError(f"service inputs {seen[name]} and {path} share the name {name!r}")
        seen[name] = path
        for link in [path, *_dir_links(path)]:
            if os.path.islink(link) and os.path.isdir(link):
                raise ValueError(f"service input {link} is a symlink to a directory, which HTCondor refuses")
    return paths


def _dir_links(path: str) -> list[str]:
    """The directory symlinks under ``path``, found without following any."""
    return [
        os.path.join(root, d)
        for root, dirs, _ in os.walk(path)
        for d in dirs
        if os.path.islink(os.path.join(root, d))
    ]


def _mirror(inputs: list[str], dest: Path) -> None:
    """``dest`` holding each input by its basename: a file as a symlink, a directory as a real tree
    whose files are symlinks."""
    dest.mkdir()
    for src in inputs:
        target = dest / os.path.basename(src)
        if not os.path.isdir(src):
            os.symlink(src, target)
            continue
        for root, _, files in os.walk(src):
            here = target / os.path.relpath(root, src)
            here.mkdir(parents=True, exist_ok=True)
            for name in files:
                os.symlink(os.path.join(root, name), here / name)


__all__ = ["ServiceJob", "machine_ads", "queued_refusal"]
