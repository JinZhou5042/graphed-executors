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
from contextlib import ExitStack
from pathlib import Path
from typing import Any

from graphed.services import ServiceSpec

from . import launch
from . import server as _server
from .launch import ENV_FILE, SECRET_FILE, CondorPilots, write_secret

ANNOUNCE_SOURCE = Path(__file__).with_name("announce.py")
RUN_DIR = "service"
AD_ATTRS = ["JobStatus", "ExitCode", "HoldReasonCode", "HoldReason"]
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
        none."""
        launcher = self.launcher
        assert launcher._schedd is not None and launcher.log_dir is not None, (
            "start the launcher before a service job: it submits to the pilots' schedd"
        )
        self.dir = launcher.log_dir / f"service-{self.key}"
        self.dir.mkdir()  # the key is per call: no call reuses another's directory
        desc = self.files(self.dir)
        htc = launch._htcondor()
        with ExitStack() as stack:
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

    def stop(self) -> None:
        """Remove the job at once (a service never exits by itself; a spooled job that completed is
        retrieved first, so its ``service.out``/``.err`` come back), then drop its secret file."""
        self._stack.close()
        if self.dir is not None:
            (self.dir / SECRET_FILE).unlink(missing_ok=True)


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


__all__ = ["ServiceJob"]
