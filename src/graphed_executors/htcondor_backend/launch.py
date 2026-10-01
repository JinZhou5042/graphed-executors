"""Pilot launchers: :class:`CondorPilots` submits pilot jobs through the ``htcondor2`` bindings, and
:class:`LocalPilots` runs the same pilot program as local subprocesses (a laptop dry run of the wire path).

A pilot is ``python -m graphed_executors.htcondor_backend.pilot <url> <secret file>``. The secret travels
as a transferred file, never in ``arguments`` or ``environment``: both are readable by anyone who can
query the job ad. ``htcondor2`` is imported only by :func:`_htcondor`, at ``CondorPilots.start``.

Each launcher registers the release of what it acquires the moment the acquisition returns (a pilot
process once spawned, a cluster once ``schedd.submit`` returns, before its sandbox spools), so a start
that fails part-way leaves nothing behind, and ``stop`` is the close of what a start kept. Each release
logs its failure and never raises, so one failing release never skips the next.
"""

from __future__ import annotations

import getpass
import json
import os
import re
import shutil
import subprocess
import sys
import sysconfig
import tarfile
import tempfile
import time
import uuid
from collections.abc import Mapping, Sequence
from contextlib import ExitStack, suppress
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from graphed_executors.submit.services import release_quietly

from .server import POLL_S
from .sites import SITES, WEIGHT_ATTRS, SiteProfile, choose_schedd, counts_as_alive

CLOSE_WAIT_S = 2 * POLL_S  # every pilot has polled /next, and seen the server's 410, by then

SECRET_FILE = "graphed-secret"
ENV_FILE = "env.tgz"
# a driverless job's files; here, not in driver.py, so importing the package never imports the -m entry
PLAN_FILE, RUN_FILE, RESULT_FILE, LOG_FILE = "plan.pkl", "run.json", "result.pkl", "driver.log"
PILOT_MODULE = "graphed_executors.htcondor_backend.pilot"


def _htcondor() -> Any:
    """The ``htcondor2`` module, imported on first use."""
    try:
        import htcondor2  # noqa: PLC0415  (lazy: the bindings are the optional extra)
    except ImportError as exc:  # pragma: no cover - the htcondor CI job has the bindings installed
        raise ImportError(
            "CondorPilots needs the htcondor bindings (Linux wheels only): "
            "pip install 'graphed-executors[htcondor]'"
        ) from exc
    return htcondor2


STORE_CREDENTIAL = "$(condor_config_val SEC_CREDENTIAL_PRODUCER) | condor_store_cred add-krb -i -"


def ensure_credential(htc: Any, desc: Mapping[str, str]) -> None:
    """Store the submitter's Kerberos credential, as ``condor_submit`` does and ``schedd.submit`` does not,
    when ``desc`` sends one and the credd holds none: such a job never starts."""
    if str(desc.get("MY.SendCredential", "")).lower() != "true":
        return
    # asked first: issue_credentials fails where the producer is missing (an image), stored or not
    if htc.Credd().query_user_cred(htc.CredType.Kerberos) is not None:
        return
    try:
        htc.Submit(dict(desc)).issue_credentials()
    except Exception as exc:
        raise RuntimeError(
            f"the credd holds no Kerberos credential of yours and storing one here failed ({exc}); "
            f"run on the login host, outside any container: {STORE_CREDENTIAL}"
        ) from exc


def publish(path: Path, text: str) -> None:
    """Replace ``path`` with ``text`` at once, readable by this user only (``mkstemp``'s mode)."""
    fd, tmp = tempfile.mkstemp(dir=path.parent)
    with os.fdopen(fd, "w") as f:
        f.write(text)
    os.replace(tmp, path)


def write_secret(path: Path, secret: bytes) -> None:
    publish(path, secret.hex())


@runtime_checkable
class PilotLauncher(Protocol):
    """Starts ``n`` pilots that call back to ``url`` with ``secret``, counts the live ones, and removes them."""

    def start(self, url: str, secret: bytes, n: int) -> None: ...

    def alive(self) -> int: ...

    def stop(self) -> None: ...


class LocalPilots:
    """Pilots as local subprocesses of ``python``, with ``pythonpath`` prepended to their ``PYTHONPATH``."""

    def __init__(self, *, python: str = sys.executable, pythonpath: Sequence[str | Path] = ()) -> None:
        self.python = python
        self.pythonpath = [str(p) for p in pythonpath]
        self.log_dir = Path(tempfile.mkdtemp(prefix="graphed-local-pilots-"))
        self._procs: list[subprocess.Popen[bytes]] = []
        self._stack = ExitStack()
        self._stop_by = 0.0  # until then a stopping pilot may exit on its own; a failed start: at once

    def start(self, url: str, secret: bytes, n: int) -> None:
        secret_path = self.log_dir / SECRET_FILE
        write_secret(secret_path, secret)
        env = dict(os.environ)
        env["PYTHONPATH"] = os.pathsep.join([*self.pythonpath, env.get("PYTHONPATH", "")]).rstrip(os.pathsep)
        cmd = [self.python, "-m", PILOT_MODULE, url, str(secret_path)]
        self._procs = []
        with ExitStack() as stack:
            for _ in range(n):
                proc = subprocess.Popen(cmd, env=env)
                stack.callback(release_quietly, f"pilot pid {proc.pid}", self._stop_pilot, proc)
                self._procs.append(proc)
            self._stack = stack.pop_all()

    def _stop_pilot(self, proc: subprocess.Popen[bytes]) -> None:
        with suppress(subprocess.TimeoutExpired):
            proc.wait(max(0.0, self._stop_by - time.monotonic()))
        proc.terminate()  # a no-op once it exited
        proc.wait()

    def alive(self) -> int:
        return sum(p.poll() is None for p in self._procs)

    def stop(self) -> None:
        self._stop_by = time.monotonic() + CLOSE_WAIT_S
        self._stack.close()
        shutil.rmtree(self.log_dir, ignore_errors=True)


class CondorPilots:
    """Pilot jobs on an HTCondor pool, one cluster of ``n`` jobs per :meth:`start`.

    ``log_dir`` holds the submit-side files (the secret, ``pilot.sh``, ``env.tgz``) and receives the
    pilots' ``pilot.<n>.out``/``.err`` and ``pilots.log``; ``None`` makes a fresh directory under the
    site's sandbox root, else a temporary one. ``user_modules`` are files or packages transferred into
    each pilot's scratch dir, which is on ``sys.path`` because the pilot runs with ``python -m``.
    ``schedd_locate=(pool, name)`` submits to that schedd, found through that collector: inside a job
    there is no local schedd for the site's default choice to find.
    """

    def __init__(
        self,
        site: str | SiteProfile = "generic",
        *,
        image: str | None = None,
        request_cpus: int = 1,
        request_memory_mb: int = 2048,
        log_dir: str | Path | None = None,
        user_modules: Sequence[str | Path] = (),
        env: str | Path | None = None,
        extra_submit: Mapping[str, str] | None = None,
        schedd_locate: tuple[str, str] | None = None,
    ) -> None:
        self.profile = SITES[site] if isinstance(site, str) else site
        self.image = image
        self.request_cpus = request_cpus
        self.request_memory_mb = request_memory_mb
        self.log_dir = Path(log_dir) if log_dir is not None else None
        self.user_modules = [os.path.abspath(m) for m in user_modules]
        self.env = Path(env) if env is not None else Path(sys.prefix)
        self.extra_submit = dict(extra_submit or {})
        self.schedd_locate = schedd_locate
        self.cluster: tuple[str, int] | None = None  # (schedd name, ClusterId): the choice varies per run
        self._schedd: Any = None
        self._constraint = ""
        self._secret = Path(SECRET_FILE)
        self._batch = f"graphed-pilots-{uuid.uuid4().hex[:8]}"
        self._stack = ExitStack()

    def _template_vars(self) -> dict[str, str]:
        return {
            "image": self.image or "",
            "uid": str(os.getuid()) if hasattr(os, "getuid") else "",
            "user": getpass.getuser(),
            "home": os.path.expanduser("~"),
        }

    def submit_description(self, url: str, n: int, base: Mapping[str, str] | None = None) -> dict[str, str]:
        """The submit keys for ``n`` pilots calling back to ``url``: the base keys updated by ``base``,
        then the site's, then ``extra_submit``. Pure: it reads no file and needs no bindings."""
        inputs = [SECRET_FILE, *([ENV_FILE] if self.profile.ship_env else []), *self.user_modules]
        desc = {
            "universe": "vanilla",
            "executable": "pilot.sh",
            "arguments": f"{url} {SECRET_FILE}",
            "output": "pilot.$(ProcId).out",
            "error": "pilot.$(ProcId).err",
            "log": "pilots.log",
            "should_transfer_files": "YES",
            "when_to_transfer_output": "ON_EXIT_OR_EVICT",
            "transfer_input_files": ",".join(inputs),
            # nothing comes back but the logs: the unpacked venv must not, and lxplus refuses a spooled
            # job that sets neither this nor output_destination
            "transfer_output_files": '""',
            "request_cpus": str(self.request_cpus),
            "request_memory": str(self.request_memory_mb),
            "JobBatchName": self._batch,
        }
        desc.update(base or {})
        if self.log_dir is not None:
            desc["initialdir"] = str(self.log_dir)
        subs = self._template_vars()
        desc.update({k: v.format(**subs) for k, v in self.profile.submit.items()})
        desc.update(self.extra_submit)
        return desc

    def _sandbox(self) -> str | None:
        root = self.profile.sandbox_root
        return root.format(user=getpass.getuser()) if root is not None else None

    def _refuse(self) -> None:
        profile = self.profile
        if self.image is None and any("{image}" in v for v in profile.submit.values()):
            raise ValueError(f"site {profile.name!r} runs pilots in a container: pass image=<path or URL>")
        root = self._sandbox()
        log_dir = self.log_dir
        if (
            root is not None
            and log_dir is not None
            and not Path(os.path.abspath(log_dir)).is_relative_to(root)
        ):
            raise ValueError(
                f"site {profile.name!r} can only read submit files under {root}: log_dir={log_dir} lies outside it"
            )
        if profile.ship_env:
            _check_shippable(self.env)

    def start(self, url: str, secret: bytes, n: int) -> None:
        self._refuse()
        # absolute in the cwd the files are written from: stop() and a service job may read it from another
        self.log_dir = Path(
            os.path.abspath(self.log_dir or tempfile.mkdtemp(prefix="graphed-pilots-", dir=self._sandbox()))
        )
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self._secret = self.log_dir / SECRET_FILE
        write_secret(self._secret, secret)
        script = self._stage(self.log_dir, "pilot.sh", PILOT_MODULE)
        # a relative executable resolves against our cwd, not initialdir
        desc = self.submit_description(url, n, {"executable": str(script)})
        htc = _htcondor()
        name, schedd = self._choose(htc)
        with ExitStack() as stack:
            result = self._submit(htc, schedd, desc, n, stack)
            self.cluster = (name, int(result.cluster()))
            self._constraint = f"ClusterId == {self.cluster[1]}"
            self._schedd = schedd
            self._stack = stack.pop_all()

    @property
    def _job_python(self) -> str:
        """The interpreter this launcher's jobs run: the shipped venv's, else this process's."""
        return "./env/bin/python" if self.profile.ship_env else sys.executable

    def _stage(self, log_dir: Path, script_name: str, module: str, placeholder: bytes | None = None) -> Path:
        """Write the job script running ``python -m <module> "$@"`` and, when the site ships it, ``env.tgz``.
        ``placeholder`` (the driver's) is written as ``result.pkl``, and ``driver.log`` created, before the
        interpreter starts: a job whose declared outputs are missing at exit is held, whatever killed it."""
        pre = ""
        if placeholder is not None:
            octal = "".join(f"\\{byte:03o}" for byte in placeholder)
            pre = f"printf '{octal}' > {RESULT_FILE}\n: >> {LOG_FILE}\n"
        script = log_dir / script_name
        script.write_text(
            f"#!/bin/sh\n{pre}[ -f {ENV_FILE} ] && tar xzf {ENV_FILE}\n"
            f'exec {self._job_python} -m {module} "$@"\n'
        )
        script.chmod(0o755)
        if self.profile.ship_env:
            with tarfile.open(log_dir / ENV_FILE, "w:gz") as tar:
                tar.add(self.env, arcname="env")
        return script

    def _submit(self, htc: Any, schedd: Any, desc: Mapping[str, str], n: int, stack: ExitStack) -> Any:
        """Submit ``n`` jobs of ``desc``, spooling their sandbox where the site needs it. The cluster's
        removal goes on ``stack`` as soon as ``schedd.submit`` returns, so a failed spool leaves none."""
        # schedd_locate is a job's case: no local credd to ask, and the running job keeps one stored
        if self.schedd_locate is None:
            ensure_credential(htc, desc)
        result = schedd.submit(htc.Submit(dict(desc)), count=n, spool=self.profile.spool)
        constraint = f"ClusterId == {int(result.cluster())}"
        stack.callback(release_quietly, f"cluster {constraint}", self._remove, htc, schedd, constraint)
        if self.profile.spool:
            schedd.spool(result)
        return result

    def _remove(self, htc: Any, schedd: Any, constraint: str) -> None:
        """Fetch a spooled cluster's finished logs into ``log_dir`` and remove what is still queued."""
        left = schedd.query(constraint=constraint, projection=["JobStatus"])
        if left:
            if self.profile.spool and any(ad.get("JobStatus") == 4 for ad in left):
                schedd.retrieve(f"{constraint} && JobStatus == 4")
            # a spooled job stays in the queue after it completes until it is removed
            schedd.act(htc.JobAction.Remove, constraint, reason="graphed: run closed")

    def _choose(self, htc: Any) -> tuple[str, Any]:
        """The site's schedd: lowest :func:`schedd_weight` among the query's ads, asking each collector
        in the param's list in turn, else the user's own schedd; ``schedd_locate`` overrides both."""
        if self.schedd_locate is not None:
            pool, name = self.schedd_locate
            return name, htc.Schedd(htc.Collector(pool).locate(htc.DaemonType.Schedd, name))
        if self.profile.schedd_query is None:
            # the name a later session locates this schedd by: its own ad's, not this host's
            name = htc.param.get("SCHEDD_HOST") or htc.Collector().locate(htc.DaemonType.Schedd)["Name"]
            return str(name), htc.Schedd()
        param, constraint = self.profile.schedd_query
        errors = []
        for node in collectors(htc, param):
            try:
                collector = htc.Collector(node)
                ads = collector.query(htc.AdType.Schedd, constraint, list(WEIGHT_ATTRS))
            except Exception as exc:  # the next collector in the list may answer
                errors.append(f"{node}: {exc}")
                continue
            if ads:
                name = choose_schedd(ads)
                # Schedd(<collector ad>) needs CondorVersion in the ad; locate returns a complete one
                return name, htc.Schedd(collector.locate(htc.DaemonType.Schedd, name))
            errors.append(f"{node}: no schedd matches {constraint}")
        raise RuntimeError(f"no schedd found through {param}: {'; '.join(errors)}")

    def alive(self) -> int:
        ads = self._schedd.query(constraint=self._constraint, projection=["JobStatus", "HoldReasonCode"])
        return sum(counts_as_alive(ad) for ad in ads)

    def _drain(self) -> None:
        deadline = time.monotonic() + CLOSE_WAIT_S
        while self.alive() and time.monotonic() < deadline:
            time.sleep(1.0)

    def stop(self) -> None:
        """Wait for the pilots to exit, fetch the spooled logs into ``log_dir``, and remove the jobs."""
        release_quietly(
            f"the wait for the pilots of {self.cluster} to exit", self._drain
        )  # removal still runs
        self._stack.close()
        self._secret.unlink(missing_ok=True)


def collectors(htc: Any, param: str) -> list[str]:
    """The collectors listed in the config ``param``, in order."""
    return re.findall(r"[\w/:\-.]+", str(htc.param[param]))


def _check_shippable(env: Path) -> None:
    """Refuse an ``env`` pilots cannot run from an unpacked copy: not a venv, or with editable installs."""
    recipe = "build it with `python -m venv --system-site-packages` and `pip install <path>` (not -e)"
    if not (env / "pyvenv.cfg").is_file():
        raise ValueError(f"env={env} is not a venv (no pyvenv.cfg); {recipe}")
    purelib = Path(
        sysconfig.get_path("purelib", scheme="venv", vars={"base": str(env), "platbase": str(env)})
    )
    editable = sorted(
        p.parent.name.removesuffix(".dist-info")
        for p in purelib.glob("*.dist-info/direct_url.json")
        if json.loads(p.read_text()).get("dir_info", {}).get("editable")
    )
    if editable:
        raise ValueError(
            f"env={env} has editable installs, which point outside the shipped copy: "
            f"{', '.join(editable)}; {recipe}"
        )
