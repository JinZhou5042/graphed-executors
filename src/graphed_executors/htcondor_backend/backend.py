"""``HTCondorBackend``: a :class:`SubmitBackend` at the all-False floor whose workers are pilots it
launches itself. Pilots pull pickled tasks from the backend's :class:`TaskServer` and post results
back; the driver resolves future arguments before a task is queued, so pilots never talk to each other.

The backend's service surface is duck-typed, read by the engine's service set (``submit/services.py``):
``site_services``, ``service_hosts`` (which the caller may narrow) and ``service_ports`` come from the
site row, ``advertise_host`` is the task server's host, ``host_identity()`` names the driver's host as
condor writes it for that host's slots, and ``driver_memory_mb`` bounds the services beside the driver (a
driver job's slot ``Memory``; ``None``, the host's physical memory). Attached, the row is the launcher's
profile; in a driver job (``in_job=``) it is the job's own site row. Attached over ``CondorPilots`` on a
row with ``"cluster"`` hosts, ``host_service``/``release_service`` run a service as its own job
(:class:`~graphed_executors.htcondor_backend.services.ServiceJob`) that announces its endpoint to the task
server's ``/announce``: a job no slot of the pool could ever run is removed and refused, and a job waiting
for a slot is waited for with no deadline (an exception leaving the runner's ``with`` block ends the wait),
its ``timeout_s`` counted from its start. A run's service jobs go before its pilots: the first plan's
servers announce before the pilots are submitted, and a later plan's wait with the runner's queued pilots
held and are matched beside its running ones. In a driver job
a managed service starts beside the driver, or, when it is one of the run's DAG SERVICE nodes
(``announced=``), is resolved by that node's announce: the driver job submits no service job, and the
wait reads the node's queue ad on the DAG's schedd (``schedd_locate=``), so a node that ended, is held,
or that no slot could ever run fails it at once.
"""

from __future__ import annotations

import io
import logging
import pickle
import secrets
import socket
import threading
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from concurrent.futures import Future
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import Any, Protocol, TypeVar, overload

from graphed.core.execution import ExecResult, Plan
from graphed.core.plan import DurablePlanV2
from graphed.services import ServiceSpec

from graphed_executors.parsl_backend.backend import _ParslFuture
from graphed_executors.submit import SubmitCapabilities, SubmitRunner
from graphed_executors.submit.protocol import SubmitFuture
from graphed_executors.submit.services import (
    ServiceUnavailable,
    host_identity,
    machine_ad,
    minted_endpoint,
    release_quietly,
)

from . import launch as _launch
from . import server as _server
from .launch import CLAIM_ATTRS, CondorPilots, PilotLauncher, located_schedd
from .server import TaskServer, WorkerLost
from .services import AD_ATTRS, ServiceJob, machine_ads, queued_refusal
from .sites import SITES, SiteProfile, counts_as_alive

R = TypeVar("R")

# Pilots queue like any other job, so a busy pool can take minutes to start the first one.
N_WORKERS_WAIT_S = 600.0
IDLE_LOG_S = 30.0  # between log lines of a service job still waiting for a slot
_IDLE, _RUNNING = 1, 2  # JobStatus
# a SERVICE node's periodic_remove takes a held node out of the queue, so only its history says why
_GONE = ("LastHoldReason", "RemoveReason")

logger = logging.getLogger(__name__)

_FLOOR = SubmitCapabilities(
    peer_data_movement=False,
    scatter_broadcast=False,
    pin_to_worker=False,
    per_task_retries=False,
    per_task_resources=False,
    cancel_running=False,
    worker_file_cache=False,
)


class _Announcer(Protocol):
    """What :meth:`HTCondorBackend._await_announce` reads of a job that announces: its announce ``key``, its
    queue ``ad()`` (once it is gone, its history row, else ``{}``) and the ``dir`` its ``service.out``/``.err``
    land in."""

    key: str
    dir: Path | None

    def ad(self) -> Any: ...


class _Unread(Exception):
    """A DAG SERVICE node's queue ad could not be read: the bindings, the locate or a query failed."""


class _Node:
    """DAG SERVICE node ``key`` as :meth:`HTCondorBackend._await_announce` reads a service job: :meth:`ad` is
    its queue ad within DAGMan job ``dag`` on ``schedd``, else, once it left the queue, its history row naming
    why. Its first idle ad is matched once against the pool's slots less what the DAG's running jobs and
    ``claims()`` hold, and no slot raises :class:`ServiceUnavailable` for service ``name``; DAGMan owns the
    node, so nothing acts on it."""

    def __init__(
        self,
        name: str,
        key: str,
        dir: Path | None,
        schedd: Any,
        dag: int,
        locate: tuple[str, str],
        claims: Callable[[], dict[str, list[Any]]],
    ) -> None:
        self.key = key
        self.dir = dir
        self._name = name
        self._schedd = schedd
        self._dag = dag
        self._locate = locate
        self._claims = claims
        self._matched = False

    def ad(self) -> Any:
        node = f'DAGManJobId == {self._dag} && DAGNodeName == "{self.key}"'
        refusal = None
        try:
            ads = list(self._schedd.query(constraint=node, projection=AD_ATTRS))
            ad = ads[0] if ads else {}
            if ad.get("JobStatus") == _IDLE and not self._matched:
                self._matched = True
                refusal = self._refusal(node)
        except Exception as exc:
            raise _Unread(exc) from exc
        if refusal is not None:
            raise ServiceUnavailable(self._name, {"managed": refusal})
        return ad if ads else self._departed(node)

    def _departed(self, node: str) -> dict[str, Any]:
        """The departed node's ``JobStatus`` and ``ExitCode`` with its ``LastHoldReason``, else its
        ``RemoveReason``, from its history row; ``{}``, logged, when that row is missing or unreadable."""
        try:
            # match=1: a larger match scans a big schedd's whole history
            rows = list(self._schedd.history(node, projection=["JobStatus", "ExitCode", *_GONE], match=1))
        except Exception as exc:
            logger.warning("SERVICE node %s left the queue; its history is unreadable (%r)", self.key, exc)
            return {}
        if not rows:
            logger.warning("SERVICE node %s left the queue with no history row", self.key)
            return {}
        row = rows[0]
        why = "LastHoldReason" if row.get("LastHoldReason") else "RemoveReason"
        return {name: row[name] for name in ("JobStatus", "ExitCode", why) if name in row}

    def _refusal(self, node: str) -> str | None:
        machines = machine_ads(self._locate)
        whole = list(self._schedd.query(constraint=node)) if machines else []
        # no slot listed says nothing of the pool; a node gone meanwhile is the wait's to report
        if not whole:
            return None
        dag = f"the DAG's running jobs (DAGMan job {self._dag})"
        running = f"DAGManJobId == {self._dag} && JobStatus == {_RUNNING}"
        claims = {dag: list(self._schedd.query(constraint=running, projection=CLAIM_ATTRS)), **self._claims()}
        return queued_refusal(f"SERVICE node {self.key}", whole[0], machines, claims)


class HTCondorBackend:
    """Starts a task server and ``n_pilots`` pilots through ``launcher``; :meth:`close` removes them.
    Pilot jobs whose services are jobs too (``CondorPilots`` with ``host_service``) are submitted at
    the first need of a worker, put off while a plan's services start (:meth:`starting_services`) and
    never made once the run ends, and that need waits once for ``min_pilots``, counted from the submit.

    ``host`` is the name pilots dial back to (default: this machine's FQDN); the server binds the
    first free port of ``port_range`` on all interfaces, by default the ``driver_ports`` of the
    launcher's site profile (``generic`` for a launcher without one). ``in_job`` is the site row of the
    driver job this backend runs in (``driver.py`` passes it), else ``None`` (attached). ``announced`` maps
    each service a SERVICE node of the driver job's DAG hosts to the node id it announces under; the DAG's
    directory is ``dag_dir``, and ``schedd_locate=(pool, name)`` names its schedd, where the wait for a
    node's announce reads the node's queue ad.
    ``service_hosts`` narrows where a managed service may run to a subset of the hosts the row offers
    (``None``: all of them); a host it does not offer is refused before the task server starts.
    """

    def __init__(
        self,
        launcher: PilotLauncher,
        n_pilots: int,
        *,
        host: str | None = None,
        port_range: tuple[int, int] | None = None,
        in_job: SiteProfile | None = None,
        announced: Mapping[str, str] | None = None,
        service_hosts: Sequence[str] | None = None,
        schedd_locate: tuple[str, str] | None = None,
        dag_dir: str | Path | None = None,
    ) -> None:
        self.capabilities = _FLOOR
        self.launcher = launcher
        self._handlers: dict[str, Callable[[list[dict[str, object]]], None]] = {}
        profile: SiteProfile = getattr(launcher, "profile", SITES["generic"])
        # a driver job's services stay beside it, reached like its task server
        offered = profile.service_hosts if in_job is None else ("driver",)
        if service_hosts is not None and not set(service_hosts) <= set(offered):
            where = "a driver job" if in_job is not None else f"site {profile.name!r}"
            raise ValueError(
                f"service_hosts={tuple(service_hosts)!r} names a host {where} does not offer: "
                f"it offers {offered!r}"
            )
        self.advertise_host = host or socket.getfqdn()
        self._in_job = in_job
        self.service_hosts = (
            offered if service_hosts is None else tuple(h for h in offered if h in service_hosts)
        )
        self.service_ports: tuple[int, int] | None
        # the engine holds the services beside the driver to it; None reads the host's physical memory
        self.driver_memory_mb: int | None = None
        if in_job is None:
            self.site_services = profile.services
            self.service_ports = profile.service_ports
        else:  # self-submitted pilots reach the driver on worker_ports
            self.site_services = in_job.services
            self.service_ports = in_job.worker_ports if isinstance(launcher, CondorPilots) else None
            slot = machine_ad("Memory")  # the job's slot, not the node it shares
            self.driver_memory_mb = None if slot is None else int(slot)
        low, high = port_range if port_range is not None else profile.driver_ports
        try:
            self._server = TaskServer(self.advertise_host, (low, high), launcher)
        except OSError as exc:
            raise OSError(
                f"no free port for the task server: site={profile.name} ports={low}-{high}"
            ) from exc
        self._services: dict[str, ServiceJob] = {}  # every service job submitted and not yet released
        self._lock = threading.Lock()  # a service job is recorded before close() reads the record, or refused
        self._closing = threading.Event()  # set: a service job still waiting for a slot ends its wait
        self._announced = dict(announced or {})
        self._schedd_locate = schedd_locate
        self._dag_dir = None if dag_dir is None else Path(dag_dir)
        # the capability IS this pair of attributes: absent, the engine refuses naming it
        if self._announced:
            self.host_service = self._host_announced
            self.release_service = self._release_announced
        elif in_job is None and isinstance(launcher, CondorPilots) and "cluster" in self.service_hosts:
            self.host_service = self._host_service
            self.release_service = self._release_service
        # pilot jobs submitted with the services' jobs would hold the room those need: they wait
        deferred = isinstance(launcher, CondorPilots) and hasattr(self, "host_service")
        self.min_pilots = 1  # the first need of a worker waits for this many
        self._n_pilots = n_pilots
        self._pilots_lock = threading.Lock()
        self._submitted_at = None if deferred else time.monotonic()
        self._held = False  # the queued pilots are held while a later plan's services start
        self._waited = False
        self._starting = 0  # plans whose services are starting (ServiceSet resolve phases)
        self._set_start = threading.Lock()  # held by the one service set starting, through its probe
        self._wanted = False  # a need of a worker arrived during one
        self._closed = False
        with ExitStack() as stack:  # a refused start must not leave the server holding its port
            stack.callback(release_quietly, "the task server's port", self._server.shutdown)
            stack.callback(release_quietly, "the task server", self._server.close)
            if deferred and isinstance(launcher, CondorPilots):
                launcher.prepare(self._server.url, self._server.secret)
            else:
                launcher.start(self._server.url, self._server.secret, n_pilots)
            stack.callback(release_quietly, "the pilots", launcher.stop)
            # closed first: pilots see 410 and exit before they are stopped (a second close is a no-op)
            stack.callback(release_quietly, "the task server", self._server.close)
            self._stack = stack.pop_all()

    def _need(self) -> None:
        """A need of a worker (:meth:`n_workers`, :meth:`submit` or :meth:`wait_for_pilots`)."""
        with self._pilots_lock:
            if self._starting:
                self._wanted = True
            else:
                self._move_pilots()

    def _move_pilots(self) -> None:
        """Release the pilots a plan's services held, or submit deferred ones; under ``_pilots_lock``."""
        # pilots move only once no plan is starting its services, and none is submitted once the run ends
        # (a release still runs: the plans the drain finishes may need them)
        if self._held:
            assert isinstance(self.launcher, CondorPilots)
            self.launcher.release_held()
            self._held = False
        if self._submitted_at is None and not self._closing.is_set():
            self.launcher.start(self._server.url, self._server.secret, self._n_pilots)
            self._submitted_at = time.monotonic()

    @contextmanager
    def starting_service_set(self) -> Iterator[None]:
        """A plan's service set starting, through its probe (:meth:`ServiceSet.start`): one at a time, so no
        set waits for room or a pilot that another starting set holds while that set waits for its own. A set
        waiting its turn raises once the runner closes (:meth:`stop_waiting`, :meth:`close`), within one
        ``POLL_S``, whatever the starting set is waiting for."""
        while not self._set_start.acquire(timeout=_server.POLL_S):
            if self._closing.is_set():
                raise RuntimeError(
                    "a plan's services still waited for another plan's to start when the runner closed"
                )
        try:
            yield
        finally:
            self._set_start.release()

    @contextmanager
    def starting_services(self) -> Iterator[None]:
        """A plan's services starting (:class:`ServiceSet`'s resolve phase): a need of a worker meanwhile
        is recorded, and the last phase to end moves the pilots a need asked for or a server held, its
        services started or failed (a failure is logged; the next need retries it)."""
        with self._pilots_lock:
            self._starting += 1
        try:
            yield
        finally:
            with self._pilots_lock:
                self._starting -= 1
                if not self._starting and (self._wanted or self._held):
                    self._wanted = False
                    release_quietly("the pilots", self._move_pilots)

    def _need_worker(self) -> None:
        if self._waited:
            self._need()
        else:
            self.wait_for_pilots(self.min_pilots)

    def n_workers(self) -> int:
        """Pilots registered and live right now. The first call, or the first :meth:`submit`, waits
        once for ``min_pilots``, submitting deferred pilots first."""
        self._need_worker()
        return self._server.live_pilots()

    def wait_for_pilots(self, n: int, timeout: float = N_WORKERS_WAIT_S) -> int:
        """Wait for ``n`` live pilots, a need of a worker at each poll; ``timeout`` counts from the call,
        or from the pilots' submit when a plan's service start defers it. A wait for ``min_pilots`` or
        more is the first need's wait; once the run ends, a wait no pilot can end raises."""
        called = time.monotonic()
        while True:
            self._need()
            if (live := self._server.live_pilots()) >= n:
                break
            submitted = self._submitted_at
            if self._closing.is_set() and (submitted is None or self._closed):
                raise RuntimeError(
                    f"{live} of {n} pilots connected when the runner closed"
                    + ("; none was submitted" if submitted is None else "")
                )
            if submitted is not None and time.monotonic() > max(called, submitted) + timeout:
                raise RuntimeError(
                    f"{live} of {n} pilots connected after {timeout}s; "
                    f"see the pilot logs in {getattr(self.launcher, 'log_dir', None)}"
                )
            time.sleep(0.05)
        self._waited = self._waited or n >= self.min_pilots
        return live

    def submit(
        self,
        fn: Callable[..., object],
        /,
        *args: object,
        key: str,
        retries: int = 0,
        priority: int = 0,
        resources: Mapping[str, float] | None = None,
        workers: Sequence[str] | None = None,
    ) -> SubmitFuture:
        """Never blocks on a future argument: the task queues once its arguments are done. The hints
        are ignored. The first call, or the first :meth:`n_workers`, waits once for ``min_pilots``."""
        self._need_worker()
        raw: Future[Any] = Future()
        self._server.add(key, fn, args, raw)
        return _ParslFuture(raw, self._handlers)

    def broadcast(self, payload: bytes, *, token: str) -> object:
        return payload  # each task carries its own copy through the driver

    def subscribe_events(
        self, topic: str, handler: Callable[[list[dict[str, object]]], None]
    ) -> Callable[[], None]:
        self._handlers[topic] = handler

        def unsub() -> None:
            self._handlers.pop(topic, None)

        return unsub

    def cancel(self, futures: Sequence[Any]) -> None:
        for fut in futures:  # pre-run only: a leased task runs to completion
            fut.cancel()

    def describe_failure(self, exc: BaseException) -> tuple[str, str] | None:
        return (exc.key, exc.pilot) if isinstance(exc, WorkerLost) else None

    def host_identity(self) -> str:
        """The driver's host as condor writes ``Machine`` for its slots: ``FULL_HOSTNAME`` attached (the
        only bindings use here), the job's own machine ad in a driver job."""
        if self._in_job is not None:
            return host_identity()
        return str(_launch._htcondor().param["FULL_HOSTNAME"])

    def _host_service(self, spec: ServiceSpec, scope: str) -> tuple[str, str, str]:
        """Run ``spec`` as a :class:`ServiceJob` and wait for its announce: ``(endpoint, identity, key)``
        once it is ready where it runs. A job no slot of the pool could ever run raises
        :class:`ServiceUnavailable` (a pool whose collector lists no slot is not asked); one that ends,
        is held, or has not announced within ``spec.timeout_s`` of its start raises. Each is removed,
        and every failure forgets the call's announce secret. Once the pilots are submitted (a later
        plan), the first call holds their queued jobs until the plan's services have started or failed.
        The match counts each slot less what the running pilots (kept until the runner closes) and the
        set's earlier servers (keys of ``scope``, kept until the run ends) hold."""
        assert isinstance(self.launcher, CondorPilots)
        machines = machine_ads(self.launcher.schedd_locate)
        claims: dict[str, list[Any]] = {}
        with self._pilots_lock:
            self._hold_queued()
            if machines and self._submitted_at is not None:
                claims.update(self._pilot_claims())
        if machines:
            with self._lock:
                siblings = [job for key, job in self._services.items() if key.startswith(f"{scope}-")]
            for sibling in siblings:
                if sibling.cluster is not None:
                    server = f"its server {sibling.spec.name!r} (cluster {sibling.cluster}, whose slot stays taken until the run ends)"
                    claims[server] = self.launcher.running_claims(f"ClusterId == {sibling.cluster}")
        key = f"{scope}-{secrets.token_hex(8)}"
        secret = self._server.announce_secret([key])
        with ExitStack() as stack:
            stack.callback(self._server.forget_announce, [key])
            job = ServiceJob(spec, self.launcher, key=key, url=self._server.url, secret=secret)
            with self._lock:
                if self._closing.is_set():
                    raise RuntimeError(f"service {spec.name!r} ({key}) not submitted: the runner is closing")
                self._services[key] = job
            stack.callback(self._services.pop, key, None)
            stack.callback(release_quietly, f"service job {key}", job.stop)
            job.submit()
            refusal = job.match_refusal(machines, claims)
            if refusal is not None:
                raise ServiceUnavailable(spec.name, {"managed": refusal})
            hostport, identity = self._await_announce(job, spec)
            stack.pop_all()
        host, _, port = hostport.rpartition(":")
        return minted_endpoint(spec.check, host, int(port)), identity, key

    def _hold_queued(self) -> None:
        """Hold the runner's queued pilot jobs, once submitted, so none takes the room a service waits for,
        until the plan's services have started or failed (:meth:`starting_services` releases them); under
        ``_pilots_lock``."""
        if isinstance(self.launcher, CondorPilots) and self._submitted_at is not None and not self._held:
            self.launcher.hold_queued()
            self._held = True

    def _pilot_claims(self) -> dict[str, list[Any]]:
        """The running ads of the runner's pilot jobs, by holder (none before they are submitted)."""
        launcher = self.launcher
        if not isinstance(launcher, CondorPilots) or launcher.cluster is None:
            return {}
        pilots = f"the runner's running pilots (cluster {launcher.cluster[1]}, whose slots stay taken until it closes)"
        return {pilots: launcher.running_claims()}

    def _await_announce(self, job: _Announcer, spec: ServiceSpec) -> tuple[str, str]:
        """The job's announce. No deadline runs while it waits for a slot (idle, or held while its input
        spools), which is logged at the first such answer and every ``IDLE_LOG_S`` and ends when the
        backend closes; ``timeout_s`` counts from the first answer that it runs."""
        deadline: float | None = None
        logged = -IDLE_LOG_S
        while True:
            left = (
                _server.POLL_S
                if deadline is None
                else max(0.0, min(_server.POLL_S, deadline - time.monotonic()))
            )
            got = self._server.wait_announce(job.key, left)
            if got is not None:
                return got
            ad = job.ad()
            state = (
                ", ".join(f"{name}={ad[name]!r}" for name in (*AD_ATTRS, *_GONE) if name in ad) or "no job ad"
            )
            now = time.monotonic()
            if deadline is None and ad.get("JobStatus") == _RUNNING:
                deadline = now + spec.timeout_s
            if deadline is None and self._closing.is_set():  # before the ad check: close() removes the job
                raise RuntimeError(
                    f"service {spec.name!r} ({job.key}) still waited for a slot when the runner closed: {state}"
                )
            if not counts_as_alive(ad):
                raise RuntimeError(
                    f"service {spec.name!r} ({job.key}) ended before it announced: {state}; "
                    f"its service.out/.err, if it wrote them, are in {job.dir}"
                )
            if deadline is None:
                if now - logged >= IDLE_LOG_S:
                    logger.info("service %r (%s) waits for a slot: %s", spec.name, job.key, state)
                    logged = now
            elif now >= deadline:
                raise TimeoutError(
                    f"service {spec.name!r} ({job.key}) did not announce within timeout_s={spec.timeout_s} "
                    f"of its start: {state}"
                )

    def _release_service(self, key: str) -> None:
        """Stop the service ``host_service`` started under ``key``; its announces are refused first."""
        self._server.forget_announce([key])
        self._services.pop(key).stop()

    def _host_announced(self, spec: ServiceSpec, scope: str) -> tuple[str, str, str]:
        """``spec``'s SERVICE node's announce: ``(endpoint, identity, node id)``, with the runner's queued pilot
        jobs held meanwhile. Where the DAG's schedd is at hand (``schedd_locate``, and ``$_CONDOR_JOB_AD``
        naming the DAGMan job), the wait is :meth:`_await_announce`'s over the node's queue ad: a node that
        left the queue or is held raises at once, an idle one no slot could ever run raises
        :class:`ServiceUnavailable`, and ``timeout_s`` counts from its first running answer. Otherwise, and
        from a failing import, locate or query on (logged once), the wait is ``timeout_s`` long."""
        node = self._announced.get(spec.name)
        if node is None:
            raise ValueError(
                f"service {spec.name!r} has no SERVICE node in this run's DAG "
                f"(announce_only={self._announced!r}), and a driver job submits no service job"
            )
        with self._pilots_lock:
            self._hold_queued()
        got = None
        try:
            handle = self._node(spec.name, node)
            if handle is not None:
                got = self._await_announce(handle, spec)
        except _Unread as exc:
            logger.warning(
                "SERVICE node %s (service %r): its queue ad is unreadable (%r); waiting for its announce alone",
                node,
                spec.name,
                exc.__cause__,
            )
        if got is None:
            got = self._server.wait_announce(node, spec.timeout_s)
        if got is None:
            raise TimeoutError(
                f"SERVICE node {node} (service {spec.name!r}) did not announce within "
                f"timeout_s={spec.timeout_s}"
            )
        host, _, port = got[0].rpartition(":")
        return minted_endpoint(spec.check, host, int(port)), got[1], node

    def _node(self, name: str, key: str) -> _Node | None:
        """DAG SERVICE node ``key`` of service ``name`` on the DAG's schedd, or ``None`` when ``schedd_locate``
        or the job ad's ``DAGManJobId`` is missing; :class:`_Unread` when the bindings or the locate fail."""
        dag = machine_ad("DAGManJobId", env="_CONDOR_JOB_AD")
        if self._schedd_locate is None or dag is None:
            return None
        where = None if self._dag_dir is None else self._dag_dir / f"service-{key}"
        try:
            schedd = located_schedd(_launch._htcondor(), self._schedd_locate)
            return _Node(name, key, where, schedd, int(dag), self._schedd_locate, self._pilot_claims)
        except Exception as exc:
            raise _Unread(exc) from exc

    def _release_announced(self, key: str) -> None:
        """Nothing: a pending announce of node ``key`` is its restart's, which the next set resolves, and
        DAGMan removes the node when the DAG ends."""

    def stop_waiting(self) -> None:
        """End every wait for a service job that has no slot yet, within one ``POLL_S``: the job is
        removed and its run raises. A job that has started keeps its ``timeout_s``; no service job is
        submitted after this."""
        self._closing.set()

    def close(self) -> None:
        """Remove every service job this backend submitted, in the calling thread (a wait for one, or
        for pilots, then raises), stop serving (pilots see 410 and exit), stop the pilots, free the
        port; each step runs even when an earlier one fails, and each failure is logged."""
        with self._pilots_lock, self._lock:  # a pilot submit in flight finishes first, so the stop removes it
            self._closing.set()
            self._closed = True
            jobs = list(self._services.items())
        for key, job in jobs:
            release_quietly(f"service job {key}", job.stop)
        self._stack.close()


class _MainProbe(pickle.Unpickler):
    def find_class(self, module: str, name: str) -> Any:
        if module == "__main__":
            raise pickle.UnpicklingError(f"__main__.{name}")
        return super().find_class(module, name)


def _require_importable(obj: object, role: str) -> None:
    """Refuse ``obj`` when a pilot could not unpickle it: a lambda or local function does not pickle,
    and a ``__main__`` global pickles here but names a module the pilot does not have."""
    try:
        _MainProbe(io.BytesIO(pickle.dumps(obj))).load()
    except (pickle.PicklingError, pickle.UnpicklingError, AttributeError, TypeError) as exc:
        raise ValueError(
            f"pilots import plan.{role} by name; move {obj!r} into a module and pass it in "
            f"`user_modules=[...]` ({exc})"
        ) from exc


def _require_plan_importable(plan: Plan[Any] | DurablePlanV2, roles: Sequence[str]) -> None:
    """Refuse ``plan`` when a pilot could not import one of the V1 ``roles`` the caller ships; a
    ``DurablePlanV2``'s stage processes travel by value, so pilots need not import them."""
    if isinstance(plan, DurablePlanV2):
        return
    for role in roles:
        if (part := getattr(plan, role)) is not None:
            _require_importable(part, role)


class HTCondorRunner(SubmitRunner):
    """A :class:`SubmitRunner` that refuses a ``Plan`` pilots cannot import, and whose backend waits for
    ``min_pilots`` at its first need of a worker, so pilots that never start are an error instead of a
    queue that never drains."""

    backend: HTCondorBackend

    def __init__(
        self,
        backend: HTCondorBackend,
        *,
        min_pilots: int = 1,
        monitor: Any = None,
        retries: int = 3,
        max_in_flight: int = 2,
        services: Mapping[str, str] | None = None,
    ) -> None:
        super().__init__(
            backend, monitor=monitor, retries=retries, max_in_flight=max_in_flight, services=services
        )
        self._min_pilots = backend.min_pilots = min_pilots

    def wait_for_pilots(self) -> int:
        """Wait for ``min_pilots``; pilots lost afterwards fail the run's tasks as lost workers."""
        return self.backend.wait_for_pilots(self._min_pilots)

    @overload
    def run(self, plan: Plan[R]) -> ExecResult[R]: ...
    @overload
    def run(self, plan: DurablePlanV2) -> ExecResult[Any]: ...
    def run(self, plan: Plan[R] | DurablePlanV2) -> ExecResult[R] | ExecResult[Any]:
        _require_plan_importable(plan, ("process", "combine"))
        return super().run(plan)

    def __exit__(self, *exc: object) -> None:
        if exc[0] is not None:  # the plans still running are not waited for on a slot
            self.backend.stop_waiting()
        self.close()

    def close(self) -> None:
        """Finish every submitted plan, a service's wait for a slot included, then close the backend,
        which removes every job the runner submitted, also when that wait is interrupted."""
        try:
            self._plans.close()
        finally:
            self.backend.close()


def htcondor_runner(
    *,
    n_pilots: int,
    site: str | SiteProfile = "generic",
    image: str | None = None,
    request_cpus: int = 1,
    request_memory_mb: int = 2048,
    log_dir: str | Path | None = None,
    user_modules: Sequence[str | Path] = (),
    env: str | Path | None = None,
    extra_submit: Mapping[str, str] | None = None,
    host: str | None = None,
    port_range: tuple[int, int] | None = None,
    min_pilots: int = 1,
    monitor: Any = None,
    retries: int = 3,
    max_in_flight: int = 2,
    services: Mapping[str, str] | None = None,
    service_hosts: Sequence[str] | None = None,
) -> HTCondorRunner:
    """``n_pilots`` pilot jobs on the ``site`` pool behind an :class:`HTCondorRunner`;
    ``runner.close()`` removes them. ``port_range`` is the driver-side port range; default from the
    site profile. ``services`` are the runner's given service endpoints. ``service_hosts`` narrows
    where a managed service runs to a subset of the site's, such as ``("cluster",)``; a host the site
    does not offer is refused before any pilot is submitted."""
    pilots = CondorPilots(
        site,
        image=image,
        request_cpus=request_cpus,
        request_memory_mb=request_memory_mb,
        log_dir=log_dir,
        user_modules=user_modules,
        env=env,
        extra_submit=extra_submit,
    )
    backend = HTCondorBackend(pilots, n_pilots, host=host, port_range=port_range, service_hosts=service_hosts)
    return HTCondorRunner(
        backend,
        min_pilots=min_pilots,
        monitor=monitor,
        retries=retries,
        max_in_flight=max_in_flight,
        services=services,
    )
