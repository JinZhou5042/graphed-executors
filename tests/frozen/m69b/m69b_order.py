"""Harness for ``test_service_order.py``: when a run's service jobs and its pilots are submitted
(plan-services.md §5.2 "Ordering").

- ``OrderSchedd``: m68a's recorded schedd (``services_harness.RecordingSchedd``, whose ``log`` it keeps)
  that also stamps every submit, ``act`` and announce in ``events``, numbers each submitted cluster, and
  answers ``query`` per cluster: a pilot not yet started is idle, a started one running (with its claim),
  an exited or removed one gone; a service job answers ``service_status``. On a pilots' submit it can
  start real pilot processes in ``initialdir`` with the job's ``arguments``; on a service submit it posts
  the job's signed announce ``announce_after_s`` later, naming ``service_port``.
- ``order_bindings``: m68a's ``record_bindings``, with ``Hold`` and ``Release`` added to the fake
  module's ``JobAction`` (m68a's has only ``Remove``).
- ``order_plan``/``twin``: a ``SpyProcess`` plan over cluster-only services, and its value on a
  ``ThreadBackend`` given the same endpoint.
- The pool helpers (``require_pool``, ``largest_slot_mb``, ``queued``, ``unrun``) as
  ``test_histserv_cluster.py`` has them.

Pilots unpickle ``SpyProcess`` from ``services_harness``, so a pilot process runs with m68a's
directory on its ``PYTHONPATH``.
"""

from __future__ import annotations

import contextlib
import hashlib
import hmac
import json
import os
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from services_harness import (
    HARNESS_DIR,
    RecordingSchedd,
    SpyProcess,
    SubmitResult,
    hosted_spec,
    record_bindings,
    run_bounded,
    spy_plan,
    submit_api,
)

if TYPE_CHECKING:
    import pytest
    from services_harness import FakeHTCondor

ANNOUNCE_AFTER_S = 2.0
IDENTITY = "wn9.m69b.example"
PILOT_MODULE = "graphed_executors.htcondor_backend.pilot"
PILOTS_BATCH = "graphed-pilots-"
SERVICE_BATCH = "graphed-service-"
SIG_HEADER = "X-Graphed-Sig"
RUN_S = 120.0
POOL_PROBE_S = 30.0


class OrderJobAction:
    Remove = "Remove"
    Hold = "Hold"
    Release = "Release"


@dataclass(frozen=True)
class Event:
    """One stamped schedd event: ``kind`` is ``submit-pilots``, ``submit-service``, ``pilot-start``,
    ``act``, ``announce`` or ``mark``; ``text`` is the service key, the action and constraint, or the
    mark's label."""

    t: float
    kind: str
    cluster: int | None = None
    text: str = ""
    reason: str | None = None


def cluster_of(constraint: object) -> int | None:
    found = re.search(r"ClusterId\s*==\s*(\d+)", str(constraint))
    return int(found.group(1)) if found else None


class OrderSchedd(RecordingSchedd):
    def __init__(
        self,
        service_port: int,
        *,
        pilots: bool = False,
        pilot_after_s: float = 0.0,
        announce_after_s: float | None = ANNOUNCE_AFTER_S,
        service_status: int = 2,
        spool_raises: BaseException | None = None,
    ) -> None:
        super().__init__(spool_raises=spool_raises)
        self.events: list[Event] = []
        self.service_port = service_port
        self.start_pilots = pilots
        self.pilot_after_s = pilot_after_s
        self.announce_after_s = announce_after_s
        self.service_status = service_status
        self._lock = threading.Lock()
        self._next_cluster = 7000
        self._pilots: dict[int, list[subprocess.Popen[bytes] | None]] = {}
        self._finished: set[int] = set()
        self._services: dict[int, Path] = {}
        self._gone: set[int] = set()

    def _stamp(
        self, kind: str, cluster: int | None = None, text: str = "", reason: str | None = None
    ) -> None:
        with self._lock:
            self.events.append(Event(time.monotonic(), kind, cluster, text, reason))

    def mark(self, label: str) -> None:
        self._stamp("mark", text=label)

    def submit(self, description: Any, count: int = 0, spool: bool = False, **kwargs: Any) -> SubmitResult:
        super().submit(description, count, spool, **kwargs)
        desc = dict(description)
        with self._lock:
            self._next_cluster += 1
            cluster = self._next_cluster
        batch = str(desc.get("JobBatchName", ""))
        job_dir = Path(desc["initialdir"])
        if batch.startswith(PILOTS_BATCH):
            with self._lock:
                self._pilots[cluster] = [None] * count
            self._stamp("submit-pilots", cluster)
            if self.start_pilots:
                threading.Thread(target=self._start, args=(cluster, desc, count), daemon=True).start()
        else:
            with self._lock:
                self._services[cluster] = job_dir
            self._stamp("submit-service", cluster, batch.removeprefix(SERVICE_BATCH))
            if self.announce_after_s is not None:
                threading.Thread(target=self._announce_later, args=(cluster, job_dir), daemon=True).start()
        return SubmitResult(cluster)

    def _start(self, cluster: int, desc: dict[str, Any], count: int) -> None:
        time.sleep(self.pilot_after_s)
        job_dir = Path(desc["initialdir"])
        path = os.pathsep.join([HARNESS_DIR, os.environ.get("PYTHONPATH", "")]).rstrip(os.pathsep)
        env = {**os.environ, "PYTHONPATH": path}
        argv = [sys.executable, "-m", PILOT_MODULE, *str(desc["arguments"]).split()]
        for i in range(count):
            with self._lock:
                if cluster in self._gone:
                    return
                with (
                    open(job_dir / f"pilot.{i}.out", "wb") as out,
                    open(job_dir / f"pilot.{i}.err", "wb") as err,
                ):
                    proc = subprocess.Popen(argv, cwd=job_dir, env=env, stdout=out, stderr=err)
                self._pilots[cluster][i] = proc
            self._stamp("pilot-start", cluster)

    def _announce_later(self, cluster: int, job_dir: Path) -> None:
        time.sleep(self.announce_after_s or 0.0)
        if cluster not in self._gone:
            with contextlib.suppress(OSError):
                self.announce(job_dir)

    def announce(self, job_dir: Path) -> int:
        """Post the signed announce of the service job in ``job_dir``: its key at ``service_port``."""
        cfg = json.loads((job_dir / "service.json").read_text())
        secret = bytes.fromhex((job_dir / "graphed-secret").read_text().strip())
        body = f"{cfg['key']} 127.0.0.1:{self.service_port} {IDENTITY}".encode()
        self._stamp("announce", text=str(cfg["key"]))
        request = urllib.request.Request(str(cfg["url"]).rstrip("/") + "/announce", data=body, method="POST")
        request.add_header(SIG_HEADER, hmac.new(secret, body, hashlib.sha256).hexdigest())
        try:
            with urllib.request.urlopen(request, timeout=30.0) as response:
                return int(response.status)
        except urllib.error.HTTPError as exc:
            return int(exc.code)

    def service_dirs(self) -> list[Path]:
        with self._lock:
            return list(self._services.values())

    def _pilot_ads(self, cluster: int) -> list[dict[str, Any]]:
        ads: list[dict[str, Any]] = []
        if cluster in self._finished:
            return ads
        for i, proc in enumerate(self._pilots[cluster]):
            ad: dict[str, Any] = {"ClusterId": cluster, "ProcId": i}
            if proc is None:
                ads.append({**ad, "JobStatus": 1})
            elif proc.poll() is None:
                claim = {
                    "RemoteHost": f"slot1_{i + 1}@{IDENTITY}",
                    "MemoryProvisioned": 128,
                    "CpusProvisioned": 1,
                }
                ads.append({**ad, "JobStatus": 2, **claim})
        return ads

    def query(self, constraint: str = "true", projection: Any = None, *args: Any, **kwargs: Any) -> list[Any]:
        self.log.append(("query", str(constraint), tuple(projection or ())))
        cluster = cluster_of(constraint)
        wanted = re.search(r"JobStatus\s*==\s*(\d+)", str(constraint))
        with self._lock:
            if cluster is None or cluster in self._gone:
                return []
            if cluster in self._pilots:
                ads = self._pilot_ads(cluster)
            elif cluster in self._services:
                ads = [{"ClusterId": cluster, "ProcId": 0, "JobStatus": self.service_status}]
            else:
                return []
        return [ad for ad in ads if wanted is None or ad["JobStatus"] == int(wanted.group(1))]

    def act(self, action: Any, constraint: Any = None, *args: Any, **kwargs: Any) -> None:
        super().act(action, constraint, *args, **kwargs)
        reason = kwargs.get("reason", args[0] if args else None)
        cluster = cluster_of(constraint)
        self._stamp("act", cluster, f"{action} {constraint}", None if reason is None else str(reason))
        if str(action) == OrderJobAction.Remove and cluster is not None:
            with self._lock:
                self._gone.add(cluster)
                procs = [p for p in self._pilots.get(cluster, []) if p is not None]
            for proc in procs:
                proc.terminate()

    def finish_pilots(self) -> None:
        """Every pilot not started leaves the queue, as one that exited on its own does."""
        with self._lock:
            self._finished.update(c for c, procs in self._pilots.items() if not any(procs))

    def stop_pilots(self) -> None:
        """Teardown backstop for pilot processes the run left behind."""
        with self._lock:
            procs = [p for ps in self._pilots.values() for p in ps if p is not None]
        for proc in procs:
            if proc.poll() is None:
                proc.kill()
            proc.wait(30.0)


@contextlib.contextmanager
def closing(runner: Any, timeout_s: float) -> Iterator[Any]:
    """``runner`` closed within ``timeout_s`` when the block ends; an exception leaving the block ends any
    wait for a slot first, as leaving the runner's own ``with`` block does."""
    try:
        yield runner
    except BaseException:
        runner.backend.stop_waiting()
        raise
    finally:
        run_bounded(runner.close, timeout_s)


def order_bindings(monkeypatch: pytest.MonkeyPatch, schedd: OrderSchedd) -> FakeHTCondor:
    fake = record_bindings(monkeypatch, schedd)
    monkeypatch.setattr(fake, "JobAction", OrderJobAction)
    return fake


def of_kind(events: list[Event], kind: str) -> list[Event]:
    return [e for e in events if e.kind == kind]


def between(events: list[Event], start: str, stop: str | None = None) -> list[Event]:
    """The events after the mark ``start`` and before the mark ``stop`` (else to the end)."""
    labels = [e.text if e.kind == "mark" else None for e in events]
    first = labels.index(start) + 1
    last = labels.index(stop) if stop is not None else len(events)
    return events[first:last]


def acts(events: list[Event], action: str) -> list[Event]:
    return [e for e in events if e.kind == "act" and e.text.split(" ", 1)[0] == action]


def order_plan(tag: str, *services: str, timeout_s: float = 30.0) -> Any:
    """Two tasks of ``SpyProcess(tag)`` (bound to ``web``) over ``services``, each with an image, so no
    backend runs one beside the driver."""
    return spy_plan(SpyProcess(tag), 2, [hosted_spec(name, timeout_s=timeout_s) for name in services])


def twin(plan: Any, endpoint: str) -> Any:
    """``plan``'s value on a one-thread ``SubmitRunner`` given ``endpoint`` for each of its services."""
    api = submit_api()
    runner = api.SubmitRunner(api.ThreadBackend(1), services={s.name: endpoint for s in plan.services})
    try:
        return run_bounded(lambda: runner.run(plan), RUN_S).value
    finally:
        runner.close()


# ---- the pool (as test_histserv_cluster.py) ------------------------------------------------------------


def require_pool() -> Any:
    """The bindings and a schedd of a reachable pool, or a skip (no bindings) / failure (no pool)."""
    import pytest  # noqa: PLC0415  (the pilots import this module without pytest)
    from m69b_harness import require_histserv  # noqa: PLC0415  (pulls awkward; the pool rows only)

    htcondor2: Any = pytest.importorskip(
        "htcondor2",
        reason="the htcondor bindings are not installed (Linux wheels only; the test-htcondor job)",
    )
    try:
        ads = run_bounded(
            lambda: htcondor2.Collector().query(htcondor2.AdType.Schedd, projection=["Name"]), POOL_PROBE_S
        )
    except Exception as exc:
        pytest.fail(f"htcondor2 is installed but no pool answers ({exc}): start a personal HTCondor")
    assert ads, "htcondor2 is installed but the collector lists no schedd: start a personal HTCondor"
    require_histserv()
    return htcondor2


def largest_slot_mb(htc: Any) -> int:
    """The largest slot memory the collector advertises: partitionable ``TotalSlotMemory``, static
    ``Memory``, dynamic slots skipped."""
    ads = htc.Collector().query(htc.AdType.Startd, projection=["SlotType", "TotalSlotMemory", "Memory"])
    sizes = [
        int(ad["TotalSlotMemory"] if ad.get("SlotType") == "Partitionable" else ad["Memory"])
        for ad in ads
        if ad.get("SlotType") != "Dynamic"
    ]
    assert sizes, ads
    return max(sizes)


def queued(schedd: Any, constraint: str) -> list[Any]:
    return list(schedd.query(constraint=constraint, projection=["ClusterId", "JobStatus", "JobBatchName"]))


def unrun(schedd: Any, constraint: str) -> list[tuple[Any, bool]]:
    """``(NumJobStarts, JobCurrentStartDate present)`` of each history ad matching ``constraint``."""
    ads = schedd.history(constraint, ["ClusterId", "NumJobStarts", "JobCurrentStartDate"], match=10)
    return [(ad.get("NumJobStarts"), "JobCurrentStartDate" in ad) for ad in ads]
