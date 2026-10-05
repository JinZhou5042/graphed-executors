"""Shared harness for the m69b frozen suite: histserv servers as the executors' services
(plan-services.md, the m69b executors unit).

Everything a pilot or a driver job unpickles is module-level here (``Steps``, ``ResolveSpy``), and the
live files ship this file as a ``user_modules`` entry, so module-level imports stay stdlib, graphed,
awkward, numpy and boost-histogram. The histogram backend under test is reached only through
``histserv_api()``/``gh_api()`` inside test bodies, so the suite collects before
``graphed_histogram.histserv`` exists.

- ``Steps``: a ``PartitionedSource`` over in-memory events, ``n`` blind steps.
- ``histograms``: the two dyadic-weight fills every non-hgg row runs, backed on a context or local.
- ``sized_memory_mb``: the one server size at which each of two slots fits a server alone and the two
  together do not, read off ``ctx.servers()`` predictions of throwaway contexts.
- ``ResolveSpy``: wraps a plan's process; at ``resolve_services`` it records, per receipt endpoint, the
  receipts naming it and that server's ``stats()["histogram_count"]``, then forwards.
- ``user_servers``: ``python -m histserv`` children on free ports, killed at teardown.
- ``host_memory_mb``: the host's physical memory, read the way the engine's driver check reads it.
- ``same_values``: bit-for-bit equality of two ``{slot: bh.Histogram}`` values (edges, storage, flow view).
"""

from __future__ import annotations

import contextlib
import importlib
import os
import secrets
import socket
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import awkward as ak
import boost_histogram as bh
import numpy as np
from graphed import Session
from graphed.awkward import AwkwardBackend, AwkwardForm
from graphed.core import Partition
from graphed.services import split_endpoint

HARNESS_DIR = str(Path(__file__).resolve().parent)
HARNESS_FILE = str(Path(__file__).resolve())
RUN_TIMEOUT_S = 240.0
SERVER_UP_S = 60.0
MIB = 1 << 20
#: a size no slot here approaches: a throwaway context's one server holds whatever it is given
UNBOUNDED_MB = 1 << 20
N_EVENTS = 2000
N_STEPS = 4
DIGITLESS = str.maketrans("0123456789", "ghijklmnop")

# ---- deferred accessors for the code under test --------------------------------------------------


def histserv_api() -> Any:
    """``graphed_histogram.histserv``: Context, Histogram, backed, Receipt, HistservError."""
    return importlib.import_module("graphed_histogram.histserv")


def gh_api() -> Any:
    """``graphed_histogram``: plan, unpack, boost."""
    return importlib.import_module("graphed_histogram")


def require_histserv() -> Any:
    """``histserv`` imported: a failure where the GIL is enabled, a skip only on a free-threaded
    interpreter (grpcio has no cp314t wheel)."""
    gil_enabled = getattr(sys, "_is_gil_enabled", None)
    if gil_enabled is None or gil_enabled():
        return importlib.import_module("histserv")
    pytest_mod = importlib.import_module("pytest")
    return pytest_mod.importorskip("histserv", reason="grpcio has no wheel for a free-threaded interpreter")


def host_memory_mb() -> int:
    """This host's physical memory in MiB: ``os.sysconf`` pages on POSIX, ``GlobalMemoryStatusEx`` on
    Windows."""
    if sys.platform == "win32":
        import ctypes  # noqa: PLC0415  (Windows only)

        class _Status(ctypes.Structure):
            _fields_ = (
                ("dwLength", ctypes.c_ulong),
                ("dwMemoryLoad", ctypes.c_ulong),
                ("ullTotalPhys", ctypes.c_ulonglong),
                ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong),
                ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong),
                ("ullAvailVirtual", ctypes.c_ulonglong),
                ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
            )

        status = _Status()
        status.dwLength = ctypes.sizeof(_Status)
        assert ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status))
        return int(status.ullTotalPhys) // MIB
    return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") // MIB


def unique(prefix: str) -> str:
    """A context name no other row uses (a name holds one packing state for the process). Its suffix
    has no digits, so a number a refusal is asserted to name cannot come from the name."""
    return f"{prefix}-{uuid.uuid4().hex[:8].translate(DIGITLESS)}"


# ---- bounds (copied from tests/frozen/m66/htcondor_harness.py) -----------------------------------


def run_bounded(fn: Callable[[], Any], timeout_s: float = RUN_TIMEOUT_S) -> Any:
    """Run ``fn`` on a daemon thread and fail if it does not finish in ``timeout_s``: a hang is a
    failure, never a wedged CI job. Returns the value or re-raises the call's exception."""
    out: dict[str, Any] = {}

    def _drive() -> None:
        try:
            out["result"] = fn()
        except BaseException as exc:
            out["error"] = exc

    thread = threading.Thread(target=_drive, daemon=True)
    thread.start()
    thread.join(timeout_s)
    assert not thread.is_alive(), f"HARD TIMEOUT: call did not finish within {timeout_s}s"
    if "error" in out:
        raise out["error"]
    return out["result"]


# ---- the source and the fills ----------------------------------------------------------------------


@dataclass(frozen=True)
class Steps:
    """A ``graphed.write.PartitionedSource`` over in-memory events: ``n`` blind steps of ``uri``."""

    data: ak.Array
    uri: str

    def __call__(self) -> ak.Array:
        raise AssertionError("the whole-dataset loader must never run during a plan")

    def partitions(self, steps_per_file: int = 1) -> tuple[Partition, ...]:
        return tuple(Partition.blind(self.uri, "", s, steps_per_file) for s in range(steps_per_file))

    def read_partition(self, partition: Partition, columns: Any, resources: Any) -> ak.Array:
        part = partition.resolve(len(self.data))
        return self.data[part.entry_start : part.entry_stop]


def events_data(seed: int = 6969) -> ak.Array:
    """Two observables in [0, 1) and dyadic weights, so every sum and every variance is exact."""
    rng = np.random.default_rng(seed)
    return ak.Array(
        {
            "x": rng.random(N_EVENTS),
            "y": rng.random(N_EVENTS),
            "w": rng.choice(np.array([0.25, 0.5, 1.0, 2.0]), N_EVENTS),
        }
    )


def events(tag: str) -> Any:
    """A fresh session's ``events`` source over ``events_data()``."""
    data = events_data()
    session = Session(AwkwardBackend())
    form = AwkwardForm(ak.Array(data.layout.to_typetracer(forget_length=True)))
    return session.source("events", form=form, data=Steps(data, f"mem://m69b/{tag}"))


def histograms(
    bins: Mapping[str, int], ctx: Any = None, backed: tuple[str, ...] | None = None
) -> dict[str, Any]:
    """One Weight histogram per ``bins`` entry (``Regular(n, 0, 1)``, so a slot's flow extent is
    ``n + 2``), each filled from its own observable with the dyadic weights; those named in ``backed``
    (default: all) are ``histserv.Histogram``s on ``ctx``, the rest local, and ``ctx=None`` is the
    unbacked twin."""
    ev = events(unique("ev"))
    names = tuple(bins)
    on = names if backed is None else backed
    out: dict[str, Any] = {}
    for i, name in enumerate(names):
        axis = bh.axis.Regular(bins[name], 0.0, 1.0)
        if ctx is not None and name in on:
            h = histserv_api().Histogram(axis, storage=bh.storage.Weight(), context=ctx)
        else:
            h = gh_api().boost.Histogram(axis, storage=bh.storage.Weight())
        h.fill(ev.x if i % 2 == 0 else ev.y, weight=ev.w)
        out[name] = h
    return out


def served_plan(bins: Mapping[str, int], ctx: Any = None, backed: tuple[str, ...] | None = None) -> Any:
    return gh_api().plan(histograms(bins, ctx, backed), steps_per_file=N_STEPS)


def predicted(plan_bins: Mapping[str, int], backed: tuple[str, ...], workers: int) -> int:
    """The predicted bytes of the one server a throwaway context opens for ``backed`` alone."""
    ctx = histserv_api().Context(memory_mb=UNBOUNDED_MB, workers=workers, name=unique("m69b-probe"))
    served_plan(plan_bins, ctx, backed)
    ((_name, _size, bytes_, n),) = ctx.servers()
    assert n == len(backed), ctx.servers()
    return int(bytes_)


def sized_memory_mb(bins: Mapping[str, int], workers: int) -> int:
    """The size (MiB) at which each of the two equal slots ``bins`` names fits a server alone and the
    two together do not."""
    a, b = bins
    one = max(predicted(bins, (a,), workers), predicted(bins, (b,), workers))
    both = predicted(bins, (a, b), workers)
    size = -(-one // MIB)
    assert size * MIB < both, f"fixture: slots too small to split ({one}, {both} bytes)"
    return size


# ---- what the run's servers hold ------------------------------------------------------------------

_SEEN_LOCK = threading.Lock()
_SEEN: dict[str, dict[str, tuple[int, int]]] = {}


def server_count(endpoint: str) -> int:
    """``stats()["histogram_count"]`` of the histserv at ``endpoint`` (``scheme://host:port``)."""
    client = importlib.import_module("histserv").Client(split_endpoint(endpoint)[1])
    with client:
        return int(client.stats()["histogram_count"])


def receipts(value: Mapping[Any, Any]) -> list[Any]:
    receipt = histserv_api().Receipt
    return [v for v in value.values() if isinstance(v, receipt)]


@dataclass(frozen=True)
class ResolveSpy:
    """A plan process that forwards to ``inner`` and, at ``resolve_services``, records
    ``{endpoint: (receipts naming it, its stats() histogram_count)}`` under ``tag``."""

    inner: Any
    tag: str

    def __call__(self, partition: Partition, resources: Any) -> Any:
        return self.inner(partition, resources)

    def bind_services(self, endpoints: Mapping[str, str]) -> ResolveSpy:
        return replace(self, inner=self.inner.bind_services(endpoints))

    def resolve_services(self, value: Any) -> Any:
        named: dict[str, int] = {}
        for r in receipts(value):
            named[r.endpoint] = named.get(r.endpoint, 0) + 1
        with _SEEN_LOCK:
            _SEEN[self.tag] = {ep: (n, server_count(ep)) for ep, n in named.items()}
        return self.inner.resolve_services(value)


def spied(plan: Any, tag: str) -> Any:
    return replace(plan, process=ResolveSpy(plan.process, tag))


def seen(tag: str) -> dict[str, tuple[int, int]]:
    with _SEEN_LOCK:
        return dict(_SEEN.get(tag, {}))


# ---- ports and user-run servers -------------------------------------------------------------------


def _bindable(host: str, port: int) -> bool:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        if sys.platform != "win32":  # skip TIME_WAIT only; a LISTEN socket still refuses the bind
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, port))
        return True
    except OSError:
        return False
    finally:
        s.close()


def listening(port: int, host: str = "127.0.0.1") -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1.0)
        return s.connect_ex((host, port)) == 0


def port_free(port: int) -> bool:
    """Nothing listens on ``port``: it binds on loopback and on all interfaces, and a connect is refused."""
    return _bindable("127.0.0.1", port) and _bindable("", port) and not listening(port)


def free_range(n: int) -> tuple[int, int]:
    """An inclusive range of ``n`` consecutive ports free right now, above the 10000-10100 site band."""
    for _ in range(200):
        low = 20000 + secrets.randbelow(30000)
        if all(port_free(p) for p in range(low, low + n)):
            return (low, low + n - 1)
    raise AssertionError(f"no {n} consecutive free ports found")


@contextlib.contextmanager
def user_servers(n: int) -> Iterator[list[str]]:
    """``n`` ``python -m histserv`` children the user started, as ``tcp://127.0.0.1:<port>``; killed
    at teardown."""
    low, _high = free_range(n)
    procs: list[subprocess.Popen[bytes]] = []
    try:
        for port in range(low, low + n):
            argv = [sys.executable, "-m", "histserv", "--port", str(port), "--log-level", "WARNING"]
            procs.append(subprocess.Popen(argv))
        deadline = time.monotonic() + SERVER_UP_S
        for port, proc in zip(range(low, low + n), procs, strict=True):
            while not listening(port):
                assert proc.poll() is None, f"histserv on {port} exited with {proc.returncode}"
                assert time.monotonic() < deadline, f"histserv on {port} not listening in {SERVER_UP_S}s"
                time.sleep(0.1)
        yield [f"tcp://127.0.0.1:{port}" for port in range(low, low + n)]
    finally:
        for proc in procs:
            proc.kill()
            proc.wait()


# ---- equality ---------------------------------------------------------------------------------------


def hist_bytes(h: Any) -> tuple[Any, ...]:
    """What bit-for-bit equality compares: the class, the storage, every axis's edges and the flow view."""
    assert isinstance(h, bh.Histogram), type(h)
    edges = tuple(np.asarray(axis.edges).tobytes() for axis in h.axes)
    return (h.storage_type, edges, np.asarray(h.view(flow=True)).tobytes())


def same_values(got: Mapping[Any, Any], want: Mapping[Any, Any]) -> bool:
    return set(got) == set(want) and all(hist_bytes(got[k]) == hist_bytes(want[k]) for k in want)
