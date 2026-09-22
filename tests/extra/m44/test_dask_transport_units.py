"""Per-file coverage-gate witnesses for ``dask_backend/transport.py`` (the >=90% ratchet).

The frozen m44 suite drives this module through real clusters and so only reaches its happy paths.
The surfaces below are the ones a cluster run never takes: the §1.7 canary's connect-class fallback
and its two refusals, the block plane's stale-epoch refusal and evict-after-serve, the bounded
inbox's drop signal, the purge's on-disk cleanup, and the driver endpoint's put/poll paths. Each
test drives the REAL object against a worker/client stand-in carrying only the attributes the
transport touches, and asserts the mechanism's witness (counters, the served bytes, the unlinked
spill file, the recorded ``client.run`` targets), never just a return code.
"""

from __future__ import annotations

import asyncio
import os
import pickle
import queue
import threading
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("distributed")

from graphed_executors.dask_backend import transport as T
from graphed_executors.dask_backend._transport_run import DRIVER, PLUGIN_NAME

_ADDR = "tcp://127.0.0.1:9001"
_PEER = "tcp://127.0.0.1:9002"


# ---- stand-ins: only the attributes transport.py actually touches -------------------------------
class _Rpc:
    """One ``worker.rpc(dest)`` endpoint; each op's reply function returns an answer or raises."""

    def __init__(self, dest: str, worker: _Worker) -> None:
        self._dest = dest
        self._worker = worker

    async def graphed_transport_ping(self, nonce: str) -> Any:
        return self._worker.ping(nonce)

    async def graphed_transport_recv(self, src: str, epoch: str, data: Any) -> Any:
        self._worker.sent.append((self._dest, data))
        return {"accepted": True}

    async def graphed_block_pull(self, epoch: str, digests: list[str]) -> Any:
        self._worker.sent.append((self._dest, tuple(digests)))
        return self._worker.reply(digests)


class _Worker:
    def __init__(self, local_directory: str, ping: Any = None, reply: Any = None) -> None:
        self.address = _ADDR
        self.local_directory = local_directory
        self.handlers: dict[str, Any] = {}
        self.extensions: dict[str, Any] = {}
        self.thread_id = threading.get_ident()
        self.events: list[tuple[str, Any]] = []
        self.sent: list[tuple[str, Any]] = []
        self.loop: Any = None
        self.ping = ping if ping is not None else _echo
        self.reply = reply if reply is not None else (lambda _x: {"wires": []})

    def rpc(self, dest: str) -> _Rpc:
        return _Rpc(dest, self)

    def log_event(self, topic: str, payload: Any) -> None:
        self.events.append((topic, payload))


def _echo(nonce: str) -> dict[str, str]:
    return {"nonce": nonce}


def _refused(_x: Any) -> Any:
    raise OSError("connection refused")  # the connect class: a nanny restart, not seam drift


class _DriftedPlugin(T.GraphedTransportPlugin):
    """A worker whose handler dict accepts the registration but does not dispatch it: the in-process
    fallback must NOT be able to paper over that."""

    def _on_ping(self, nonce: str) -> dict[str, Any]:
        return {"nonce": "not-the-probe"}


async def _setup(plugin: T.GraphedTransportPlugin, worker: _Worker) -> None:
    worker.loop = asyncio.get_running_loop()
    await plugin.setup(worker)


def _ready(tmp_path: Path, epoch: str = "e1", **kw: Any) -> tuple[T.GraphedTransportPlugin, _Worker, Any]:
    worker = _Worker(str(tmp_path))
    plugin = T.GraphedTransportPlugin()
    asyncio.run(_setup(plugin, worker))
    spec = T.make_transport_spec(epoch, (_ADDR, _PEER), **kw)
    plugin.ensure_run(spec)
    return plugin, worker, spec


# ---- §1.7 canary: the green arm, the narrow fallback, and the two refusals ----------------------
def test_canary_green_arm_is_the_real_rpc_round_trip(tmp_path: Path) -> None:
    worker = _Worker(str(tmp_path))
    plugin = T.GraphedTransportPlugin()
    asyncio.run(_setup(plugin, worker))
    assert plugin.canary_arm == "rpc", "a healthy cluster must validate the seam over a real socket"
    assert plugin.canary_ok is True
    assert set(worker.handlers) == {"graphed_transport_recv", "graphed_block_pull", "graphed_transport_ping"}
    assert worker.extensions[PLUGIN_NAME] is plugin


def test_canary_falls_back_to_direct_dispatch_only_on_a_connect_class_failure(tmp_path: Path) -> None:
    # a nanny RESTART: the worker's own server is not accepting self-connections yet, so the bounded
    # self-RPC fails connect-class and the same seam is checked in process instead of deadlocking.
    worker = _Worker(str(tmp_path), ping=_refused)
    plugin = T.GraphedTransportPlugin()
    asyncio.run(_setup(plugin, worker))
    assert plugin.canary_arm == "direct"
    assert plugin.canary_ok is True


def test_canary_refuses_a_seam_that_does_not_dispatch(tmp_path: Path) -> None:
    worker = _Worker(str(tmp_path), ping=_refused)
    plugin = _DriftedPlugin()
    with pytest.raises(RuntimeError, match="did not dispatch/echo"):
        asyncio.run(_setup(plugin, worker))
    assert plugin.canary_ok is False


def test_canary_refuses_an_rpc_that_answers_without_echoing_the_nonce(tmp_path: Path) -> None:
    worker = _Worker(str(tmp_path), ping=lambda _n: {"nonce": "someone-elses"})
    plugin = T.GraphedTransportPlugin()
    with pytest.raises(RuntimeError, match="did not echo the nonce"):
        asyncio.run(_setup(plugin, worker))
    assert plugin.canary_ok is False


# ---- recv accounting + the bounded inbox --------------------------------------------------------
def test_recv_off_the_io_loop_is_counted_apart_from_on_loop(tmp_path: Path) -> None:
    plugin, worker, spec = _ready(tmp_path)
    worker.thread_id = -1  # this call is NOT on the worker's event-loop thread
    assert plugin._on_recv(src=_PEER, epoch=spec.epoch, data=pickle.dumps("m")) == {"accepted": True}
    counters = plugin.counters(spec.epoch)
    assert counters["recv_invocations"] == 1
    assert counters["recv_on_loop"] == 0, "an off-loop delivery must not be counted as on-loop"


def test_deliver_local_refuses_an_unknown_epoch_and_signals_a_full_inbox(tmp_path: Path) -> None:
    plugin, _worker, spec = _ready(tmp_path, inbox_maxsize=1)
    assert plugin.deliver_local("no-such-epoch", DRIVER, "m") is False
    assert plugin.deliver_local(spec.epoch, DRIVER, "first") is True
    assert plugin.deliver_local(spec.epoch, DRIVER, "second") is False, "a full inbox must drop, not block"
    assert plugin.counters(spec.epoch)["sends_dropped"] == 1


# ---- the block plane: stale refusal, RAM+spill serve, evict-after-serve, purge ------------------
def test_block_pull_on_a_purged_epoch_is_refused_not_silently_empty(tmp_path: Path) -> None:
    plugin, _worker, _spec = _ready(tmp_path)
    reply = asyncio.run(plugin._on_block_pull(epoch="gone", digests=["d0"]))
    assert reply == {"stale": True}, "a lost block must never read as zero rows"
    assert plugin.stale_epoch_rejects == 1


def test_block_pull_serves_ram_and_spilled_wires_then_evicts_them(tmp_path: Path) -> None:
    plugin, worker, spec = _ready(tmp_path)
    ram, disk = b"ram-wire", b"disk-wire-longer"
    plugin.store_block(spec.epoch, "d-ram", ram, to_disk=False)
    plugin.store_block(spec.epoch, "d-disk", disk, to_disk=True)
    run = plugin.ensure_run(spec)
    spilled_path = Path(run.spilled["d-disk"])
    assert spilled_path.read_bytes() == disk, "the spilled wire really is on the producer's disk"

    async def _pull() -> Any:
        worker.loop = asyncio.get_running_loop()
        return await plugin._on_block_pull(epoch=spec.epoch, digests=["d-ram", "d-disk"])

    reply = asyncio.run(_pull())
    assert [bytes(getattr(w, "data", w)) for w in reply["wires"]] == [ram, disk]
    counters = plugin.counters(spec.epoch)
    assert counters["bytes_served"] == len(ram) + len(disk)
    assert counters["serve_pid"] == os.getpid()
    # evict-after-serve: the reader plane never holds the producer store plus the gathered dest.
    assert run.blocks == {} and run.spilled == {}
    assert not spilled_path.exists()


def test_purge_unlinks_the_spill_and_forgets_the_epoch(tmp_path: Path) -> None:
    plugin, _worker, spec = _ready(tmp_path)
    plugin.store_block(spec.epoch, "d", b"wire", to_disk=True)
    spilled_path = Path(plugin.ensure_run(spec).spilled["d"])
    assert spilled_path.exists()
    assert plugin.active_epochs() == (spec.epoch,)
    plugin.purge(spec.epoch)
    assert not spilled_path.exists()
    assert plugin.active_epochs() == ()
    assert plugin.counters(spec.epoch) == {}


def test_record_maxes_the_peak_assigns_the_pid_and_sums_the_rest(tmp_path: Path) -> None:
    plugin, _worker, spec = _ready(tmp_path)
    plugin.record(spec.epoch, peak_holder_bytes=500, serve_pid=11, holder_spill_count=1)
    plugin.record(spec.epoch, peak_holder_bytes=100, serve_pid=22, holder_spill_count=1)
    counters = plugin.counters(spec.epoch)
    assert counters["peak_holder_bytes"] == 500, "a peak is a max, never the last value"
    assert counters["serve_pid"] == 22, "a pid is assigned, never summed"
    assert counters["holder_spill_count"] == 2


# ---- worker-side endpoint: the bounded overlay, broadcast, close --------------------------------
def test_overlay_bounds_the_outbox_and_broadcast_skips_the_driver(tmp_path: Path, monkeypatch: Any) -> None:
    plugin, worker, spec = _ready(tmp_path, overlay={_ADDR: (_PEER, DRIVER)})
    # the task-thread -> IO-loop bridge is dask's; what is under test is which peers broadcast dials.
    monkeypatch.setattr(T, "sync", lambda _loop, fn, *a: asyncio.run(fn(*a)))
    endpoint = T.DaskWorkerTransport(plugin, spec)
    assert endpoint.peers() == (_PEER, DRIVER)
    assert spec.peers_of("tcp://127.0.0.1:9999") == (), "an address outside the overlay may send nowhere"
    endpoint.broadcast("hello")
    assert [dest for dest, _payload in worker.sent] == [_PEER], (
        "broadcast is peer-only; driver rides log_event"
    )
    assert worker.events == [], "no driver traffic from a broadcast"
    endpoint.close()  # per-epoch state dies with the driver purge, not with the endpoint


def test_root_send_rides_log_event_and_arms_the_none_safe_root_witness(tmp_path: Path) -> None:
    plugin, worker, spec = _ready(tmp_path)
    endpoint = T.DaskWorkerTransport(plugin, spec)
    assert endpoint.has_root is False
    assert endpoint.send(DRIVER, ("root", None)) is True
    assert endpoint.has_root is True, "a genuinely-None root must not read as 'no root captured'"
    assert endpoint.root_sent is None
    [(topic, (src, blob))] = worker.events
    assert topic.endswith(spec.epoch) and src == _ADDR
    assert pickle.loads(blob) == ("root", None)


# ---- task-side helpers: the missing plugin, the pre-created inbox, the pull classifier ----------
def test_a_missing_plugin_is_a_loud_error_on_the_task_side(tmp_path: Path, monkeypatch: Any) -> None:
    worker = _Worker(str(tmp_path))
    monkeypatch.setattr("distributed.get_worker", lambda: worker)
    with pytest.raises(RuntimeError, match="plugin is not registered"):
        T._get_plugin()
    plugin, worker2, spec = _ready(tmp_path)
    monkeypatch.setattr("distributed.get_worker", lambda: worker2)
    assert T.open_endpoint(spec)._plugin is plugin


def test_ensure_run_probe_creates_the_inbox_before_any_peer_send(tmp_path: Path) -> None:
    worker = _Worker(str(tmp_path))
    plugin = T.GraphedTransportPlugin()
    asyncio.run(_setup(plugin, worker))
    spec = T.make_transport_spec("e-probe", (_ADDR, _PEER))
    assert plugin.active_epochs() == ()
    assert T._ensure_run_probe(spec, dask_worker=worker) is True
    assert plugin.active_epochs() == ("e-probe",), "the inbox must exist before the first peer send"
    # a worker without the plugin must answer the probe, not raise (the engine probes every worker)
    assert T._ensure_run_probe(spec, dask_worker=_Worker(str(tmp_path))) is True


@pytest.mark.parametrize(
    ("reply", "exc", "match"),
    [
        (lambda _d: (_ for _ in ()).throw(TimeoutError()), T.PullTimeoutError, "holder slow or dead"),
        (lambda _d: {"stale": True}, RuntimeError, "stale/purged epoch"),
    ],
)
def test_a_lost_holder_is_classified_never_read_as_zero_rows(
    tmp_path: Path, monkeypatch: Any, reply: Any, exc: type[Exception], match: str
) -> None:
    worker = _Worker(str(tmp_path), reply=reply)
    monkeypatch.setattr("distributed.get_worker", lambda: worker)
    # the task-thread -> IO-loop bridge is dask's; the classifier under test is the coroutine body.
    monkeypatch.setattr(T, "sync", lambda _loop, fn, *a: asyncio.run(fn(*a)))
    with pytest.raises(exc, match=match):
        T.pull_blocks("e1", _PEER, ["d0"], timeout_s=0.1)


def test_pull_blocks_coalesces_one_rpc_per_holder(tmp_path: Path, monkeypatch: Any) -> None:
    worker = _Worker(str(tmp_path), reply=lambda ds: {"wires": [f"w-{d}".encode() for d in ds]})
    monkeypatch.setattr("distributed.get_worker", lambda: worker)
    monkeypatch.setattr(T, "sync", lambda _loop, fn, *a: asyncio.run(fn(*a)))
    assert T.pull_blocks("e1", _PEER, ["a", "b"]) == [b"w-a", b"w-b"]
    assert worker.sent == [(_PEER, ("a", "b"))], "a batch is ONE rpc (the incast bound), not one per digest"


# ---- driver-side endpoint + the one-liner registration -------------------------------------------
class _Client:
    def __init__(self) -> None:
        self.runs: list[tuple[str, Any, list[str]]] = []
        self.plugins: list[Any] = []

    def run(self, fn: Any, epoch: str, message: Any, workers: list[str]) -> None:
        self.runs.append((epoch, message, list(workers)))

    def register_plugin(self, plugin: Any) -> None:
        self.plugins.append(plugin)


class _Backend:
    def __init__(self) -> None:
        self._client = _Client()
        self.topics: list[str] = []
        self.unsubscribed = 0

    def subscribe_events(self, topic: str, cb: Any) -> Any:
        self.topics.append(topic)
        self.cb = cb
        return self._unsub

    def _unsub(self) -> None:
        self.unsubscribed += 1


def test_driver_endpoint_puts_to_targets_and_drains_what_workers_logged() -> None:
    backend = _Backend()
    spec = T.make_transport_spec("e-driver", (_ADDR, _PEER))
    driver = T.open_driver_endpoint(backend, spec)
    assert driver.address == DRIVER and driver.peers() == (_ADDR, _PEER)
    assert backend.topics == [f"{PLUGIN_NAME}-e-driver"]

    driver.send(_PEER, "unicast")
    driver.broadcast("to-all")
    assert [(msg, targets) for _epoch, msg, targets in backend._client.runs] == [
        ("unicast", [_PEER]),
        ("to-all", [_ADDR, _PEER]),
    ]

    assert driver.poll() == [], "an empty inbox drains to nothing, it does not block"
    backend.cb((_PEER, pickle.dumps(("root", 7))))
    backend.cb((_ADDR, pickle.dumps("second")))
    assert driver.poll() == [(_PEER, ("root", 7)), (_ADDR, "second")]
    assert driver.recv(timeout=0.01) is None
    driver.close()
    assert backend.unsubscribed == 1


def test_broadcast_put_delivers_into_the_targets_live_epoch(tmp_path: Path) -> None:
    plugin, worker, spec = _ready(tmp_path)
    assert T._broadcast_put(spec.epoch, "m", dask_worker=worker) is True
    assert plugin.counters(spec.epoch)["sends_dropped"] == 0
    endpoint = T.DaskWorkerTransport(plugin, spec)
    assert endpoint.recv(timeout=0.01) == (DRIVER, "m")
    # a worker that never registered the plugin reports the drop instead of pretending delivery
    assert T._broadcast_put(spec.epoch, "m", dask_worker=_Worker(str(tmp_path))) is False


def test_the_setup_one_liner_registers_both_engine_plugins() -> None:
    client = _Client()
    T.dask_transport_setup(client)
    assert [type(p).__name__ for p in client.plugins] == ["GraphedWorkerPlugin", "GraphedTransportPlugin"]
    client2 = _Client()
    T.ensure_engine_plugins(client2)
    assert [type(p).__name__ for p in client2.plugins] == ["GraphedWorkerPlugin"], (
        "the transport plugin is the caller's to register, so an armed injection seam survives"
    )


def test_an_inbox_full_recv_answers_the_drop_signal(tmp_path: Path) -> None:
    plugin, _worker, spec = _ready(tmp_path, inbox_maxsize=1)
    payload_a, payload_b = pickle.dumps("a"), pickle.dumps("b")
    assert plugin._on_recv(src=_PEER, epoch=spec.epoch, data=payload_a) == {"accepted": True}
    assert plugin._on_recv(src=_PEER, epoch=spec.epoch, data=payload_b) == {"accepted": False}
    assert plugin.counters(spec.epoch)["sends_dropped"] == 1
    run = plugin.ensure_run(spec)
    assert run.inbox.get_nowait() == (_PEER, "a")
    with pytest.raises(queue.Empty):
        run.inbox.get_nowait()
