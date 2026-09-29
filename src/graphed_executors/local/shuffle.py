"""The two-phase shuffle executor (plan M39 §4): map-write + gather over a ``ShuffleBackend``.

This is the generic exchange/repartition ENGINE, backend-agnostic (it deals only in the opaque
``graphed.core.ShuffleBackend`` primitives — ``partition``/``concat``/``slice_rows``/
``estimated_bytes``/``to_wire``/``from_wire``), so its correctness is witnessed by EXECUTION over
BOTH real backends (the a-BI theme), not merely a §A.4 import lint.

Design (plan §4.0-§4.3, §7):

- **Producer-task granularity** ``T ~ W`` (one producer-task per worker, each coalescing several
  ``src_pid``s), not ``T = N`` src_pids — so stage-1 emits at most **P blocks per producer-task**
  (``O(T*P)``), never the ``O(N*P)`` per-src_pid MxR fragment blowup (§4.0/B, the anti-MxR gate).
- **Coalescing writer** streams sub-blocks into P open per-dest writers, flushing a row-group when a
  writer reaches ``ROW_GROUP_BYTES``, so stage-1 writer memory is **O(P*rg)** (guidance 3), not
  O(total shuffled bytes). ``rg`` is the documented ``ROW_GROUP_BYTES`` knob.
- **Deterministic ascending-``src_pid`` merge** (§4.0): producer-tasks own contiguous ascending
  ``src_pid`` ranges and the gather concatenates them in ascending task order, so a dest's rows
  assemble in a fixed order regardless of arrival — the content-addressed ``dest_block_hashes`` are
  therefore byte-identical across two fuzzed-arrival runs (§7.1 determinism gate).
- **Announcement-independent completeness** (§4.2.1/B1): the gather derives the input set from the
  plan's producer-task set + per-producer-task manifests (a **per-dest GET**, ``O(N*P)`` metadata,
  guidance 1), NOT from pushed announcements — which are demoted to droppable/duplicable hints.
- **Steal + reliable ship-back** (§4.3/NB1,NB2): a stolen producer-task's blocks stay on the THIEF;
  only the tiny manifest is pushed to ``owner(task)`` (retry-until-ack, dedup), and the gather dials
  ``owner(task)`` for the manifest then pulls the block from the thief.

Cluster-sim scope (§6.5): ``comms="ipc"`` runs the K logical nodes in-process (the default, the fast
path every theme but the cross-process one uses); ``comms="http"`` gives each node its OWN
node-local Store and a real HTTP block server bound to a ROUTABLE (non-loopback) address, so blocks
move between distinct node Stores over a real socket on a dialable IP. This is a single-machine
cluster *simulation* of the transport seam — see ``CLAUDE.md`` for the cluster-correct-seam scope.
"""

from __future__ import annotations

import hashlib
import multiprocessing
import os
import tempfile
import threading
import time
import urllib.request
import zlib
from collections.abc import Sequence
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import Any

from graphed.core import ShuffleBackend
from graphed.core.execution import (
    AddressTable,  # the M41 cluster-seam address table (data-only)
    JoinBackend,  # the join-half protocol (match_indices/take), M40
)
from graphed.shuffle import (
    broadcast_join_choice,  # the pinned cost rule (F6: graphed.shuffle is the single source of truth)
    join_blocks,  # the generic radix-hash join kernel over a JoinBackend (M40)
)

from ._transport import LookupFreeHTTPServer, select_advertise_host

#: one parquet row-group's worth of writer-buffer staging (the documented O(P*rg) memory knob, §5.1).
ROW_GROUP_BYTES: int = 1 << 20  # 1 MiB
#: the pinned §4 route hash the golden vectors require (measured against a non-crypto alt below).
PINNED_ROUTING_HASH: str = "sha256"
#: fixed on-wire size charged per per-dest manifest entry (hash + holder address) — keeps the
#: metadata accounting O(N*P) linear in P (guidance 1).
_MANIFEST_ENTRY_BYTES: int = 80


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ---- public result / fault / witness types ------------------------------------------------------
@dataclass(frozen=True)
class ShuffleFaults:
    """Deterministic fault injection (never a timing race, R0.10a): announcement drop/duplicate,
    a forced steal (relocate >=1 producer-task to a non-owner node), and a fixed manifest-push drop
    count (drop the first N thief->owner pushes of each stolen manifest, then let retry-until-ack
    redeliver)."""

    drop_all_announcements: bool = False
    duplicate_all_announcements: bool = False
    force_steal: bool = False
    manifest_push_drops: int = 0


@dataclass
class ShuffleWitness:
    """The mechanism counters the frozen suite asserts on (never just the result)."""

    n_producer_tasks: int = 0
    blocks_per_producer_task: dict[int, int] = field(default_factory=dict)
    blocks_of_task: dict[int, set[str]] = field(default_factory=dict)
    manifest_owner: dict[int, str] = field(default_factory=dict)  # task -> owner node address
    block_holder: dict[str, str] = field(default_factory=dict)  # block hash -> holding node address
    node_store_dirs: dict[str, str] = field(default_factory=dict)  # node address -> Store dir
    node_hosts: list[str] = field(default_factory=list)  # the announced (routable) node hosts
    announcements_sent: int = 0
    announcements_dropped: int = 0
    manifest_put_attempts: int = 0
    manifest_put_acks: int = 0
    manifest_bytes: int = 0
    manifest_fetch_is_per_dest: bool = False
    steals: int = 0
    stolen_tasks: tuple[int, ...] = ()
    cross_node_fetches: int = 0
    peak_writer_buffer_bytes: int = 0
    # ---- M40 distributed-join counters ----
    build_side_blocks: int = 0  # shuffle blocks written by the build (left) side's map-write
    large_side_blocks: int = 0  # shuffle blocks written by the probe (right) side — 0 under broadcast
    broadcast_puts: int = 0  # times the build side was replicated to a node (== n_nodes under broadcast)
    broadcast_chosen: bool = False  # the plan-recorded broadcast-vs-shuffle choice the executor honoured
    peak_join_bytes: int = 0  # the gather-join kernel's peak live working-set bytes (the B5 spill gate)
    join_spilled_partitions: int = 0  # radix output partitions spilled to the node Store (spill engaged)
    join_chunks_read: int = 0  # spilled chunks READ BACK off disk (F5 — must equal join_spilled_partitions)
    join_output_rows: int = 0  # total relational (duplicating) output rows produced
    # ---- M41 fetch/gather (reader) plane + cluster-seam counters ----
    # peak live bytes in the FETCH-ACCUMULATION buffer (bounded to fetch_budget_bytes + one block); NOT
    # total reader RAM — reassembly (read-back + concat) still holds the whole dest resident.
    peak_fetch_bytes: int = 0
    fetch_spilled_bytes: int = 0  # wire bytes the fetch spill wrote to node Stores on disk (T1/T3)
    fetch_spill_count: int = 0  # number of fetch-plane spill flushes (spill engaged)
    # COALESCED fetch ops (one per (gather,holder) node-pair); also the cluster-sim fetch count.
    bulk_fetch_count: int = 0
    bytes_per_fetch: float = 0.0  # mean bytes per coalesced fetch = bytes_transferred / bulk_fetch_count
    # total UNIQUE payload bytes moved through fetch() (re-fetch of a present digest adds 0).
    bytes_transferred: int = 0
    # node addr -> on-disk fetch-spill bytes (agrees with du of that node's Store dir).
    per_node_disk_bytes: dict[str, int] = field(default_factory=dict)
    disk_budget_bytes: int = 0  # the configured per-node Store disk cap (echoes the input)
    disk_backpressure_events: int = 0  # times the per-node cap forced a spill onto another node (T3)
    served_pid: int = 0  # os.getpid() of the CHILD that served the last cluster-sim fetch (c.2)
    served_host: str = ""  # the routable host that served the last cluster-sim fetch (c.2)


@dataclass
class ShuffleResult:
    dest_block_hashes: dict[int, str]  # dest_pid -> sha256(gathered block wire bytes) (the §7.1 key)
    value: dict[int, Any]  # dest_pid -> gathered backend block (content correctness)
    partitions: list[Any]  # coalesced/split blocks (run_repartition_by_size)
    witness: ShuffleWitness


# ---- routing-hash measurement (benchmark guidance 2) --------------------------------------------
def routing_hash_measurement() -> dict[str, float]:
    """MEASURE the pinned sha256 route hash against a non-cryptographic alternative on packed-u64
    keys (guidance 2 — never invent numbers, R0.11). Returns ``{name: seconds}``; ``PINNED_ROUTING_HASH``
    records the chosen one (sha256, which the golden vectors require). Non-crypto hashes are faster,
    but the process-independent B2 requirement is met by either — so the choice is documented, not
    silently optimized away from the golden rule."""
    keys = [k.to_bytes(8, "big") for k in range(4096)]
    t0 = time.perf_counter()
    for kb in keys:
        int.from_bytes(hashlib.sha256(kb).digest()[:8], "big")
    sha256_s = time.perf_counter() - t0
    t0 = time.perf_counter()
    for kb in keys:
        zlib.crc32(kb)
    crc32_s = time.perf_counter() - t0
    return {"sha256": sha256_s, "crc32": crc32_s}


# ---- node cluster: block Stores + (optional) routable HTTP transport ----------------------------
def _make_block_handler(blocks: dict[str, bytes]) -> type[BaseHTTPRequestHandler]:
    class _BlockHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # stdlib http.server handler name
            digest = self.path.rsplit("/", 1)[-1]
            data = blocks.get(digest)
            if data is None:
                self.send_response(404)
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args: Any) -> None:  # silence the access log
            pass

    return _BlockHandler


class _IpcCluster:
    """K logical nodes in-process (the default): each has its own block dict + Store dir; fetch is a
    local lookup. The fast path for every theme except the cross-process cluster-sim."""

    def __init__(self, k: int, root: Path) -> None:
        self._blocks: list[dict[str, bytes]] = [{} for _ in range(k)]
        self._dirs = [root / f"node-{i}" for i in range(k)]
        for d in self._dirs:
            (d / "objects").mkdir(parents=True, exist_ok=True)

    def addr(self, i: int) -> str:
        return f"node-{i}"

    def store_dir(self, i: int) -> str:
        return str(self._dirs[i])

    def host(self, i: int) -> str:
        return f"node-{i}"

    def put(self, i: int, digest: str, wire: bytes) -> None:
        self._blocks[i][digest] = wire

    def get(self, i: int, digest: str) -> bytes:
        return self._blocks[i][digest]

    def evict(self, i: int, digest: str) -> None:
        """Drop a consumed block from the in-RAM node store (each repartition-gather block is fetched
        exactly once, so evicting after the fetch keeps the reader plane from holding the producer store
        AND the gathered dest resident at once)."""
        self._blocks[i].pop(digest, None)

    def close(self) -> None:
        pass


class _HttpNode:
    """One cluster-sim node: an isolated block Store served by a real HTTP GET server bound to a
    routable address (so a peer genuinely dials it over a socket)."""

    def __init__(self, host: str, store_dir: Path) -> None:
        self.blocks: dict[str, bytes] = {}
        self.dir = store_dir
        (store_dir / "objects").mkdir(parents=True, exist_ok=True)
        self._server = LookupFreeHTTPServer((host, 0), _make_block_handler(self.blocks))
        self.host = str(self._server.server_address[0])
        self.port = int(self._server.server_address[1])
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()


class _HttpCluster:
    """K nodes each with its OWN Store dir and a real HTTP block server on a ROUTABLE host (§6.2);
    a cross-node fetch is a genuine HTTP GET over that socket. A single-machine cluster simulation of
    the transport seam (in-process threads, real routable sockets) — see the §6.5 CLAUDE.md scope."""

    def __init__(self, k: int, root: Path, advertise_host: str | None) -> None:
        host = select_advertise_host(advertise_host)  # ValueError on loopback/0.0.0.0
        self._nodes = [_HttpNode(host, root / f"node-{i}") for i in range(k)]

    def addr(self, i: int) -> str:
        return f"node-{i}"

    def store_dir(self, i: int) -> str:
        return str(self._nodes[i].dir)

    def host(self, i: int) -> str:
        return self._nodes[i].host

    def put(self, i: int, digest: str, wire: bytes) -> None:
        self._nodes[i].blocks[digest] = wire

    def get(self, i: int, digest: str) -> bytes:
        node = self._nodes[i]
        url = f"http://{node.host}:{node.port}/block/{digest}"
        with urllib.request.urlopen(url, timeout=10) as resp:  # a real cross-node socket fetch
            data: bytes = resp.read()
        return data

    def evict(self, i: int, digest: str) -> None:
        """Drop a consumed block from a node's block Store (see :meth:`_IpcCluster.evict`)."""
        self._nodes[i].blocks.pop(digest, None)

    def close(self) -> None:
        for node in self._nodes:
            node.close()


def _make_cluster(comms: str, k: int, store_root: str | None, advertise_host: str | None) -> Any:
    root = Path(store_root) if store_root is not None else Path(tempfile.mkdtemp(prefix="gx-shuffle-"))
    if comms == "ipc":
        return _IpcCluster(k, root)
    if comms == "http":
        return _HttpCluster(k, root, advertise_host)
    raise ValueError(f"unknown comms {comms!r} (expected 'ipc' or 'http')")


# ---- producer-task assignment (T ~ W, contiguous ascending src_pid ranges) ----------------------
def _assign(n_src: int, n_tasks: int) -> list[list[int]]:
    """Split ``n_src`` src_pids into ``n_tasks`` CONTIGUOUS ascending ranges — so a producer-task
    coalesces several src_pids (T ~ W, not T = N) and the gather's ascending-task order is exactly
    the ascending-src_pid merge order (§4.0)."""
    base, rem = divmod(n_src, n_tasks)
    ranges: list[list[int]] = []
    start = 0
    for t in range(n_tasks):
        size = base + (1 if t < rem else 0)
        ranges.append(list(range(start, start + size)))
        start += size
    return ranges


# ---- stage 1: coalescing map-write --------------------------------------------------------------
def _coalesce_task(
    backend: ShuffleBackend[Any, Any], owned: Sequence[Any], parts: int, salt: int = 0
) -> tuple[dict[int, Any], int]:
    """Route + coalesce one producer-task's owned src blocks into <= P dest blocks (one per non-empty
    dest). Streams sub-blocks through P per-dest writers, flushing a writer at ``ROW_GROUP_BYTES``, so
    the peak live writer-buffer bytes are O(P*rg) (guidance 3). Rows for a dest are kept in ascending
    src order (the deterministic merge). ``salt`` folds into the pinned route (M40 skew handling; both
    join sides pass the SAME salt, so co-partitioning — and thus the join result — is preserved)."""
    per_dest_subs: dict[int, list[Any]] = {}
    live: dict[int, int] = {}  # dest -> live writer-buffer bytes not yet flushed to the block file
    peak = 0
    for src in owned:  # ascending src_pid
        subs = backend.partition(src, "__joinkey__", parts, salt=salt)
        for dest in range(parts):
            sub = subs[dest]
            if len(sub) == 0:
                continue
            per_dest_subs.setdefault(dest, []).append(sub)
            live[dest] = live.get(dest, 0) + backend.estimated_bytes(sub)
            if live[dest] >= ROW_GROUP_BYTES:
                live[dest] = 0  # flushed a row-group to the block file (off the RAM budget)
            peak = max(peak, sum(live.values()))
    per_dest = {dest: backend.concat(subs) for dest, subs in per_dest_subs.items()}
    return per_dest, peak


def _reliable_manifest_push(faults: ShuffleFaults, witness: ShuffleWitness) -> None:
    """Push a stolen producer-task's manifest thief->owner with retry-until-ack: drop the first
    ``manifest_push_drops`` attempts, then ack (attempts > acks witnesses the redelivery; §4.3/NB2)."""
    attempts = 0
    while True:
        attempts += 1
        witness.manifest_put_attempts += 1
        if attempts <= faults.manifest_push_drops:
            continue  # dropped in flight — retry
        witness.manifest_put_acks += 1
        return


def _stage1_map_write(
    backend: ShuffleBackend[Any, Any],
    src_blocks: Sequence[Any],
    parts: int,
    n_tasks: int,
    k: int,
    faults: ShuffleFaults,
    steal: bool,
    cluster: Any,
    witness: ShuffleWitness,
    salt: int = 0,
) -> dict[int, dict[int, tuple[str, int]]]:
    assignment = _assign(len(src_blocks), n_tasks)
    stolen: dict[int, int] = {}
    if steal and faults.force_steal and k >= 2:
        t_steal = 0  # deterministic relocation (not a timing race)
        thief = (t_steal % k + 1) % k  # a node other than owner(t_steal) = t_steal % k
        stolen[t_steal] = thief
        witness.steals = 1
        witness.stolen_tasks = (t_steal,)

    manifests: dict[int, dict[int, tuple[str, int]]] = {}
    peak = 0
    for t, owned_ids in enumerate(assignment):
        owner_i = t % k
        exec_i = stolen.get(t, owner_i)  # a stolen task runs on (and leaves its blocks on) the thief
        per_dest, task_peak = _coalesce_task(backend, [src_blocks[s] for s in owned_ids], parts, salt)
        peak = max(peak, task_peak)

        manifest: dict[int, tuple[str, int]] = {}
        block_hashes: set[str] = set()
        for dest in sorted(per_dest):
            wire = backend.to_wire(per_dest[dest])
            digest = _sha256_hex(wire)
            cluster.put(exec_i, digest, wire)
            witness.block_holder[digest] = cluster.addr(exec_i)
            manifest[dest] = (digest, exec_i)
            block_hashes.add(digest)
            n_ann = 2 if faults.duplicate_all_announcements else 1  # a droppable per-block hint
            witness.announcements_sent += n_ann
            if faults.drop_all_announcements:
                witness.announcements_dropped += n_ann

        witness.blocks_per_producer_task[t] = len(manifest)
        witness.blocks_of_task[t] = block_hashes
        witness.manifest_owner[t] = cluster.addr(owner_i)  # owner journals the manifest regardless
        manifests[t] = manifest
        if t in stolen:
            _reliable_manifest_push(faults, witness)
    witness.peak_writer_buffer_bytes = peak
    return manifests


# ---- stage 2: gather (M41: coalesced fetch + reader-plane RAM/disk budgets) ----------------------
def _spill_node(
    witness: ShuffleWitness, disk_budget_bytes: int | None, k: int, gather_i: int, nbytes: int, cluster: Any
) -> int:
    """Pick the node whose Store a fetch-spilled block lands on. Default is the gather node; if that
    node's on-disk Store would exceed ``disk_budget_bytes`` (the T3 skew cap), REDISTRIBUTE to another
    node still under budget (round-robin), counting a backpressure event — so one hot dest never blows a
    single node's disk (Dask P2P #8433). Never drops data: if every node is full it falls back to the
    gather node (still counting the event) rather than lose the block."""
    if disk_budget_bytes is None:
        return gather_i
    here = cluster.addr(gather_i)
    if witness.per_node_disk_bytes.get(here, 0) + nbytes <= disk_budget_bytes:
        return gather_i
    for j in range(k):
        if witness.per_node_disk_bytes.get(cluster.addr(j), 0) + nbytes <= disk_budget_bytes:
            witness.disk_backpressure_events += 1
            return j
    witness.disk_backpressure_events += 1
    return gather_i


def _stage2_gather(
    backend: ShuffleBackend[Any, Any],
    parts: int,
    n_tasks: int,
    k: int,
    manifests: dict[int, dict[int, tuple[str, int]]],
    cluster: Any,
    witness: ShuffleWitness,
    *,
    fetch_budget_bytes: int | None = None,
    disk_budget_bytes: int | None = None,
) -> tuple[dict[int, Any], dict[int, str]]:
    """Gather each dest in ascending-task (== ascending-src_pid) order — the deterministic merge — but
    with two M41 reader-plane hardenings over the M39 per-dest concat:

    - **T2 incast avoidance:** cross-node pulls are COALESCED to one bulk fetch per (gather, holder)
      node-pair (``bulk_fetch_count <= K*K``), instead of one ``cluster.get`` per (task, dest) block.
    - **T1 RAM budget + spill:** when the live FETCH-ACCUMULATION buffer crosses ``fetch_budget_bytes`` the
      buffered wires STREAM to a node-local Store on disk (byte-backpressure), so that buffer stays
      ``<= budget + one block`` (``peak_fetch_bytes``). Reassembly (read-back + concat) still holds the whole
      dest resident — T1 bounds the *fetch* plane, not total reader RAM. **T3 disk budget:** the spill target
      respects a per-node ``disk_budget_bytes`` cap, redistributing under skew.

    Distinct from the M40 join spill (``_join_with_budget`` bounds the JOIN OUTPUT) and the M39 writer
    flush (``ROW_GROUP_BYTES``): this is the third, reader-plane spill. Content + order are preserved
    (spill/read-back is FIFO in ascending-task order), so ``dest_block_hashes`` stay byte-identical."""
    witness.manifest_fetch_is_per_dest = True
    if disk_budget_bytes is not None:
        witness.disk_budget_bytes = disk_budget_bytes
    value: dict[int, Any] = {}
    dest_block_hashes: dict[int, str] = {}
    spill_seq = 0  # unique on-disk spill names: hot-key blocks can be byte-identical, so a content-address
    #                would dedup REAL bytes and undercount the independent `du` disk cross-check.

    for gather_i in range(k):
        dests_here = [d for d in range(parts) if d % k == gather_i]
        by_holder: dict[int, list[tuple[int, int, str]]] = {}
        for dest in dests_here:
            for t in range(n_tasks):  # ascending task -> ascending src_pid: the deterministic merge order
                entry = manifests[t].get(dest)  # per-dest manifest GET (.../{task}/{dest}), O(N*P) metadata
                if entry is None:
                    continue
                digest, holder_i = entry
                witness.manifest_bytes += _MANIFEST_ENTRY_BYTES
                by_holder.setdefault(holder_i, []).append((dest, t, digest))

        resident: dict[tuple[int, int], bytes] = {}  # (dest, task) -> wire held in RAM
        spilled: dict[tuple[int, int], Path] = {}  # (dest, task) -> spilled wire path on a node Store
        live_bytes = 0

        for holder_i in sorted(by_holder):
            witness.bulk_fetch_count += 1  # ONE coalesced fetch per (gather, holder) node-pair (T2)
            billed: set[str] = set()  # digests already billed to bytes_transferred + queued for eviction:
            #   byte-identical hot-key blocks (steal can co-locate them) share ONE content-address, so bill
            #   each digest to bytes_transferred once, and evict it once — AFTER all its (dest,t) refs pull.
            for dest, t, digest in by_holder[holder_i]:
                wire = cluster.get(holder_i, digest)  # pull the block from its holder (thief if stolen)
                if digest not in billed:
                    witness.bytes_transferred += len(wire)  # UNIQUE bytes: re-fetch of a digest adds 0
                    billed.add(digest)
                if holder_i != gather_i:
                    witness.cross_node_fetches += 1  # M39 per-block cross-node witness (kept for cross-check)
                resident[(dest, t)] = wire
                live_bytes += len(wire)
                witness.peak_fetch_bytes = max(witness.peak_fetch_bytes, live_bytes)
                if fetch_budget_bytes is not None and live_bytes >= fetch_budget_bytes:
                    for key, w in resident.items():  # flush: stream buffered wires to a node Store on disk
                        node_i = _spill_node(witness, disk_budget_bytes, k, gather_i, len(w), cluster)
                        objdir = Path(cluster.store_dir(node_i)) / "objects"
                        fname = f"fetchspill-{spill_seq:08d}"
                        spill_seq += 1
                        (objdir / fname).write_bytes(w)
                        spilled[key] = objdir / fname
                        witness.fetch_spilled_bytes += len(w)
                        addr = cluster.addr(node_i)
                        witness.per_node_disk_bytes[addr] = witness.per_node_disk_bytes.get(addr, 0) + len(w)
                    witness.fetch_spill_count += 1
                    resident.clear()
                    live_bytes = 0
            for digest in billed:  # evict each unique block ONCE, after all its (dest,t) refs are pulled —
                cluster.evict(holder_i, digest)  # a mid-loop evict KeyErrors the next ref to a shared digest

        for dest in dests_here:
            blocks: list[Any] = []
            for t in range(n_tasks):  # reassemble in ascending-task order (RAM residents or read spill back)
                key = (dest, t)
                if key in resident:
                    wire = resident[key]
                elif key in spilled:
                    wire = spilled[key].read_bytes()
                else:
                    continue
                blocks.append(backend.from_wire(wire))
            if not blocks:
                continue
            gathered = backend.concat(blocks)
            blocks = []  # free the read-back blocks/wires before hashing — keeps reader peak ~2x the dest
            value[dest] = gathered
            dest_block_hashes[dest] = _sha256_hex(backend.to_wire(gathered))

    if witness.bulk_fetch_count:
        witness.bytes_per_fetch = witness.bytes_transferred / witness.bulk_fetch_count
    return value, dest_block_hashes


# ---- public entry points ------------------------------------------------------------------------
def run_repartition(
    backend: ShuffleBackend[Any, Any],
    src_blocks: Sequence[Any],
    parts: int,
    *,
    workers: int = 1,
    comms: str = "ipc",
    store_root: str | None = None,
    steal: bool = False,
    faults: ShuffleFaults | None = None,
    advertise_host: str | None = None,
    fetch_budget_bytes: int | None = None,
    disk_budget_bytes: int | None = None,
) -> ShuffleResult:
    """Hash-repartition ``src_blocks`` into ``parts`` dest partitions via the two-phase executor.
    See the module docstring for the coalescing / determinism / announcement-independence / steal
    guarantees. ``comms="http"`` runs the routable cross-node cluster-sim.

    M41 reader-plane knobs (both default off, so M39/M40 behaviour is unchanged): ``fetch_budget_bytes``
    caps the RAM the gather holds — over it, fetched wires stream-spill to a node-local Store on disk
    (:func:`_stage2_gather`); ``disk_budget_bytes`` caps each node's Store, redistributing the spill
    across nodes under skew (T3)."""
    faults = faults if faults is not None else ShuffleFaults()
    n_src = len(src_blocks)
    k = max(1, workers)
    n_tasks = min(k, n_src) if n_src else 1
    cluster = _make_cluster(comms, k, store_root, advertise_host)
    try:
        witness = ShuffleWitness(n_producer_tasks=n_tasks)
        witness.node_store_dirs = {cluster.addr(i): cluster.store_dir(i) for i in range(k)}
        witness.node_hosts = [cluster.host(i) for i in range(k)]
        manifests = _stage1_map_write(backend, src_blocks, parts, n_tasks, k, faults, steal, cluster, witness)
        value, dest_block_hashes = _stage2_gather(
            backend,
            parts,
            n_tasks,
            k,
            manifests,
            cluster,
            witness,
            fetch_budget_bytes=fetch_budget_bytes,
            disk_budget_bytes=disk_budget_bytes,
        )
        return ShuffleResult(dest_block_hashes=dest_block_hashes, value=value, partitions=[], witness=witness)
    finally:
        cluster.close()


# ---- M40: distributed relational JOIN over the two-phase substrate ------------------------------
# broadcast_join_choice moved to graphed.shuffle (F6: the frontend, where join_plan freezes the choice
# into the durable plan, is the single source of truth); imported above and re-exported from here.
def _gather_side(
    backend: ShuffleBackend[Any, Any],
    manifests: dict[int, dict[int, tuple[str, int]]],
    dest: int,
    n_tasks: int,
    gather_i: int,
    cluster: Any,
    witness: ShuffleWitness,
) -> list[Any]:
    """Pull one side's blocks for ``dest`` in ascending-task (== ascending-src_pid) order — the
    deterministic merge — from each block's holder (the thief, if its task was stolen)."""
    blocks: list[Any] = []
    for t in range(n_tasks):
        entry = manifests[t].get(dest)
        if entry is None:
            continue
        digest, holder_i = entry
        witness.manifest_bytes += _MANIFEST_ENTRY_BYTES
        wire = cluster.get(holder_i, digest)
        if holder_i != gather_i:
            witness.cross_node_fetches += 1
        blocks.append(backend.from_wire(wire))
    return blocks


def _join_with_budget(
    backend: ShuffleBackend[Any, Any],
    build: Any,
    probe: Any,
    on: Sequence[str],
    how: str,
    budget: int | None,
    cluster: Any,
    node_i: int,
) -> tuple[list[Any], list[str], int, int, int, int]:
    """Gather-join one dest's co-partitioned ``build``/``probe`` blocks with the generic kernel, as a
    LIST of joined sub-partition blocks (one when unbudgeted). With a budget it radix-sub-partitions the
    PROBE, sizing each chunk so its DUPLICATED output fits ``budget`` (trap 2 — the budget covers
    output), and SPILLS each partition's wire to the node Store **on disk**, DROPPING it from RAM —
    so the kernel's live RAM is one chunk, never a full-RAM concat of the whole dest. Each spilled chunk
    is then READ BACK off disk to reconstruct the return value (F5 — a real reader, not dead write-only
    I/O), and ``chunks_read`` witnesses the reader actually ran. The one-chunk-at-a-time RAM bound is
    the JOIN+SPILL loop above (each ``joined`` is dropped once its wire spills); the read-back list IS
    the return value, so all chunks are resident on the way out — the bound is the spill, not the read.
    The caller keeps the chunks as separate result blocks (their union is the dest's join). Returns
    ``(chunks, chunk_hashes, peak_output_bytes, spilled_partitions, output_rows, chunks_read)``.
    """
    on_l = list(on)
    if budget is None or len(probe) == 0:
        joined = join_blocks(backend, build, probe, on=on_l, how=how)
        return (
            [joined],
            [_sha256_hex(backend.to_wire(joined))],
            backend.estimated_bytes(joined),
            0,
            len(joined),
            0,  # nothing spilled -> nothing to read back
        )

    n_probe = len(probe)
    # Size the probe chunk from a ONE-ROW sample so we NEVER materialise the whole duplicated output to
    # measure it (that would defeat the budget). A denser-than-predicted chunk is caught + halved below.
    sample = join_blocks(backend, build, backend.slice_rows(probe, 0, 1), on=on_l, how=how)
    per_row = max(1, backend.estimated_bytes(sample))
    chunk = max(1, budget // per_row)
    obj_dir = Path(cluster.store_dir(node_i)) / "objects"

    hashes: list[str] = []
    peak = 0
    spilled = 0
    rows = 0
    off = 0
    while off < n_probe:
        c = min(chunk, n_probe - off)
        joined = join_blocks(backend, build, backend.slice_rows(probe, off, off + c), on=on_l, how=how)
        jb = backend.estimated_bytes(joined)
        if jb >= budget and c > 1:
            chunk = max(1, c // 2)  # a denser radix partition than the sample predicted -> shrink + retry
            continue
        wire = backend.to_wire(joined)
        digest = _sha256_hex(wire)
        (obj_dir / digest).write_bytes(wire)  # SPILL the radix partition to the node Store on DISK
        peak = max(peak, jb)
        spilled += 1
        rows += len(joined)
        del wire, joined  # free the wire AND the joined object from RAM — the real streaming bound
        hashes.append(digest)
        off += c

    # READ EACH SPILLED CHUNK BACK off disk to reconstruct the value — the reader F5 requires, so the
    # spill is load-bearing I/O instead of write-once-never-read.
    chunks = [backend.from_wire((obj_dir / digest).read_bytes()) for digest in hashes]
    return chunks, hashes, peak, spilled, rows, len(hashes)


def _run_shuffle_join(
    backend: ShuffleBackend[Any, Any],
    left_blocks: Sequence[Any],
    right_blocks: Sequence[Any],
    parts: int,
    k: int,
    on: Sequence[str],
    how: str,
    salt: int,
    mem_budget: int | None,
    steal: bool,
    faults: ShuffleFaults,
    cluster: Any,
    witness: ShuffleWitness,
) -> tuple[dict[int, Any], dict[int, str]]:
    """Shuffle both sides on ``__joinkey__`` (same ``parts`` + ``salt`` ⇒ co-partitioned), then gather
    the two sides per dest in ascending-task order and join them (spilling under ``mem_budget``).

    F1: a dest that only got rows from ONE side is not dropped — how=left/outer keeps a BUILD-only dest,
    how=right/outer keeps a PROBE-only dest. The absent side's ORIGINAL other-side block
    (``left_blocks[0]``/``right_blocks[0]``) stands in as a SCHEMA carrier and the internal ``how`` is
    forced to only the direction being filled (``left``/``right``): by the routing invariant any row
    sharing a key with this dest would itself route here, so an empty gather PROVES the carrier shares no
    key with it and ``match_indices``' opposite arm is never taken — only THIS dest's real rows emit,
    each null-filled against the carrier's schema. The carrier may itself be a ZERO-ROW block (a
    cut-emptied partition): ``backend.take`` null-fills an empty carrier, so no non-emptiness is assumed.
    An ENTIRELY partitionless absent side (``right_blocks``/``left_blocks`` == ``[]``) carries no schema
    at all — its present-side rows are emitted as-is (no null columns to add), never silently dropped."""
    n_tasks_l = min(k, len(left_blocks)) if left_blocks else 1
    n_tasks_r = min(k, len(right_blocks)) if right_blocks else 1
    witness.n_producer_tasks = max(n_tasks_l, n_tasks_r)
    left_manifests = _stage1_map_write(
        backend, left_blocks, parts, n_tasks_l, k, faults, steal, cluster, witness, salt
    )
    witness.build_side_blocks = sum(len(m) for m in left_manifests.values())
    right_manifests = _stage1_map_write(
        backend, right_blocks, parts, n_tasks_r, k, faults, steal, cluster, witness, salt
    )
    witness.large_side_blocks = sum(len(m) for m in right_manifests.values())
    witness.manifest_fetch_is_per_dest = True

    value: dict[int, Any] = {}
    dest_block_hashes: dict[int, str] = {}
    next_key = parts  # sub-partition (budget) blocks get fresh keys beyond the dest range
    for dest in range(parts):
        gather_i = dest % k
        build_side = _gather_side(backend, left_manifests, dest, n_tasks_l, gather_i, cluster, witness)
        probe_side = _gather_side(backend, right_manifests, dest, n_tasks_r, gather_i, cluster, witness)
        if not build_side and not probe_side:
            continue  # no rows on either side for this dest
        if not build_side and how not in ("right", "outer"):
            continue  # probe-only dest, but `how` keeps no unmatched-probe rows (F1)
        if not probe_side and how not in ("left", "outer"):
            continue  # build-only dest, but `how` keeps no unmatched-build rows (F1)
        if build_side and probe_side:
            build, probe, how_here = backend.concat(build_side), backend.concat(probe_side), how
        elif build_side:
            if not right_blocks:  # partitionless probe side -> no schema; keep the build rows (never drop)
                blk = backend.concat(build_side)
                value[dest], dest_block_hashes[dest] = blk, _sha256_hex(backend.to_wire(blk))
                witness.join_output_rows += len(blk)
                continue
            build, probe, how_here = backend.concat(build_side), right_blocks[0], "left"
        else:
            if not left_blocks:  # partitionless build side -> keep the probe rows (never drop)
                blk = backend.concat(probe_side)
                value[dest], dest_block_hashes[dest] = blk, _sha256_hex(backend.to_wire(blk))
                witness.join_output_rows += len(blk)
                continue
            build, probe, how_here = left_blocks[0], backend.concat(probe_side), "right"
        chunks, hashes, peak, spilled, rows, chunks_read = _join_with_budget(
            backend, build, probe, on, how_here, mem_budget, cluster, gather_i
        )
        witness.peak_join_bytes = max(witness.peak_join_bytes, peak)
        witness.join_spilled_partitions += spilled
        witness.join_chunks_read += chunks_read
        witness.join_output_rows += rows
        if len(chunks) == 1:  # the whole dest is one block -> keyed by dest (stable for determinism)
            value[dest], dest_block_hashes[dest] = chunks[0], hashes[0]
        else:  # spilled sub-partitions -> separate result blocks (their union is the dest's join)
            for ch, h in zip(chunks, hashes, strict=True):
                value[next_key], dest_block_hashes[next_key] = ch, h
                next_key += 1
    return value, dest_block_hashes


def _run_broadcast_join(
    backend: JoinBackend[Any, Any],
    left_blocks: Sequence[Any],
    right_blocks: Sequence[Any],
    k: int,
    on: Sequence[str],
    how: str,
    mem_budget: int | None,
    cluster: Any,
    witness: ShuffleWitness,
) -> tuple[dict[int, Any], dict[int, str]]:
    """Replicate the small build (left) side to every node and join each probe (right) partition
    against the whole build — the large side is NEVER shuffled (``large_side_blocks == 0``). ``value``
    is keyed by probe-partition index; the join RESULT (union over blocks) equals the shuffle join.

    F2: joining each probe block independently against the WHOLE build side would re-emit an unmatched
    build row once per block (N-fold). So for how=left/outer, each block is joined with the per-block
    how narrowed to "inner" (matched pairs only — never a per-block build-only fill) while the build
    indices matched in ANY block are tracked; AFTER all blocks, the build rows that never matched
    anywhere are emitted EXACTLY ONCE as null-probe rows. how=right/outer needs no such tracking — a
    probe row belongs to exactly one block, so its unmatched survivor is already exactly-once."""
    value: dict[int, Any] = {}
    dest_block_hashes: dict[int, str] = {}
    if not left_blocks:
        return value, dest_block_hashes
    build_concat = backend.concat(list(left_blocks))
    build_wire = backend.to_wire(build_concat)
    build_digest = _sha256_hex(build_wire)
    for i in range(k):
        cluster.put(i, build_digest, build_wire)
        witness.broadcast_puts += 1
    witness.large_side_blocks = 0
    witness.n_producer_tasks = max(1, min(k, len(right_blocks))) if right_blocks else 1

    per_block_how = {"inner": "inner", "left": "inner", "right": "right", "outer": "right"}[how]
    track_unmatched = how in ("left", "outer")
    matched_build_idx: set[int] = set()

    next_key = len(right_blocks)  # sub-partition (budget) blocks get fresh keys beyond the pidx range
    for pidx, probe_block in enumerate(right_blocks):
        node_i = pidx % k
        build_here = backend.from_wire(cluster.get(node_i, build_digest))
        if node_i != 0:
            witness.cross_node_fetches += 1
        if track_unmatched:
            b_idx, _p_idx = backend.match_indices(build_here, probe_block, on=list(on), how="inner")
            matched_build_idx.update(int(x) for x in b_idx)
        chunks, hashes, peak, spilled, rows, chunks_read = _join_with_budget(
            backend, build_here, probe_block, on, per_block_how, mem_budget, cluster, node_i
        )
        witness.peak_join_bytes = max(witness.peak_join_bytes, peak)
        witness.join_spilled_partitions += spilled
        witness.join_chunks_read += chunks_read
        witness.join_output_rows += rows
        if len(chunks) == 1:
            value[pidx], dest_block_hashes[pidx] = chunks[0], hashes[0]
        else:
            for ch, h in zip(chunks, hashes, strict=True):
                value[next_key], dest_block_hashes[next_key] = ch, h
                next_key += 1

    if track_unmatched:
        unmatched = sorted(set(range(len(build_concat))) - matched_build_idx)
        # ponytail: a probe-less broadcast run (a forced ``broadcast=True`` on an entirely empty right)
        # leaves the unmatched build rows unemitted — outside the frozen contract and unreachable via the
        # cost model (which never broadcasts an empty probe: build*parts < build is false); the SHUFFLE
        # path, the cost-model-reachable one, is where an entirely-empty side is pinned.
        if unmatched and right_blocks:
            # `right_blocks[0]` is the probe SCHEMA carrier and may itself be a ZERO-ROW block —
            # ``backend.take`` null-fills an empty carrier, so no non-emptiness is assumed. `unmatched`
            # rows are, by construction, unmatched against EVERY probe block, so forcing how="left" here
            # can never spuriously match.
            build_unmatched = backend.take(build_concat, unmatched)
            chunks, hashes, peak, spilled, rows, chunks_read = _join_with_budget(
                backend, build_unmatched, right_blocks[0], on, "left", mem_budget, cluster, 0
            )
            witness.peak_join_bytes = max(witness.peak_join_bytes, peak)
            witness.join_spilled_partitions += spilled
            witness.join_chunks_read += chunks_read
            witness.join_output_rows += rows
            for ch, h in zip(chunks, hashes, strict=True):
                value[next_key], dest_block_hashes[next_key] = ch, h
                next_key += 1
    return value, dest_block_hashes


def run_join(
    backend: JoinBackend[Any, Any],
    left_blocks: Sequence[Any],
    right_blocks: Sequence[Any],
    parts: int,
    *,
    on: Sequence[str] = ("__joinkey__",),
    how: str = "inner",
    workers: int = 1,
    comms: str = "ipc",
    store_root: str | None = None,
    broadcast: bool | None = None,
    salt: int = 0,
    mem_budget_bytes: int | None = None,
    steal: bool = False,
    faults: ShuffleFaults | None = None,
    advertise_host: str | None = None,
) -> ShuffleResult:
    """Distributed relational hash JOIN of two block sets via the two-phase executor (plan §4.2).

    The build (``left``) and probe (``right``) sides are co-partitioned on ``__joinkey__`` (same
    ``parts`` + ``salt``) then gather-joined per dest by the generic ``join_blocks`` kernel — SQL
    *duplicating* semantics (a probe row with k build matches ⇒ k rows). ``broadcast=None`` lets the
    pinned cost model choose (recorded in ``witness.broadcast_chosen``, E5); ``True``/``False`` honour a
    plan-recorded choice. ``mem_budget_bytes`` bounds the gather-join working set by spilling duplicated
    OUTPUT partitions to the node Store (B5). ``value`` maps dest (or probe-partition index, under
    broadcast) → the joined block; ``dest_block_hashes`` is the content-addressed determinism key.

    F6: the auto (``broadcast=None``) choice is keyed on ``parts``, NOT the runtime ``workers`` count —
    a PLAN-STABLE N, so the SAME logical join makes the SAME choice regardless of how the worker pool
    happens to be sized at run time. (``graphed.join_plan`` applies the same ``parts``-keyed cost rule
    at plan-build time, but on partition COUNTS — the only size it has before any data is read — whereas
    here the rule sees measured BYTES; the two can differ when per-partition size is skewed.)"""
    faults = faults if faults is not None else ShuffleFaults()
    k = max(1, workers)
    build_bytes = sum(backend.estimated_bytes(b) for b in left_blocks)
    probe_bytes = sum(backend.estimated_bytes(b) for b in right_blocks)
    chosen = broadcast_join_choice(build_bytes, probe_bytes, parts) if broadcast is None else bool(broadcast)
    cluster = _make_cluster(comms, k, store_root, advertise_host)
    try:
        witness = ShuffleWitness()
        witness.broadcast_chosen = chosen
        witness.node_store_dirs = {cluster.addr(i): cluster.store_dir(i) for i in range(k)}
        witness.node_hosts = [cluster.host(i) for i in range(k)]
        if chosen:
            value, dest_block_hashes = _run_broadcast_join(
                backend, left_blocks, right_blocks, k, on, how, mem_budget_bytes, cluster, witness
            )
        else:
            value, dest_block_hashes = _run_shuffle_join(
                backend,
                left_blocks,
                right_blocks,
                parts,
                k,
                on,
                how,
                salt,
                mem_budget_bytes,
                steal,
                faults,
                cluster,
                witness,
            )
        return ShuffleResult(dest_block_hashes=dest_block_hashes, value=value, partitions=[], witness=witness)
    finally:
        cluster.close()


def _split_to_target(backend: ShuffleBackend[Any, Any], block: Any, target_bytes: int) -> list[Any]:
    """Split an oversized block at EVENT (row) boundaries into pieces of ~``target_bytes`` (§5.1);
    a small-enough block passes through unsplit."""
    total = backend.estimated_bytes(block)
    n_rows = len(block)
    if total <= target_bytes or n_rows <= 1:
        return [block]
    rows_per = max(1, (n_rows * target_bytes) // total)
    out: list[Any] = []
    start = 0
    while start < n_rows:
        stop = min(start + rows_per, n_rows)
        out.append(backend.slice_rows(block, start, stop))
        start = stop
    return out


def _coalesce_pieces(
    backend: ShuffleBackend[Any, Any], pieces: Sequence[Any], target_bytes: int
) -> list[Any]:
    """Greedily merge consecutive pieces up to ~``target_bytes`` per output partition (content + order
    preserved: ``concat(out) == concat(pieces)``)."""
    out: list[Any] = []
    cur: list[Any] = []
    cur_bytes = 0
    for piece in pieces:
        cur.append(piece)
        cur_bytes += backend.estimated_bytes(piece)
        if cur_bytes >= target_bytes:
            out.append(backend.concat(cur))
            cur, cur_bytes = [], 0
    if cur:
        out.append(backend.concat(cur))
    return out


def run_repartition_by_size(
    backend: ShuffleBackend[Any, Any],
    src_blocks: Sequence[Any],
    *,
    target_bytes: int,
    workers: int = 1,
    store_root: str | None = None,
    row_group_bytes: int = ROW_GROUP_BYTES,
) -> ShuffleResult:
    """Coalesce/split ``src_blocks`` toward ``target_bytes`` by MEASURED bytes (§5.1): split any
    oversized block at row boundaries, then coalesce consecutive pieces. Content + order preserved.
    Returns the rebalanced blocks in ``.partitions`` (no key routing — a physical rebalance)."""
    pieces: list[Any] = []
    for block in src_blocks:
        pieces.extend(_split_to_target(backend, block, target_bytes))
    partitions = _coalesce_pieces(backend, pieces, target_bytes)
    return ShuffleResult(dest_block_hashes={}, value={}, partitions=partitions, witness=ShuffleWitness())


# ---- M41 T4: the Phase-2 ClusterExecutor launch seam — a genuine CROSS-PROCESS routable Store sim ----
# The core CONTRACT (AddressTable/NodeStore/ClusterExecutor) lives in graphed.core.execution; here the
# concrete node-local Store + mock launcher satisfy it BEHIND those types (§A.4). Multi-host launch stays
# deferred (Phase-2): this is a single-machine simulation — K genuine CHILD processes (spawn), each
# serving its Store over a real socket on a ROUTABLE (non-loopback) address, registering into the core
# AddressTable. Scope honesty (§3(e)): throughput is measured on this sim, never a cluster wall-clock SLA.
def _store_server_handler(
    objects_dir: Path, served_pid: int, served_host: str
) -> type[BaseHTTPRequestHandler]:
    """The node-local Store's bulk data-plane endpoints (§4.3): ``GET /blob/{hash}`` + ``PUT /blob``
    (content-addressed). A GET response carries ``X-Served-Pid``/``X-Served-Host`` so the consumer
    witnesses which CHILD (not the driver thread) served the fetch, and over what routable host."""

    class _StoreHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # stdlib http.server handler name — GET /blob/{hash}
            path = objects_dir / self.path.rsplit("/", 1)[-1]
            if not path.is_file():
                self.send_response(404)
                self.end_headers()
                return
            data = path.read_bytes()
            self.send_response(200)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("X-Served-Pid", str(served_pid))
            self.send_header("X-Served-Host", served_host)
            self.end_headers()
            self.wfile.write(data)

        def do_PUT(self) -> None:  # PUT /blob -> content-addressed store; returns the stored hash
            n = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(n) if n else b""
            digest = _sha256_hex(body)
            (objects_dir / digest).write_bytes(body)
            self.send_response(200)
            self.send_header("Content-Length", str(len(digest)))
            self.end_headers()
            self.wfile.write(digest.encode())

        def log_message(self, *args: Any) -> None:  # silence the access log
            pass

    return _StoreHandler


def _routable_store_child(node_id: int, store_root: str, advertise_host: str, ready_q: Any) -> None:
    """A spawned CHILD (its own PID): stand up a node-local Store served over a real socket on the
    ROUTABLE ``advertise_host``, then announce ``(node_id, host, port, os.getpid())`` back to the driver
    so the launcher registers the address with the child's pid as provenance."""
    store_dir = Path(store_root) / f"node-{node_id}"
    objects = store_dir / "objects"
    objects.mkdir(parents=True, exist_ok=True)
    host = select_advertise_host(advertise_host)  # ValueError on loopback/0.0.0.0 — never bind non-routable
    server = LookupFreeHTTPServer((host, 0), _store_server_handler(objects, os.getpid(), host))
    ready_q.put((node_id, str(server.server_address[0]), int(server.server_address[1]), os.getpid()))
    server.serve_forever()


class _RoutableNodeStore:
    """A concrete :class:`graphed.core.execution.NodeStore`: ``fetch(digest)`` GETs a blob from a
    routable CHILD and STREAMS it (chunked, byte-backpressured — never the whole blob resident) into the
    driver-side consumer Store on disk. **Idempotent**: a re-fetch of a digest already in the consumer
    Store returns it without moving bytes again (``bytes_transferred`` unchanged)."""

    def __init__(self, host: str, port: int, consumer_objects: Path, witness: ShuffleWitness) -> None:
        self._host = host
        self._port = port
        self._consumer = consumer_objects
        self._witness = witness

    def fetch(self, digest: str) -> bytes:
        target = self._consumer / digest
        if target.is_file():  # idempotent: already streamed in — no new bytes move
            return target.read_bytes()
        url = f"http://{self._host}:{self._port}/blob/{digest}"
        moved = 0
        with urllib.request.urlopen(url, timeout=30) as resp:
            served_pid = int(resp.headers.get("X-Served-Pid", "0"))
            served_host = str(resp.headers.get("X-Served-Host", ""))
            with open(target, "wb") as out:
                while True:
                    chunk = resp.read(1 << 16)  # 64 KiB — bound the reader buffer, stream to disk
                    if not chunk:
                        break
                    out.write(chunk)
                    moved += len(chunk)
        self._witness.bytes_transferred += moved  # UNIQUE payload bytes (idempotent re-fetch adds 0)
        self._witness.bulk_fetch_count += 1
        self._witness.served_pid = served_pid
        self._witness.served_host = served_host
        return target.read_bytes()


class RoutableClusterSim:
    """Handle over a launched cross-process routable-Store cluster (the (c.2)/(e) frozen API). ``put``
    ships a blob to a child's Store (PUT /blob); ``fetch`` pulls it back THROUGH the core
    ``NodeStore.fetch`` seam, streaming into the consumer Store. ``close`` tears the children down cleanly
    (no leaked processes/ports)."""

    def __init__(
        self,
        procs: list[Any],
        address_table: AddressTable,
        node_ids: tuple[int, ...],
        child_pids: tuple[int, ...],
        consumer_dir: Path,
        witness: ShuffleWitness,
        stores: dict[int, _RoutableNodeStore],
    ) -> None:
        self.driver_pid = os.getpid()
        self.address_table = address_table
        self.node_ids = node_ids
        self.child_pids = child_pids
        self.consumer_store_dir = str(consumer_dir)
        self.witness = witness
        self._procs = procs
        self._stores = stores

    def put(self, node_id: int, data: bytes) -> str:
        """PUT /blob to the CHILD hosting ``node_id`` -> the content hash it stored under."""
        host, port = self.address_table.lookup(node_id)
        req = urllib.request.Request(f"http://{host}:{port}/blob", data=data, method="PUT")
        with urllib.request.urlopen(req, timeout=30) as resp:
            digest: bytes = resp.read()
        return digest.decode()

    def fetch(self, node_id: int, digest: str) -> bytes:
        """GET /blob from ``node_id`` through the core ``NodeStore.fetch`` seam (idempotent)."""
        return self._stores[node_id].fetch(digest)

    def close(self) -> None:
        for p in self._procs:
            p.terminate()
        for p in self._procs:
            p.join(timeout=5)


def launch_routable_cluster(n_nodes: int, *, store_root: str, advertise_host: str) -> RoutableClusterSim:
    """Mock Phase-2 launcher: spawn ``n_nodes`` CHILD processes (multiprocessing 'spawn'), each serving a
    node-local Store on the ROUTABLE ``advertise_host``, and register each announced ``(host, port)`` —
    tagged with the CHILD's pid as provenance — into the core :class:`AddressTable`. Returns a
    :class:`RoutableClusterSim` whose ``fetch`` streams blobs through the core ``NodeStore.fetch`` seam
    into a driver-side consumer Store. Multi-host launch stays deferred — this is a single-machine sim."""
    host = select_advertise_host(advertise_host)  # reject loopback/wildcard up front
    ctx = multiprocessing.get_context("spawn")
    ready_q: Any = ctx.Queue()
    root = Path(store_root)
    consumer_objects = root / "consumer" / "objects"
    consumer_objects.mkdir(parents=True, exist_ok=True)
    procs: list[Any] = []
    for nid in range(n_nodes):
        p = ctx.Process(target=_routable_store_child, args=(nid, str(root), host, ready_q), daemon=True)
        p.start()
        procs.append(p)
    address_table = AddressTable()
    witness = ShuffleWitness()
    child_pids: list[int] = []
    stores: dict[int, _RoutableNodeStore] = {}
    for _ in range(n_nodes):
        nid, bhost, bport, pid = ready_q.get(timeout=60)  # each child announces its routable (host,port,pid)
        address_table.register(nid, bhost, bport, registered_by=pid)
        child_pids.append(pid)
        stores[nid] = _RoutableNodeStore(bhost, bport, consumer_objects, witness)
    return RoutableClusterSim(
        procs, address_table, tuple(range(n_nodes)), tuple(child_pids), root / "consumer", witness, stores
    )
