# CLAUDE.md — graphed-executors

Defers to the root **`graphed-project/CLAUDE.md`**; the **project plan
(`graphed-project-plan-gated.md`) always wins.** This file distills **milestone M7**.

## What this repo is

`graphed-executors`: pluggable executor backends; the reference single-machine one is `graphed_executors.local`. The execution *contract* lives in `graphed-core`
(`graphed_core.execution`: `Plan`, `Task`, `Partition`, `StopCondition`, `Executor`); this repo
implements it for a single machine, **two ways**: a thread pool and a process pool.

> Guardrails (M7): single machine only (no cluster — Phase 2); the published contract is **provisional
> until exercised by a real adapter**; executors MUST return remote `StageError`s intact (picklable,
> per M6), never an opaque string.
>
> **M39 amendment (§6.5, cluster-correct seam):** the M7 "single machine only" line is narrowed to the
> *launch/scheduling* layer. M39 ships a **cluster-correct data-plane seam** here: a routable
> `HttpTransport` (`_transport.is_routable_host` / `select_advertise_host` reject loopback/`0.0.0.0`),
> node-local content-addressed block Stores with per-hash `fetch`, the two-phase map-write + gather
> executor (`shuffle.py`), the reliable manifest push, and a **cross-address cluster-sim** over that
> routable transport (`comms="http"`: each node its own Store on a dialable IP). The seam is designed
> and tested to be cluster-correct; **real multi-host launch (a batch/HTCondor/TaskVine adapter that
> spans machines) stays Phase 2.** The `comms="http"` cluster-sim runs the nodes in-process over real
> routable sockets (a single-machine simulation of the transport, R0.10a-clean); spawning genuinely
> separate node processes/hosts is the M41 hardening item.

## Implemented (M7)

- `ThreadExecutor` + `ProcessExecutor` (`executors.py`) — same driver, different pool. Per-worker
  `open_once` resources are thread-local (threads) / a per-process global via an initializer
  (processes, spawn context: cross-platform + free-threaded-safe).
- **Deterministic, straggler-tolerant tree reduction** (`_reduce.py`): `plan_tree` builds a fixed
  binary combine-tree by leaf index; `tree_reduce` fires each combine as soon as both inputs are
  ready (a slow leaf blocks only its path to the root). Fixed grouping ⇒ bit-for-bit results.
- Adaptive reshaping: an `next_tasks(context)` plan pulls partitions sized from observed timings; a
  `running_fold` reduces the discovered set. Stopping conditions (target events / wall-clock / error
  budget) end submission early.
- `open_once` file-locality (`resources.py`): a uri is opened at most once per worker.
- Error propagation: a worker failure re-raises in the driver intact; a `StageError` round-trips and
  renders via M6's `format_traceback`.

## Validated against the corpus

The AGC ttbar slice + dimuon + ADL analyses run end-to-end over partitions via BOTH executors and
reproduce the corpus reference histograms **bit-for-bit**, invariant to `opt_level` and projection.

## Dependencies / gates

Runtime: `graphed-core` (contract) + `graphed-debug` (StageError) + `graphed` (Session). Tests use
`graphed-numpy` / `graphed-awkward` / `graphed-corpus`. Gates: `ruff` + `ruff format` · `mypy
--strict` · `pytest tests/frozen tests/extra --cov=graphed_executors` per-file >=90% line+branch
(`scripts/coverage_gate.py`, never lowered) + PR diff-coverage >=98% (`diff-cover` vs `main`) ·
`sphinx -W`.

Status: see `.graphed/state.json`.
