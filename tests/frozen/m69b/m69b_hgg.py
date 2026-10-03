"""m69b H->gg harness: the diagnostics' definitions, the varied-weight MC fixture and the direct fills.

Importing it skips the importing file without coffea, higgs_dna (through ``hgg_harness``) or histserv,
unless ``GRAPHED_HGG_REQUIRED=1``, where a missing one fails instead. ``hgg_harness`` puts
``examples/hgg`` on ``sys.path``.
"""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

if os.environ.get("GRAPHED_HGG_REQUIRED") != "1":
    pytest.importorskip("histserv")

import boost_histogram as bh
import hgg_harness as h
import numpy as np
import pyarrow.parquet as pq
from coffea.processor import accumulate

RANGES = [(0, 100), (100, 200)]
#: name -> (the oracle part's column, Regular(bins, low, high)); Weight storage, weight = ``weight``
DIAGNOSTICS: dict[str, tuple[str, tuple[int, float, float]]] = {
    "m_gg": ("mass", (80, 100.0, 180.0)),
    "pt_gg": ("pt", (50, 0.0, 250.0)),
    "lead_pt": ("lead_pt", (50, 0.0, 200.0)),
    "sublead_pt": ("sublead_pt", (50, 0.0, 200.0)),
    "lead_eta": ("lead_eta", (50, -2.5, 2.5)),
    "sublead_eta": ("sublead_eta", (50, -2.5, 2.5)),
    "n_jets": ("n_jets", (8, -0.5, 7.5)),
}
SERVER_MB = 512
VARIED_SEED = 7

# the MC fixture with lognormal genWeight magnitudes and random signs (float32), written by the m69a
# builder in a child process (its write() pins datetime.datetime for the process that runs it)
_VARY = """
import sys, numpy as np, awkward as ak
sys.path.insert(0, sys.argv[1])
import make_hgg_fixture as mk
out, _ = mk.read_collections(sys.argv[2])
rng = np.random.default_rng(int(sys.argv[4]))
n = len(out["genWeight"])
out["genWeight"] = ak.values_astype(rng.lognormal(0, 2, n) * rng.choice([-1, 1], n), np.float32)
mk.write(sys.argv[3], out)
"""


def build_varied_mc(dest_dir: Path) -> Path:
    dst = dest_dir / "nano_hgg_v15_varw.root"
    argv = [sys.executable, "-c", _VARY, str(h.DATA), str(h.MC_FIXTURE), str(dst), str(VARIED_SEED)]
    subprocess.run(argv, check=True, timeout=300)
    return dst


def fileset(fixtures: Mapping[str, Path]) -> dict[str, dict[str, Any]]:
    """coffea's ``{dataset: {file: {"object_path", "steps"}}}`` at RANGES."""
    steps = [list(r) for r in RANGES]
    return {ds: {str(uri): {"object_path": "Events", "steps": steps}} for ds, uri in fixtures.items()}


def part_path(out: Path, dataset: str, uri: Path, start: int, stop: int) -> Path:
    return out / dataset / "nominal" / f"{uri.stem}_Events_{start}-{stop}.parquet"


def direct(parts: Mapping[tuple[int, int], h.Part]) -> dict[str, bh.Histogram]:
    """Each diagnostic as boost-histogram fills the oracle parts' columns, one histogram per part,
    added onto an empty one in part order."""
    out = {}
    for name, (column, (bins, low, high)) in DIAGNOSTICS.items():
        total = bh.Histogram(bh.axis.Regular(bins, low, high), storage=bh.storage.Weight())
        for key in sorted(parts):
            table = parts[key][1]
            part = bh.Histogram(bh.axis.Regular(bins, low, high), storage=bh.storage.Weight())
            part.fill(table.column(column).to_numpy(), weight=table.column("weight").to_numpy())
            total = total + part
        out[name] = total
    return out


def oracle_totals(oracle: Mapping[str, Mapping[tuple[int, int], h.Part]]) -> dict[str, dict[str, Any]]:
    """The oracle's counters summed per dataset, as coffea's Runner accumulates them."""
    result: dict[str, dict[str, Any]] = accumulate(
        [c for parts in oracle.values() for c, _ in parts.values()]
    )
    return result


def read_part(path: Path) -> Any:
    return pq.read_table(path)


def totals(value: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    """A run's value without each dataset's ``"diagnostics"``."""
    return {ds: {k: v for k, v in leaves.items() if k != "diagnostics"} for ds, leaves in value.items()}


def edges(hist: bh.Histogram) -> list[bytes]:
    return [np.asarray(axis.edges).tobytes() for axis in hist.axes]


def bitwise(got: Any, want: bh.Histogram) -> bool:
    """Same storage, edges and flow view, bit for bit."""
    return (
        isinstance(got, bh.Histogram)
        and got.storage_type is want.storage_type
        and edges(got) == edges(want)
        and np.asarray(got.view(flow=True)).tobytes() == np.asarray(want.view(flow=True)).tobytes()
    )


def close(got: Any, want: bh.Histogram, rtol: float = 1e-12) -> bool:
    """Same storage and edges; flow values and variances within ``rtol`` relative (no absolute slack)."""
    if not (isinstance(got, bh.Histogram) and got.storage_type is want.storage_type):
        return False
    g, w = got.view(flow=True), want.view(flow=True)
    return edges(got) == edges(want) and all(
        np.allclose(g[field], w[field], rtol=rtol, atol=0.0) for field in ("value", "variance")
    )
