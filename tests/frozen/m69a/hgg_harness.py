"""m69a harness: the owner's original processor as the oracle, and the part comparator.

``oracle_parts`` runs ``data/inclusive_processor.py`` (byte-identical to the owner's file) the way its
``__main__`` does; ``compare_part`` is the bit-for-bit judge of a graphed part against an oracle part.
"""

from __future__ import annotations

import gzip
import hashlib
import importlib.util
import json
import os
import sys
from collections.abc import Iterable
from importlib import resources
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

if os.environ.get("GRAPHED_HGG_REQUIRED") != "1":
    pytest.importorskip("coffea")
    pytest.importorskip("higgs_dna")

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from coffea.nanoevents import NanoEventsFactory

DATA = Path(__file__).resolve().with_name("data")
# the analysis is a user module on sys.path, not package code
EXAMPLE = Path(__file__).resolve().parents[3] / "examples" / "hgg"
sys.path.insert(0, str(EXAMPLE))
ORIGINAL = DATA / "inclusive_processor.py"
ORIGINAL_SHA256 = "791229c2a091e7f0708f885c5fe245a702567ead37c9ab6d850d7d41ce52d1dd"
MC_FIXTURE = DATA / "nano_hgg_v15.root"
DATA_FIXTURE = DATA / "nano_hgg_v15_data.root"
YEAR = "2024"

# data/ file -> the path pull_files.py places it at, inside the installed higgs_dna package
PLACED = {
    "Cert_Collisions2024_378981_386951_Golden.json.gz": (
        "metaconditions/CAF/certification/Collisions24/Cert_Collisions2024_378981_386951_Golden.json"
    ),
    "jetid.json.gz": "systematics/JSONs/POG/JME/2024_Summer24/jetid.json.gz",
}

Counters = dict[str, dict[str, Any]]
Part = tuple[Counters, pa.Table]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def payload(name: str) -> bytes:
    """The bytes data/``name`` places: decompressed when its target is not itself gzipped."""
    raw = (DATA / name).read_bytes()
    return raw if PLACED[name].endswith(".gz") else gzip.decompress(raw)


def place_higgs_dna_data() -> dict[str, Path]:
    """Write data/'s golden and jet-ID JSONs to pull_files.py's targets (iff absent); returns name -> target."""
    root = Path(str(resources.files("higgs_dna")))
    placed = {}
    for name, rel in PLACED.items():
        body, dst = payload(name), root / rel
        if not dst.exists():
            dst.parent.mkdir(parents=True, exist_ok=True)
            dst.write_bytes(body)
        elif dst.read_bytes() != body:
            raise AssertionError(f"{dst} already exists with a different sha256 than data/{name}")
        placed[name] = dst
    return placed


def original() -> ModuleType:
    """The owner's ``inclusive_processor.py`` imported as a module (once per process)."""
    name = "inclusive_processor"
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, ORIGINAL)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    return sys.modules[name]


def metaconditions() -> dict[str, Any]:
    text = resources.files("higgs_dna.metaconditions").joinpath("Era2022_v1.json").read_text()
    loaded: dict[str, Any] = json.loads(text)
    return loaded


def oracle_parts(
    uri: str, dataset: str, year: str, ranges: Iterable[tuple[int, int]], out: str | Path
) -> dict[tuple[int, int], Part]:
    """Per range, the original's ``process()`` on NanoEvents read as its ``__main__`` reads them."""
    parts: dict[tuple[int, int], Part] = {}
    for start, stop in ranges:
        dest = Path(out) / f"{start}-{stop}"
        proc = original().HggInclusiveProcessor(
            metaconditions=metaconditions(), output_location=str(dest), year={dataset: [year]}
        )
        events = NanoEventsFactory.from_root(
            {uri: "Events"},
            entry_start=start,
            entry_stop=stop,
            metadata={"dataset": dataset, "filename": uri.split("/")[-1]},
        ).events()
        counters = proc.process(events)
        (path,) = dest.rglob("*.parquet")
        parts[(start, stop)] = (counters, pq.read_table(path))
    return parts


def _leaves(counters: Counters) -> dict[tuple[str, str], Any]:
    return {(d, k): v for d, inner in counters.items() for k, v in inner.items()}


def _bits(column: pa.ChunkedArray | pa.Array) -> bytes:
    """The valid values' bytes; floats go through their same-width unsigned view, so NaNs compare."""
    arr = column.combine_chunks() if isinstance(column, pa.ChunkedArray) else column
    values = np.asarray(arr.drop_null().to_numpy(zero_copy_only=False))
    if values.dtype.kind == "f":
        values = values.view(f"u{values.dtype.itemsize}")
    return values.tobytes()


def compare_part(expected: Part, actual: Part) -> list[str]:
    """Every difference between two parts, each entry prefixed by its leg; ``[]`` means identical.

    Legs: ``counters`` (``==`` and each value's Python type), ``schema`` (names, order, types,
    nullability), ``validity <col>`` (the validity bitmap), ``values <col>`` (valid values bit-for-bit)
    and ``metadata`` (the schema's key-value metadata).
    """
    (ce, te), (ca, ta) = expected, actual
    diffs = []
    le, la = _leaves(ce), _leaves(ca)
    if ce != ca:
        diffs.append(f"counters: expected {ce!r}, got {ca!r}")
    elif any(type(le[k]) is not type(la[k]) for k in le):
        diffs.append(
            f"counters: types {[type(v).__name__ for v in le.values()]} != {[type(v).__name__ for v in la.values()]}"
        )
    se, sa = te.schema.remove_metadata(), ta.schema.remove_metadata()
    if not se.equals(sa):
        fields = [f"{f} != {g}" for f, g in zip(se, sa, strict=False) if not f.equals(g)]
        diffs.append(f"schema: {len(se)} vs {len(sa)} fields; {'; '.join(fields)}")
    for name in te.column_names:
        if name not in ta.column_names:
            continue
        x, y = te.column(name), ta.column(name)
        if not pc.is_valid(x).equals(pc.is_valid(y)):
            diffs.append(f"validity {name}: {x.null_count}/{len(x)} null vs {y.null_count}/{len(y)} null")
        if _bits(x) != _bits(y):
            diffs.append(f"values {name}")
    if (te.schema.metadata or {}) != (ta.schema.metadata or {}):
        diffs.append(f"metadata: expected {te.schema.metadata!r}, got {ta.schema.metadata!r}")
    return diffs
