"""Compare the graphed translation with the original, part for part, on real 2024 NanoAOD.

For the first file of ``GluGluHto2G_M-125_amcatnlo_2024`` and of ``DataC_2024`` (the HiggsDNA 2024
sample manifests) it prints the file's entry count, splits ``[0, --entry-stop)`` into ``--parts``
ranges, runs the original processor (the m69a oracle) on those ranges and ONE graphed plan over both
files, and prints each part's ``compare_part`` result and each dataset's accumulated counters beside
the plan's. Exits 1 on any difference.

    python examples/hgg/validate_real.py --parts 2 [--entry-stop N] [--out DIR]
"""

from __future__ import annotations

import argparse
import sys
import tempfile
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
from coffea.processor import accumulate
from graphed.core import SequentialRunner

import analysis
from run_local import num_entries, split

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tests" / "frozen" / "m69a"))
import hgg_harness as h  # the frozen harness: the original as oracle, and the comparator

STORE = "root://cms-xrd-global.cern.ch//store"
MC = (
    f"{STORE}/mc/RunIII2024Summer24NanoAODv15/GluGluH-Hto2G_Par-M-125_TuneCP5_13p6TeV_amcatnloFXFX-pythia8/"
    "NANOAODSIM/150X_mcRun3_2024_realistic_v2-v2/120000/acebfb52-a25b-48bc-b9f6-80fe54a98d56.root"
)
DATA = f"{STORE}/data/Run2024C/EGamma0/NANOAOD/MINIv6NANOv15-v1/2540000/3151ab41-a5bd-48f1-be40-449b760a60e6.root"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--parts", type=int, default=2)
    parser.add_argument("--entry-stop", type=int, default=None, help="default: the whole file")
    parser.add_argument("--out", default=None, help="default: a temporary directory")
    parser.add_argument("--mc", default=MC, help="the MC file (default: the manifest's first)")
    parser.add_argument("--data", default=DATA, help="the data file (default: the manifest's first)")
    args = parser.parse_args(argv)

    h.place_higgs_dna_data()
    out = Path(args.out or tempfile.mkdtemp(prefix="hgg-validate-"))
    files = {"GluGluHto2G_M-125_amcatnlo_2024": args.mc, "DataC_2024": args.data}
    fileset: dict[str, dict[str, Any]] = {}
    oracle: dict[str, dict[tuple[int, int], h.Part]] = {}
    for dataset, uri in files.items():
        n = num_entries(uri)
        stop = n if args.entry_stop is None else min(args.entry_stop, n)
        ranges = split(stop, args.parts)
        print(f"FILE {dataset} {uri} num_entries={n} ranges={ranges}", flush=True)
        oracle[dataset] = h.oracle_parts(uri, dataset, h.YEAR, ranges, out / "oracle" / dataset)
        fileset[dataset] = {uri: {"object_path": "Events", "steps": [list(r) for r in ranges]}}
    value = SequentialRunner().run(analysis.plan(fileset, year=h.YEAR, out=str(out / "graphed"))).value
    differences = 0
    for dataset, uri in files.items():
        for (start, stop_), (counters, table) in oracle[dataset].items():
            path = out / "graphed" / dataset / "nominal" / f"{Path(uri).stem}_Events_{start}-{stop_}.parquet"
            # the counters leg is the dataset totals below; each part is judged on its table
            diffs = h.compare_part((counters, table), (counters, pq.read_table(path)))
            differences += len(diffs)
            print(f"PART {dataset} {start}-{stop_} rows={table.num_rows} cols={table.num_columns}")
            print(f"  compare_part {diffs or 'IDENTICAL'}", flush=True)
        expected = accumulate(counters for counters, _ in oracle[dataset].values())
        differences += expected != {dataset: value[dataset]}
        print(
            f"TOTALS {dataset}\n  original {expected}\n  graphed  { ({dataset: value[dataset]}) }", flush=True
        )
    print(f"DIFFERENCES {differences}")
    return 1 if differences else 0


if __name__ == "__main__":
    sys.exit(main())
