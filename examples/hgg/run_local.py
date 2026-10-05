"""Run the H->gg translation over NanoAOD files of one dataset on this machine, print its counters and
save its diagnostics as UHI JSON.

python examples/hgg/run_local.py FILE [FILE ...] --dataset MC --year 2024 --parts 4 --workers 4 --out output_inclusive
"""

from __future__ import annotations

import argparse
import itertools
import json
from collections.abc import Mapping
from typing import Any

import uhi.io.json
from coffea.nanoevents import NanoEventsFactory
from graphed.core import SequentialRunner

import analysis
from graphed_executors.submit import SubmitRunner, ThreadBackend


def num_entries(uri: str) -> int:
    """The file's entry count, from coffea's lazy (virtual) NanoEvents, which reads no branch for it."""
    return len(NanoEventsFactory.from_root({uri: "Events"}, mode="virtual").events())


def split(stop: int, parts: int) -> list[tuple[int, int]]:
    """``[0, stop)`` in ``parts`` contiguous ranges whose sizes differ by at most one."""
    edges = [stop * i // parts for i in range(parts + 1)]
    return list(itertools.pairwise(edges))


def fileset(uris: list[str], dataset: str, parts: int) -> dict[str, dict[str, Any]]:
    """coffea's ``{dataset: {file: {"object_path", "steps"}}}``, each file in ``parts`` steps."""
    return {
        dataset: {
            uri: {"object_path": "Events", "steps": [list(r) for r in split(num_entries(uri), parts)]}
            for uri in uris
        }
    }


def report(value: Mapping[str, Any], histograms: str) -> None:
    """Print each dataset's counters as JSON and save its diagnostics, ``{dataset: {name: histogram}}``,
    as UHI JSON at ``histograms``."""
    print(
        json.dumps(
            {ds: {k: x for k, x in v.items() if k != "diagnostics"} for ds, v in value.items()}, indent=1
        )
    )
    with open(histograms, "w") as f:
        json.dump({ds: v["diagnostics"] for ds, v in value.items()}, f, default=uhi.io.json.default)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("uri", nargs="+")
    parser.add_argument("--dataset", default="MC")
    parser.add_argument("--year", default="2024")
    parser.add_argument("--parts", type=int, default=1)
    parser.add_argument("--workers", type=int, default=1, help="1 runs in-process; more use a thread pool")
    parser.add_argument("--out", default="output_inclusive")
    parser.add_argument("--histograms", default="hgg_diagnostics.json", help="where the UHI JSON goes")
    args = parser.parse_args(argv)

    plan = analysis.plan(fileset(args.uri, args.dataset, args.parts), year=args.year, out=args.out)
    runner: Any = SequentialRunner() if args.workers == 1 else SubmitRunner(ThreadBackend(args.workers))
    try:
        value = runner.run(plan).value
    finally:
        getattr(runner, "close", lambda: None)()
    report(value, args.histograms)


if __name__ == "__main__":
    main()
