"""Run the H->gg translation at the LPC over the HiggsDNA 2024 sample manifests, its diagnostics on
histserv servers, and save them as UHI JSON.

From an LPC login node, with a proxy, in a venv that holds graphed-executors[htcondor],
graphed-histogram[histserv], the coffea fork and higgs_dna (``--env`` ships it to the jobs):

    python examples/hgg/run_lpc.py samples_2024_mc.json samples_2024_data.json --files 2 --parts 4 \\
        --pilots 8 --server-mb 512 2048 --env "$VIRTUAL_ENV" [--placement driver] [--driverless]

Each of ``--datasets`` takes its manifest's first ``--files`` files, each split into ``--parts`` entry
ranges. Parts go to ``--out`` (default ``root://cmseos.fnal.gov//store/user/<user>/hgg/``, which the
pilots write over xrootd with the job's proxy). ``--placement cluster`` runs each server as a job of its
own on the pool; ``driver`` runs them beside the driver on the login node. ``--driverless`` submits the
run as one job whose slot holds the driver, its local pilots and the servers. Service statuses (where
each server ran, when it was submitted and ready) are logged; the servers' sizes and predictions print
before the run.
"""

from __future__ import annotations

import argparse
import getpass
import json
import logging
import uuid
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from graphed_histogram import histserv

import analysis
from graphed_executors.htcondor_backend import SITES, htcondor_runner, submit_driverless
from run_local import fileset, report

DATASETS = ("GluGluHto2G_M-125_amcatnlo_2024", "DataC_2024")
IMAGE = "/cvmfs/unpacked.cern.ch/registry.hub.docker.com/coffeateam/coffea-almalinux9-noml:2026.9.0-py3.12"
#: the driver's own share of a driverless job's slot, beside its pilots and servers
DRIVER_MB = 2048
#: pickled plan parts name ``analysis.*``, so pilots need the file at the top of their scratch dir
ANALYSIS = Path(__file__).resolve().with_name("analysis.py")


def manifests(paths: Sequence[str]) -> dict[str, list[str]]:
    """``{dataset: [file, ...]}`` from HiggsDNA sample manifests, later files winning."""
    samples: dict[str, list[str]] = {}
    for path in paths:
        samples.update(json.loads(Path(path).read_text()))
    return samples


def steps(samples: dict[str, list[str]], datasets: Sequence[str], files: int, parts: int) -> dict[str, Any]:
    """coffea's fileset over each dataset's first ``files`` files, each in ``parts`` steps."""
    return {ds: fileset(samples[ds][:files], ds, parts)[ds] for ds in datasets}


class ServerTimes(logging.Formatter):
    """A record's line, and for a service status, when its server was submitted and when it was ready
    (``time.time()`` stamps)."""

    def format(self, record: logging.LogRecord) -> str:
        line = super().format(record)
        status = getattr(record, "status", None)
        if status is None:
            return line
        return f"{line} started_at={status.started_at} ready_at={status.ready_at}"


def driverless_memory_mb(ctx: histserv.Context, pilots: int, pilot_mb: int) -> int:
    """A driverless job's slot: the driver, its local pilots, and every server the context opened."""
    return DRIVER_MB + pilots * pilot_mb + sum(int(mb) for _name, mb, _predicted, _n in ctx.servers())


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("manifest", nargs="+", help="HiggsDNA sample manifests ({dataset: [file, ...]})")
    parser.add_argument("--datasets", nargs="+", default=list(DATASETS))
    parser.add_argument("--files", type=int, default=1, help="each dataset's first N files")
    parser.add_argument("--parts", type=int, default=1, help="entry ranges per file")
    parser.add_argument("--year", default="2024")
    parser.add_argument("--pilots", type=int, default=4)
    parser.add_argument("--pilot-mb", type=int, default=2048, help="each pilot's memory")
    parser.add_argument("--server-mb", type=int, nargs="+", default=[512], help="server sizes offered")
    parser.add_argument("--placement", choices=("cluster", "driver"), default="cluster")
    parser.add_argument("--driverless", action="store_true")
    parser.add_argument("--image", default=IMAGE)
    parser.add_argument("--env", default=None, help="a venv to ship to the jobs")
    parser.add_argument("--log-dir", default=None)
    parser.add_argument("--out", default=None, help="default: root://cmseos.fnal.gov//store/user/<user>/hgg/")
    parser.add_argument("--histograms", default="hgg_diagnostics.json", help="where the UHI JSON goes")
    args = parser.parse_args(argv)

    handler = logging.StreamHandler()
    handler.setFormatter(ServerTimes(logging.BASIC_FORMAT))
    logging.basicConfig(level=logging.INFO, handlers=[handler])
    out = args.out or f"root://cmseos.fnal.gov//store/user/{getpass.getuser()}/hgg/"
    ctx = histserv.Context(
        memory_mb=args.server_mb,
        workers=args.pilots,
        name=f"hgg-{uuid.uuid4().hex[:8]}",
        ports=SITES["lpc"].service_ports,
    )
    work = steps(manifests(args.manifest), args.datasets, args.files, args.parts)
    plan = analysis.plan(work, year=args.year, out=out, context=ctx)
    print("servers (name, memory_mb, predicted_bytes, n_histograms):", ctx.servers(), flush=True)
    if args.driverless:
        handle = submit_driverless(
            plan,
            site="lpc",
            image=args.image,
            n_pilots=args.pilots,
            pilots="local",
            request_memory_mb=driverless_memory_mb(ctx, args.pilots, args.pilot_mb),
            log_dir=args.log_dir,
            user_modules=[ANALYSIS],
            env=args.env,
        )
        print(f"driverless cluster {handle.cluster}: {handle.log_dir}", flush=True)
        print("status:", handle.wait(), flush=True)
        value = handle.result().value
    else:
        with htcondor_runner(
            n_pilots=args.pilots,
            site="lpc",
            image=args.image,
            request_memory_mb=args.pilot_mb,
            log_dir=args.log_dir,
            user_modules=[ANALYSIS],
            env=args.env,
            service_hosts=(args.placement,),
        ) as runner:
            value = runner.run(plan).value
    report(value, args.histograms)


if __name__ == "__main__":
    main()
