"""m65 B: ``dask_runner`` workers push their task events through the monitor's per-worker factory,
one connection per worker process."""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("distributed")

import distributed
import m65a_probe as mp
import m65b_probe as bp
from graphed.core import TaskPhase

from graphed_executors.dask_backend import dask_runner


@pytest.fixture(scope="module")
def client() -> Iterator[Any]:
    with (
        distributed.LocalCluster(
            n_workers=2, threads_per_worker=2, processes=True, dashboard_address=":0"
        ) as cluster,
        distributed.Client(cluster) as c,
    ):
        yield c


def test_dask_workers_push_one_connection_per_worker(client: Any, tmp_path: Path) -> None:
    driver = bp.PushRecorder(str(tmp_path), per_worker=True)
    res = dask_runner(client, monitor=driver).run(mp.make_plan(mp.Probe()))
    assert res.value == (1,) * mp.N
    driver_name = os.path.basename(driver.path)
    conns = bp.connections(str(tmp_path))
    workers = [c for c, name in zip(conns, sorted(os.listdir(tmp_path)), strict=True) if name != driver_name]
    names = frozenset().union(*(w for _, w, _ in workers))
    assert len(names) >= 1
    assert len(conns) == 1 + len(names)
    assert os.getpid() not in {pid for pid, _, _ in workers}
    assert sorted(e["key"] for _, _, evs in workers for e in evs if e["phase"] == "finished") == list(
        range(mp.N)
    )
    assert not [e for e in driver.events if e.phase is TaskPhase.FINISHED]
