"""m65 A3: ``dask_runner`` honours a ``RunControl`` per A-2, and its window is the cluster's task slots."""

from __future__ import annotations

import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("distributed")

import distributed
import m65a3_bodies as b
import m65a_probe as mp
from graphed.core import RunControl

from graphed_executors.dask_backend import dask_runner

PATHS = {"fixed": False, "adaptive": True}


@pytest.fixture(scope="module")
def client() -> Iterator[Any]:
    with (
        distributed.LocalCluster(
            n_workers=2, threads_per_worker=2, processes=True, dashboard_address=":0"
        ) as cluster,
        distributed.Client(cluster) as c,
    ):
        yield c


def _leg(client: Any, path: str) -> b.Leg:
    def factory(monitor: Any = None, **extra: Any) -> Any:
        return dask_runner(client, monitor=monitor, **extra)

    return b.Leg(factory, adaptive=PATHS[path])


@pytest.mark.parametrize("path", PATHS)
def test_pause_stops_dispatch_then_resume_completes(client: Any, path: str) -> None:
    b.pause_stops_dispatch_then_resume_completes(_leg(client, path))


@pytest.mark.parametrize("path", PATHS)
def test_cancel_drains_and_folds_exactly_the_completed_tasks(client: Any, path: str) -> None:
    b.cancel_drains_and_folds_exactly_the_completed_tasks(_leg(client, path))


@pytest.mark.parametrize("path", PATHS)
def test_failure_during_cancel_drain_raises(client: Any, path: str, tmp_path: Path) -> None:
    b.failure_during_cancel_drain_raises(_leg(client, path), tmp_path)


@pytest.mark.parametrize("empty_plan", [False, True], ids=["40-tasks", "no-tasks"])
@pytest.mark.parametrize("path", PATHS)
def test_cancelled_control_does_no_work(client: Any, path: str, empty_plan: bool) -> None:
    b.cancelled_control_does_no_work(_leg(client, path), empty_plan)


def test_stop_condition_does_not_end_a_cancel_drain(client: Any, tmp_path: Path) -> None:
    b.stop_condition_does_not_end_a_cancel_drain(_leg(client, "adaptive"), tmp_path)


@pytest.mark.parametrize("path", PATHS)
def test_window_is_the_dask_task_slots(client: Any, path: str) -> None:
    rec = mp.Recorder()
    ex = dask_runner(client, monitor=rec, replicate_broadcast=True)
    ex.control = RunControl()
    res = mp.Background(
        lambda: ex.run(mp.make_plan(mp.Probe(n=8, sleep_s=0.3), adaptive=PATHS[path]))
    ).result()
    assert res.value == (1,) * 8
    assert b.started_before_first_finished(rec) == 4


@pytest.mark.parametrize("path", PATHS)
def test_scale_from_zero_completes(path: str) -> None:
    with (
        distributed.LocalCluster(
            n_workers=0, threads_per_worker=2, processes=True, dashboard_address=":0"
        ) as cluster,
        distributed.Client(cluster) as c,
    ):
        ex = dask_runner(c)
        ex.control = RunControl()
        timer = threading.Timer(2.0, cluster.scale, args=(2,))
        timer.start()
        try:
            res = mp.Background(lambda: ex.run(mp.make_plan(mp.Probe(n=8), adaptive=PATHS[path]))).result(60)
        finally:
            timer.cancel()
        assert res.value == (1,) * 8
