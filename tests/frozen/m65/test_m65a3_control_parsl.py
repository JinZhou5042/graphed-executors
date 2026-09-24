"""m65 A3: ``ParslBackend.task_slots()`` never waits, so a controlled run survives an HTEX block that arrives late."""

from __future__ import annotations

import functools
import os
import sys
import threading
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("parsl")

import m65a_probe as mp
from graphed.core import RunControl

from graphed_executors.parsl_backend import ParslBackend, stop_htex
from graphed_executors.submit import SubmitRunner

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="parsl HTEX requires POSIX")

HERE = str(Path(__file__).resolve().parent)


@pytest.fixture(autouse=True)
def _worker_env(monkeypatch: pytest.MonkeyPatch) -> None:
    # HTEX starts its interchange and workers by script name, and workers do not inherit sys.path.
    monkeypatch.setenv("PATH", os.path.dirname(sys.executable) + os.pathsep + os.environ.get("PATH", ""))
    monkeypatch.setenv("PYTHONPATH", HERE + os.pathsep + os.environ.get("PYTHONPATH", ""))


@contextmanager
def _htex_without_blocks(run_dir: Path) -> Iterator[Any]:
    from parsl.executors import HighThroughputExecutor  # noqa: PLC0415
    from parsl.providers import LocalProvider  # noqa: PLC0415

    ex = HighThroughputExecutor(
        label=f"m65a3-{uuid.uuid4().hex[:8]}",
        max_workers_per_node=2,
        encrypted=False,
        provider=LocalProvider(init_blocks=0, min_blocks=0, max_blocks=1),
    )
    ex.run_dir = str(run_dir)
    ex.provider.script_dir = str(run_dir / "submit_scripts")
    os.makedirs(ex.provider.script_dir, exist_ok=True)
    ex.start()
    try:
        yield ex
    finally:
        stop_htex(ex)


class _NoWorkerCount(ParslBackend):
    def n_workers(self) -> int:
        raise AssertionError("a controlled run's window must read task_slots(), not n_workers()")


def test_parsl_task_slots_never_wait(tmp_path: Path) -> None:
    from parsl.executors import ThreadPoolExecutor  # noqa: PLC0415

    tpe = ThreadPoolExecutor(label=f"m65a3-tpe-{uuid.uuid4().hex[:8]}", max_threads=2)
    tpe.start()
    try:
        assert ParslBackend(tpe).task_slots() == 2
    finally:
        tpe.shutdown()

    with _htex_without_blocks(tmp_path / "idle") as htex:
        t0 = time.perf_counter()
        assert ParslBackend(htex).task_slots() == 0
        assert time.perf_counter() - t0 < 1.0

    for path, adaptive in (("fixed", False), ("adaptive", True)):
        with _htex_without_blocks(tmp_path / path) as htex:
            ex = SubmitRunner(_NoWorkerCount(htex))
            ex.control = RunControl()
            run = functools.partial(ex.run, mp.make_plan(mp.Probe(n=8), adaptive=adaptive))
            timer = threading.Timer(3.0, htex.scale_out_facade, args=(1,))
            timer.start()
            try:
                res = mp.Background(run).result(60)
            finally:
                timer.cancel()
            assert res.value == (1,) * 8, path
