"""``SubmitRunner(ParslBackend(...), retries=n)`` reruns a failed task up to ``n`` times (GW21)."""

from __future__ import annotations

import operator
import os
import shutil
import tempfile
import threading
import time
import uuid
from collections.abc import Iterator
from concurrent.futures import CancelledError
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("parsl")

from _hdna_payloads import FailFirst
from graphed.core import Partition
from graphed.core.execution import Plan, Task

from graphed_executors.parsl_backend import ParslBackend
from graphed_executors.parsl_backend.launch import start_htex, stop_htex
from graphed_executors.submit import SubmitRunner

HERE = str(Path(__file__).resolve().parent)
N_TASKS = 3


@pytest.fixture(scope="module")
def tpe() -> Iterator[Any]:
    from parsl.executors import ThreadPoolExecutor  # noqa: PLC0415

    executor = ThreadPoolExecutor(label=f"gw21-tpe-{uuid.uuid4().hex[:8]}", max_threads=2)
    executor.start()
    yield executor
    executor.shutdown()


@pytest.fixture(scope="module", params=["tpe", "htex"])
def executor(request: pytest.FixtureRequest) -> Iterator[Any]:
    if request.param == "tpe":
        yield request.getfixturevalue("tpe")
        return
    os.environ["PYTHONPATH"] = HERE + os.pathsep + os.environ.get("PYTHONPATH", "")
    run_dir = tempfile.mkdtemp(prefix="gw21-htex-")
    htex = start_htex(workers=2, run_dir=run_dir)
    yield htex
    stop_htex(htex)
    shutil.rmtree(run_dir, ignore_errors=True)


def _plan(marks: Path, failures: int = 1) -> Plan[int]:
    tasks = tuple(Task(i, Partition.blind(f"mem://p{i}", "", 0, 1)) for i in range(N_TASKS))
    return Plan(process=FailFirst(str(marks), failures), combine=operator.add, empty=int, tasks=tasks)


def test_a_task_that_fails_once_completes_with_one_retry(executor: Any, tmp_path: Path) -> None:
    value = SubmitRunner(ParslBackend(executor), retries=1).run(_plan(tmp_path)).value
    assert value == N_TASKS
    assert sorted(os.listdir(tmp_path)) == [f"p{i}.{k}" for i in range(N_TASKS) for k in range(2)]


@pytest.mark.parametrize(("retries", "failures"), [(0, 1), (1, 2)])
def test_a_task_failing_more_often_than_its_retries_fails_the_run(
    executor: Any, tmp_path: Path, retries: int, failures: int
) -> None:
    with pytest.raises(RuntimeError, match=f"attempt {retries} at mem://p"):
        SubmitRunner(ParslBackend(executor), retries=retries).run(_plan(tmp_path, failures))


def test_a_cancel_reaches_the_queued_attempt_on_tpe(tpe: Any, tmp_path: Path) -> None:
    backend = ParslBackend(tpe)
    gate = threading.Event()
    busy = [backend.submit(gate.wait, 30, key=f"busy-{i}") for i in range(2)]
    queued = backend.submit(
        FailFirst(str(tmp_path)), Partition.blind("mem://q", "", 0, 1), None, key="q", retries=2
    )
    backend.cancel([queued])
    gate.set()
    for fut in busy:
        fut.result(timeout=30)
    barrier = threading.Barrier(2)  # both threads reach it only after anything queued before it
    for fut in [backend.submit(barrier.wait, 30, key=f"after-{i}") for i in range(2)]:
        fut.result(timeout=30)
    with pytest.raises(CancelledError):
        queued.result(timeout=0)
    assert os.listdir(tmp_path) == []  # the attempt never ran


def test_a_resubmit_the_executor_refuses_settles_with_its_error() -> None:
    run_dir = tempfile.mkdtemp(prefix="gw21-bad-")
    htex = start_htex(workers=1, run_dir=run_dir)
    try:
        fut = ParslBackend(htex).submit(time.sleep, 3, key="k", retries=2)
        boom = RuntimeError("all blocks failed")
        htex.set_bad_state_and_fail_all(boom)
        assert fut.exception(timeout=15) is boom
    finally:
        stop_htex(htex)
        shutil.rmtree(run_dir, ignore_errors=True)
