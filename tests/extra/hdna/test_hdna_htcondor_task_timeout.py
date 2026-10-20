"""``task_timeout_s``: a task leased past the limit fails with a ``StageError`` naming its partition, caused
by ``TaskTimedOut`` naming its key, pilot and limit; its pilot stops, its late result is dropped, and a resumable rerun runs only that task (GW21)."""

from __future__ import annotations

import io
import json
import operator
import pickle
import threading
import time
from concurrent.futures import Future
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from _hdna_payloads import HangAt
from graphed.checkpoint import Store, resumable
from graphed.core import Partition
from graphed.core.execution import Plan, Task
from graphed.debug import StageError
from m74_harness import leaf_plan, prepared_job, work_dirs

from graphed_executors.htcondor_backend import (
    HTCondorBackend,
    HTCondorRunner,
    LocalPilots,
    TaskTimedOut,
    driver,
    server,
)

HERE = str(Path(__file__).resolve().parent)
N_TASKS = 4
HUNG = "mem://gw21-p1"


def _run(plan: Plan[int], task_timeout_s: float | None, exits: list[int] | None = None) -> Any:
    """Run ``plan`` on two local pilots; ``exits`` collects the pids of pilots that exit mid-run."""
    pilots = LocalPilots(pythonpath=[HERE])
    backend = HTCondorBackend(pilots, 2, host="127.0.0.1", task_timeout_s=task_timeout_s)
    out: dict[str, Any] = {}

    def run() -> None:
        with HTCondorRunner(backend) as runner:
            try:
                out["value"] = runner.run(plan).value
            except BaseException as exc:
                out["error"] = exc
            deadline = time.monotonic() + 3.0  # a lost pilot hears 410 at its next beat
            while time.monotonic() < deadline and not any(p.poll() is not None for p in pilots._procs):
                time.sleep(0.05)
            out["exits"] = [p.pid for p in pilots._procs if p.poll() is not None]

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    thread.join(90.0)
    assert not thread.is_alive(), "the run hung"
    if exits is not None:
        exits.extend(out["exits"])
    if "error" in out:
        raise out["error"]
    return out["value"]


def _plan(attempts: Path, hangs: int, hang_s: float) -> Plan[int]:
    tasks = tuple(Task(i, Partition.blind(f"mem://gw21-p{i}", "", 0, 1)) for i in range(N_TASKS))
    return Plan(
        process=HangAt(HUNG, str(attempts), hangs, hang_s), combine=operator.add, empty=int, tasks=tasks
    )


@pytest.fixture(autouse=True)
def short_lease(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(server, "LEASE_S", 3.0)  # a loaded runner can stall a live pilot past 1 s


def test_a_task_past_the_timeout_fails_and_its_pilot_stops(tmp_path: Path) -> None:
    exits: list[int] = []
    with pytest.raises(StageError) as excinfo:
        _run(_plan(tmp_path, hangs=1, hang_s=120.0), task_timeout_s=3.0, exits=exits)
    (hung,) = tmp_path.iterdir()
    stage = excinfo.value
    assert stage.partition.startswith(HUNG + "::") and stage.cause_type == "TaskTimedOut"
    assert "task_timeout_s=3.0" in stage.cause_message
    err = stage.__cause__
    assert isinstance(err, TaskTimedOut)
    assert err.key.endswith("-leaf-1") and err.seconds == 3.0
    assert f":{hung.name}:" in err.pilot  # the pilot is named hostname:pid:nonce
    assert str(err) == f"task {err.key} ran on pilot {err.pilot} longer than task_timeout_s=3.0"
    assert exits == [int(hung.name)], "the timed-out pilot kept its slot during the run"


def test_a_timed_out_task_s_late_result_is_dropped(monkeypatch: pytest.MonkeyPatch) -> None:
    ts = server.TaskServer("127.0.0.1", (10000, 10100), SimpleNamespace(alive=lambda: 1), 0.05)
    try:
        fut: Future[Any] = Future()
        ts.add("k1", operator.neg, (1,), fut)
        leased = ts.lease("pilot-a")
        assert leased is not None
        time.sleep(0.1)
        ts.reap_once()
        exc = fut.exception(0)
        assert isinstance(exc, TaskTimedOut)
        assert (exc.key, exc.pilot, exc.seconds) == ("k1", "pilot-a", 0.05)
        assert pickle.loads(pickle.dumps(exc)).args == exc.args
        ts.result("pilot-a", leased[0], True, pickle.dumps(-1))
        assert fut.exception(0) is exc
        assert not ts.beat("pilot-a")  # the pilot is told to stop
    finally:
        ts.close()
        ts.shutdown()


def test_a_resumable_rerun_after_a_timeout_runs_only_the_timed_out_task(tmp_path: Path) -> None:
    attempts, store = tmp_path / "attempts", str(tmp_path / "store")
    attempts.mkdir()
    with pytest.raises(StageError, match=r"task_timeout_s=3\.0"):
        _run(resumable(_plan(attempts, hangs=1, hang_s=120.0), store), task_timeout_s=3.0)
    assert Store(store).dead_letters() == []
    rerun = resumable(_plan(attempts, hangs=1, hang_s=120.0), store)
    assert rerun.process.reused == N_TASKS - 1  # type: ignore[attr-defined]
    assert _run(rerun, task_timeout_s=3.0) == N_TASKS
    assert len(list(attempts.iterdir())) == 2


def test_a_task_within_the_timeout_and_no_timeout_run_once(tmp_path: Path) -> None:
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    assert _run(_plan(tmp_path / "a", hangs=1, hang_s=2.5), task_timeout_s=6.0) == N_TASKS
    assert _run(_plan(tmp_path / "b", hangs=1, hang_s=2.5), task_timeout_s=None) == N_TASKS
    assert len(list((tmp_path / "a").iterdir())) == len(list((tmp_path / "b").iterdir())) == 1


def test_a_driverless_run_carries_the_timeout_to_its_backend(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    markers, _ = work_dirs(tmp_path / "work")
    job = prepared_job(
        tmp_path, monkeypatch, leaf_plan(markers), "job", n_pilots=2, pilots="local", task_timeout_s=7.5
    )
    run = json.loads((job / "run.json").read_text())
    assert run["task_timeout_s"] == 7.5
    seen: dict[str, Any] = {}

    def recording(*args: Any, **kwargs: Any) -> Any:
        seen.update(kwargs)
        raise RuntimeError("stop before the task server")

    monkeypatch.setattr(driver, "HTCondorBackend", recording)
    with pytest.raises(RuntimeError, match="stop before"):
        driver._runner(run, job, io.StringIO())
    assert seen["task_timeout_s"] == 7.5
