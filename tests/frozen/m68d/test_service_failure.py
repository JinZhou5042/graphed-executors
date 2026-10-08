"""m68d C (plan §2 "C — a lost service is a service failure, and the run stops at its first failure"),
rows C1-C6 of §4: every OS, on ``ThreadBackend`` and on ``HTCondorBackend`` over ``LocalPilots``."""

from __future__ import annotations

import pickle
import subprocess
import sys
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
from graphed.core.execution import Plan, Task
from m68d_harness import (
    DRIVER_ID,
    DRIVER_MODULE,
    HARNESS_DIR,
    LEAF_ERROR,
    PLAN_ERROR,
    SERVICE,
    WORKER_ID,
    Background,
    EventLog,
    RaiseIn,
    Web,
    backend_api,
    empty_tuple,
    engine_api,
    gated_plan,
    given_spec,
    launch_api,
    log_lines,
    pair_concat,
    partitions,
    raised_by,
    run_bounded,
    run_json,
    services_api,
    stage_plan,
    stop_leaf,
    wait_for,
    write_driver_job,
    write_machine_ad,
)

from graphed_executors.submit import SubmitRunner
from graphed_executors.submit.threadpool import ThreadBackend

BACKENDS = ["thread", "htcondor"]


def make_runner(kind: str, **kwargs: Any) -> Any:
    """``SubmitRunner(ThreadBackend(1))`` or ``HTCondorRunner`` over one local pilot."""
    if kind == "thread":
        return SubmitRunner(ThreadBackend(1), **kwargs)
    api = backend_api()
    pilots = launch_api().LocalPilots(pythonpath=[HARNESS_DIR])
    backend = api.HTCondorBackend(pilots, 1, host="127.0.0.1", port_range=(0, 0))
    return api.HTCondorRunner(backend, min_pilots=1, **kwargs)


@pytest.fixture
def runners() -> Iterator[list[Any]]:
    made: list[Any] = []
    yield made
    for runner in made:
        run_bounded(runner.close, 120.0)


def started(mark: Path) -> list[int]:
    return sorted(int(p.name.split("-")[1]) for p in mark.glob("start-*"))


def stop_plan(mark: Path) -> Plan[Any]:
    mark.mkdir()
    tasks = tuple(Task(i, p) for i, p in enumerate(partitions(mark, 8)))
    return Plan(process=stop_leaf, combine=pair_concat, empty=empty_tuple, tasks=tasks)


def kill_after_task_zero(mark: Path, web: Web) -> None:
    """Once task 0 is done, shut the service and let the later tasks run."""
    assert wait_for((mark / "t0.done").exists, 120.0), "task 0 never finished"
    web.close()
    (mark / "killed").touch()


# ---- C1: the fixed path stops at its first failed task -------------------------------------------------


@pytest.mark.parametrize("kind", BACKENDS)
def test_a_fixed_run_raises_at_its_first_failed_leaf(
    kind: str, tmp_path: Path, runners: list[Any]
) -> None:
    runner = make_runner(kind)
    runners.append(runner)
    mark = tmp_path / "mark"
    err = raised_by(lambda: runner.run(stop_plan(mark)))
    at_raise = started(mark)
    assert type(err) is ValueError and str(err) == LEAF_ERROR, repr(err)
    assert 1 in at_raise and len(at_raise) <= 3, f"leaves started by the raise: {at_raise}"
    run_bounded(runner.close, 120.0)
    assert len(started(mark)) < 8, f"leaves started by close: {started(mark)}"


@pytest.mark.parametrize("kind", BACKENDS)
def test_a_fixed_run_with_a_monitor_raises_well_within_the_drain_timeout(
    kind: str, tmp_path: Path, runners: list[Any]
) -> None:
    monitor = EventLog()
    runner = make_runner(kind, monitor=monitor)
    runners.append(runner)
    if kind == "htcondor":
        run_bounded(runner.wait_for_pilots, 120.0)
    mark = tmp_path / "mark"
    begun = time.monotonic()
    err = raised_by(lambda: runner.run(stop_plan(mark)))
    took = time.monotonic() - begun
    assert type(err) is ValueError and str(err) == LEAF_ERROR, repr(err)
    assert len(monitor.phases("submitted")) == 8, "the monitor was not attached to the run"
    assert took < engine_api()._DRAIN_TIMEOUT_S / 2, f"raised after {took:.1f}s"
    run_bounded(runner.close, 120.0)
    assert len(started(mark)) < 8, f"leaves started by close: {started(mark)}"


# ---- C2: a task whose service is gone raises ServiceUnreachable ---------------------------------------


@pytest.mark.parametrize("kind", BACKENDS)
def test_a_task_whose_service_is_gone_raises_service_unreachable(
    kind: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, runners: list[Any]
) -> None:
    monkeypatch.setenv("_CONDOR_MACHINE_AD", str(write_machine_ad(tmp_path / "worker.ad", WORKER_ID)))
    web = Web()
    runner = make_runner(kind, services={SERVICE: web.endpoint})
    runners.append(runner)
    if kind == "htcondor":  # the pilot keeps the worker's identity; the driver's own differs from now on
        run_bounded(runner.wait_for_pilots, 120.0)
        monkeypatch.setenv("_CONDOR_MACHINE_AD", str(write_machine_ad(tmp_path / "driver.ad", DRIVER_ID)))
    mark = tmp_path / "mark"
    run = Background(lambda: runner.run(gated_plan(mark, given_spec())))
    kill_after_task_zero(mark, web)
    err = run.error()
    assert isinstance(err, services_api().ServiceUnreachable), repr(err)
    task_error = (mark / "t1.error").read_text()
    assert (err.name, err.endpoint, err.worker) == (SERVICE, web.endpoint, WORKER_ID), err.args
    assert task_error in err.reason, f"the task's {task_error} is not in {err.reason!r}"
    assert f"127.0.0.1:{web.port}" in err.reason.replace(task_error, ""), err.reason


# ---- C3: a plan's own error with a live service stays intact, and was re-checked ---------------------------


@pytest.mark.parametrize("error", ["OSError", "ConnectionRefusedError"])
@pytest.mark.parametrize("kind", BACKENDS)
def test_a_plan_error_with_a_live_service_is_intact_and_was_rechecked(
    kind: str, error: str, tmp_path: Path, runners: list[Any]
) -> None:
    with Web() as web:
        runner = make_runner(kind, services={SERVICE: web.endpoint})
        runners.append(runner)
        tasks = (Task(0, partitions(tmp_path, 1)[0]),)

        def gets_during(process: RaiseIn) -> tuple[int, Any]:
            plan = Plan(process, pair_concat, empty_tuple, tasks, services=(given_spec(),))
            before = web.gets()
            try:
                outcome: Any = run_bounded(lambda: runner.run(plan))
            except Exception as exc:
                outcome = exc
            return web.gets() - before, outcome

        twin_gets, twin = gets_during(RaiseIn(None))
        failed_gets, err = gets_during(RaiseIn(error))
    assert twin.value == ((0, "ok"),), twin
    assert type(err).__name__ == error and str(err) == PLAN_ERROR, repr(err)
    assert failed_gets - twin_gets == 1, f"GETs: {failed_gets} with the failed task, {twin_gets} without"


# ---- C4: only a plan with services has its task functions wrapped ---------------------------------------


class SpyBackend(ThreadBackend):
    """``ThreadBackend(1)`` recording each submitted function's name and arguments."""

    def __init__(self) -> None:
        super().__init__(1)
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def submit(self, fn: Callable[..., object], /, *args: object, **kwargs: Any) -> Any:
        self.calls.append((fn.__name__, args))
        return super().submit(fn, *args, **kwargs)


def test_a_service_free_plan_submits_its_task_functions_unwrapped(tmp_path: Path) -> None:
    with Web() as web:
        backend = SpyBackend()
        with SubmitRunner(backend, services={SERVICE: web.endpoint}) as runner:
            run_bounded(lambda: runner.run(gated_plan(tmp_path / "plain", None, n=2, process=RaiseIn(None))))
            plain = list(backend.calls)
            backend.calls.clear()
            run_bounded(lambda: runner.run(gated_plan(tmp_path / "served", given_spec(), n=2, process=RaiseIn(None))))
            served = [c for c in backend.calls if c[0] != "_probe_services"]
    assert sorted({name for name, _ in plain}) == ["_combine_task", "_leaf_task"], plain
    assert served and {name for name, _ in served} == {"_service_checked"}, served
    assert sorted({args[1].__name__ for _, args in served}) == ["_combine_task", "_leaf_task"], served
    for _, args in served:
        assert [tuple(check) for check in args[0]] == [(SERVICE, web.endpoint, "http:/")], args[0]


# ---- C5: a driverless run whose given service dies exits 1 -------------------------------------------------


def test_a_driverless_run_whose_given_service_dies_exits_1(tmp_path: Path) -> None:
    web = Web()
    mark, job = tmp_path / "mark", tmp_path / "job"
    run = run_json("generic", endpoints={SERVICE: web.endpoint})
    write_driver_job(job, gated_plan(mark, given_spec()), run)
    driver = subprocess.Popen([sys.executable, "-m", DRIVER_MODULE, str(job)], cwd=job)
    try:
        kill_after_task_zero(mark, web)
        code = driver.wait(240.0)
    finally:
        if driver.poll() is None:
            driver.kill()
            driver.wait()
    ok, payload = pickle.loads((job / "result.pkl").read_bytes())
    assert (code, ok) == (1, False), f"exit {code}: {payload!r}"
    assert isinstance(payload, services_api().ServiceUnreachable), repr(payload)
    assert (payload.name, payload.endpoint) == (SERVICE, web.endpoint), payload.args
    assert log_lines(job, "rerun:") == [], "a service no SERVICE node hosts was rerun"


# ---- C6: the same through a DurablePlanV2 stage ----------------------------------------------------------


def test_a_stage_task_whose_service_is_gone_raises_service_unreachable(tmp_path: Path) -> None:
    web = Web()
    mark = tmp_path / "mark"
    with SubmitRunner(ThreadBackend(1), services={SERVICE: web.endpoint}) as runner:
        run = Background(lambda: runner.run(stage_plan(mark, given_spec())))
        kill_after_task_zero(mark, web)
        err = run.error()
    assert isinstance(err, services_api().ServiceUnreachable), repr(err)
    assert (err.name, err.endpoint) == (SERVICE, web.endpoint), err.args
    assert (mark / "t1.error").read_text() in err.reason, err.reason
