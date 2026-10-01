"""m68c §6 on ``SubmitRunner(ThreadBackend)`` and the local executors: a join plan (``DurablePlanV2``)
runs stage by stage, with its services, its (map, dest) edge, its control and its cancel-before-release
order; the local executors refuse it."""

from __future__ import annotations

import dataclasses
import os
import threading
from pathlib import Path
from typing import Any

import pytest
from graphed.core import RunState, StopReason
from m68c_harness import (
    Background,
    ParkingControl,
    RecordingBackend,
    StandIn,
    calls_reset,
    calls_snapshot,
    closing,
    given_spec,
    hold,
    is_gather,
    is_map,
    is_pick,
    join_v2,
    key_index,
    kill_quietly,
    managed_spec,
    pick_edge,
    pid_gone,
    read_report,
    release,
    run_bounded,
    sequential,
    stage_index,
    wait_for,
)

from graphed_executors.local import ProcessPoolExecutor, ThreadExecutor
from graphed_executors.submit import SubmitRunner, ThreadBackend
from graphed_executors.submit.services import ServiceUnavailable

PARTS = 8  # steps_per_file: map tasks per side, and dests
N_MAPS = 2 * PARTS


# ---- services -----------------------------------------------------------------------------------


def test_a_given_endpoint_join_equals_the_sequential_runner_and_calls_the_server() -> None:
    plan = join_v2("given", spec=given_spec())
    assert plan.services == (given_spec(),)
    with StandIn() as server:
        runner = SubmitRunner(ThreadBackend(2), services={"sf": server.endpoint()})
        with closing(runner):
            got = run_bounded(lambda: runner.run(plan))
        calls = server.calls()
        ref = sequential(plan, {"sf": server.endpoint()})
    assert calls > 0, "no stage task called the server"
    assert got.value == ref.value and got.value, (got.value, ref.value)


def test_a_managed_child_lives_exactly_as_long_as_the_join_run(tmp_path: Path) -> None:
    report = tmp_path / "child.json"
    plan = join_v2("managed", spec=managed_spec(report))
    calls_reset()
    runner = SubmitRunner(ThreadBackend(2))
    try:
        with closing(runner):
            got = run_bounded(lambda: runner.run(plan))
        pid = read_report(report)
        bodies = set(calls_snapshot())
        assert bodies == {str(pid)}, f"stage tasks read {bodies}, the child is {pid}"
        assert pid != os.getpid()
        assert pid_gone(pid), "the run's child outlived the run"
        assert got.value
    finally:
        if report.is_file():
            kill_quietly(read_report(report))


def test_an_unbound_service_without_a_launch_is_refused_before_any_stage_task() -> None:
    backend = RecordingBackend(ThreadBackend(2))
    with closing(SubmitRunner(backend)) as runner, pytest.raises(ServiceUnavailable):
        run_bounded(lambda: runner.run(join_v2("unbound", spec=given_spec())))
    assert backend.plan_keys() == [], backend.plan_keys()


# ---- the (map, dest) edge -----------------------------------------------------------------------


def _edge_run(tag: str, peer: bool) -> tuple[Any, RecordingBackend, Any]:
    plan = join_v2(tag)
    backend = RecordingBackend(ThreadBackend(2))
    backend.capabilities = dataclasses.replace(backend.capabilities, peer_data_movement=peer)
    with closing(SubmitRunner(backend)) as runner:
        got = run_bounded(lambda: runner.run(plan))
    return plan, backend, got


def test_the_peer_edge_picks_each_map_dest_pair_on_a_worker() -> None:
    from graphed.shuffle import split  # noqa: PLC0415  (m68c API, looked up in the body)

    plan, backend, got = _edge_run("peer", peer=True)
    maps = [i for i, st in enumerate(plan.stages) if st.kind == "map_write"]
    gather = stage_index(plan, "gather_join")
    dests = [t.key for t in plan.stages[gather].tasks]
    assert len(dests) == PARTS
    picks = [pick_edge(k) for k in backend.submitted() if is_pick(k)]
    expected = [(s, t, d) for s in maps for t in range(len(plan.stages[s].tasks)) for d in dests]
    assert sorted(picks) == sorted(expected), picks
    gathers = [k for k in backend.submitted() if is_gather(k)]
    assert len(gathers) == PARTS
    for key in gathers:
        dest = plan.stages[gather].tasks[key_index(key)].key
        inputs = [
            a
            for a in backend.gather_args[key]
            if isinstance(a, bytes) and not any(a is h for h in backend.handles)
        ]
        assert len(inputs) == N_MAPS, (key, len(inputs))
        assert all(set(split(a)) == {dest} for a in inputs), key
    moved, maps_bytes = backend.measure()
    assert moved >= PARTS / 2 * maps_bytes, (moved, maps_bytes)
    assert got.value == sequential(plan).value


def test_the_driver_edge_ships_gathers_their_own_slices() -> None:
    plan, backend, got = _edge_run("driver", peer=False)
    assert not [k for k in backend.submitted() if is_pick(k)]
    gathers = [k for k in backend.submitted() if is_gather(k)]
    assert len(gathers) == PARTS
    for key in gathers:
        args = backend.data_args(key)
        assert args and all(isinstance(a, bytes) for a in args), (key, [type(a) for a in args])
    moved, maps_bytes = backend.measure()
    assert moved < 2 * maps_bytes, (moved, maps_bytes)
    assert got.value == sequential(plan).value


# ---- no service ---------------------------------------------------------------------------------


def test_a_join_without_services_equals_the_sequential_runner_bit_for_bit() -> None:
    plan = join_v2("plain")
    assert plan.services == ()
    with closing(SubmitRunner(ThreadBackend(2))) as runner:
        got = run_bounded(lambda: runner.run(plan))
    ref = sequential(plan)
    assert got.value == ref.value and got.value
    assert (got.n_partitions, got.n_combines) == (ref.n_partitions, ref.n_combines)


def test_a_control_cancelled_before_the_run_submits_nothing() -> None:
    control = ParkingControl()
    control.cancel()
    backend = RecordingBackend(ThreadBackend(2))
    with closing(SubmitRunner(backend, control=control)) as runner:
        got = run_bounded(lambda: runner.run(join_v2("pre-cancel")))
    assert got.stopped is StopReason.CANCELLED and got.value is None, got
    assert backend.plan_keys() == []
    assert control.state is RunState.RUNNING


# ---- control and the run's scope ----------------------------------------------------------------


def test_a_mid_run_cancel_cancels_the_queued_map_tasks() -> None:
    gate = threading.Event()
    held = hold("m68c-mid-run")
    plan = join_v2("mid-run", held=held)
    backend = RecordingBackend(ThreadBackend(1), gate=gate)
    control = ParkingControl()
    runner = SubmitRunner(backend, control=control)
    try:
        run = Background(lambda: runner.run(plan))
        assert wait_for(lambda: sum(map(is_map, backend.submitted())) == N_MAPS), backend.submitted()
        control.cancel()
        gate.set()
        wait_for(lambda: any(is_map(k) for k in backend.cancelled()))
        release(held)
        got = run.result()
        backend.drain()
    finally:
        gate.set()
        release(held)
        run_bounded(runner.close)
    assert got.stopped is StopReason.CANCELLED
    assert not [k for k in backend.ran_keys() if is_gather(k)]
    unrun = [k for k in backend.cancelled() if is_map(k) and k not in backend.ran_keys()]
    assert unrun, (backend.cancelled(), backend.ran_keys())
    assert control.state is RunState.RUNNING


def test_a_cancel_at_a_stage_boundary_submits_no_next_stage() -> None:
    plan = join_v2("boundary")
    gate = threading.Event()
    backend = RecordingBackend(ThreadBackend(1), gate=gate)
    control = ParkingControl()
    runner = SubmitRunner(backend, control=control)
    try:
        run = Background(lambda: runner.run(plan))
        assert wait_for(lambda: sum(map(is_map, backend.submitted())) == N_MAPS), backend.submitted()
        control.pause()
        gate.set()
        assert control.parked.wait(60.0), "the driver never held before the next stage"
        control.cancel()
        got = run.result()
        after = control.state
    finally:
        gate.set()
        if not control.parked.is_set():
            control.cancel()  # a driver that never parked is not left paused
        run_bounded(runner.close)
    assert got.stopped is StopReason.CANCELLED
    assert not [k for k in backend.submitted() if is_pick(k) or is_gather(k)], backend.submitted()
    assert after is RunState.RUNNING


def test_a_failed_map_submit_cancels_the_run_s_queued_map_tasks() -> None:
    gate = threading.Event()
    fault = OSError("submit refused (injected)")
    backend = RecordingBackend(
        ThreadBackend(1), gate=gate, fail_key=lambda k: "-map_write.1-" in k, fault=fault
    )
    runner = SubmitRunner(backend)
    try:
        with pytest.raises(OSError) as excinfo:
            run_bounded(lambda: runner.run(join_v2("fault")))
        assert excinfo.value is fault
        assert backend.probe_keys() == []
        queued = [k for k in backend.plan_keys() if "-map_write.0-" in k]
        assert queued and set(queued) <= set(backend.cancelled()), backend.cancelled()
        gate.set()
        backend.drain()
        assert not set(queued) & set(backend.ran_keys()), backend.ran_keys()
    finally:
        gate.set()
        run_bounded(runner.close)


# ---- refusals -----------------------------------------------------------------------------------


@pytest.mark.parametrize("executor", [ThreadExecutor, ProcessPoolExecutor], ids=["thread", "process"])
def test_a_local_executor_refuses_a_join_plan_naming_submit_runner(executor: Any) -> None:
    plan = join_v2("refuse-local", spec=given_spec())
    with closing(executor(1)) as ex, pytest.raises(TypeError) as excinfo:
        run_bounded(lambda: ex.run(plan))
    assert "SubmitRunner(" in str(excinfo.value) and "DurablePlanV2" in str(excinfo.value), excinfo.value
