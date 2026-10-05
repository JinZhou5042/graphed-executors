"""m66 D1: the m42 ``SubmitBackend`` conformance bodies over ``HTCondorBackend`` with two local pilots.

Discriminates a driver-side compute stub (the pid witness), unresolved future args (the task fn
crashes on a future), a non-bytes broadcast, a stringified ``StageError``, lost monitor events, and a
capability vector that is not the all-False floor."""

from __future__ import annotations

import os
import pickle

import pytest
from graphed.core.execution import Plan, SequentialRunner, StopReason, Task
from graphed.debug import StageError
from htcondor_harness import (
    Feed,
    RecordingMonitor,
    concat_plan,
    double,
    double_with_site,
    events_by_key,
    expected_concat,
    fold_combine,
    fold_empty,
    fold_oracle,
    fold_process,
    local_backend,
    make_runner,
    mem_partitions,
    payload_len,
    run_bounded,
    stage_error_plan,
    wait_for,
)

from graphed_executors.submit import SubmitBackend, SubmitCapabilities, SubmitFuture

ALL_FALSE = SubmitCapabilities(
    peer_data_movement=False,
    scatter_broadcast=False,
    pin_to_worker=False,
    per_task_retries=False,
    per_task_resources=False,
    cancel_running=False,
    worker_file_cache=False,
)


@pytest.mark.parametrize("n", [0, 1, 2, 5, 16])
def test_fixed_plan_matches_sequential_bit_for_bit(n: int) -> None:
    plan = concat_plan(n, f"m66conf{n}")
    expected = SequentialRunner().run(plan)
    with make_runner(local_backend(2)) as runner:
        got = run_bounded(lambda: runner.run(plan))
    assert pickle.dumps(got.value) == pickle.dumps(expected.value)
    assert got.value == expected_concat(n, f"m66conf{n}")
    assert got.n_partitions == n
    assert got.n_combines == max(0, n - 1)
    assert got.stopped is StopReason.EXHAUSTED


def test_tasks_are_reduced_in_key_order_not_submission_order() -> None:
    plan = concat_plan(9, "m66confrev", reverse_tasks=True)
    with make_runner(local_backend(2)) as runner:
        assert run_bounded(lambda: runner.run(plan)).value == expected_concat(9, "m66confrev")


def test_adaptive_exhaustion_folds_everything() -> None:
    parts = mem_partitions(12, "m66confadapt")
    tasks = [Task(i, p) for i, p in enumerate(parts)]
    feed = Feed([tasks[0:4], tasks[4:8], tasks[8:12]])
    plan = Plan(process=fold_process, combine=fold_combine, empty=fold_empty, next_tasks=feed)
    with make_runner(local_backend(2)) as runner:
        got = run_bounded(lambda: runner.run(plan))
    assert got.stopped is StopReason.EXHAUSTED
    assert got.n_partitions == 12
    assert got.value.uris == frozenset(p.uri for p in parts)
    assert got.value.value == fold_oracle(got.value.uris)


def test_worker_error_surfaces_as_intact_stage_error() -> None:
    with make_runner(local_backend(2)) as runner, pytest.raises(StageError) as excinfo:
        run_bounded(lambda: runner.run(stage_error_plan(4, "m66conferr")))
    assert excinfo.value.op == "mul"
    assert excinfo.value.user_frame.filename == "user_analysis.py"
    assert excinfo.value.user_frame.lineno == 42


def test_direct_submit_seam_resolves_future_args_and_broadcast_bytes() -> None:
    backend = local_backend(2)
    try:
        assert isinstance(backend, SubmitBackend)
        assert backend.capabilities == ALL_FALSE
        assert backend.n_workers() == 2
        f1 = backend.submit(double, 21, key="graphed-m66conf-direct-1")
        assert isinstance(f1, SubmitFuture)
        assert f1.result(timeout=120) == 42
        assert f1.done() and f1.exception(timeout=120) is None
        f2 = backend.submit(
            double, f1, key="graphed-m66conf-direct-2", retries=2, priority=5, resources={"GPU": 1.0}
        )
        assert f2.result(timeout=120) == 84  # the future arg was resolved before fn ran
        backend.cancel([])
        payload = b"m66-broadcast-payload"
        handle = backend.broadcast(payload, token="m66conf-token")
        f3 = backend.submit(payload_len, handle, key="graphed-m66conf-direct-3")
        assert f3.result(timeout=120) == len(payload)
    finally:
        backend.close()


def test_future_arg_consumer_runs_on_a_pilot_not_the_driver() -> None:
    backend = local_backend(2)
    try:
        f1 = backend.submit(double, 8, key="graphed-m66conf-site-1")
        f2 = backend.submit(double_with_site, f1, key="graphed-m66conf-site-2")
        value, pid, worker = f2.result(timeout=120)
    finally:
        backend.close()
    assert value == 32
    assert pid != os.getpid(), "the composed task computed in the driver process"
    assert worker not in ("", "driver", "local"), f"no pilot identity at the task site: {worker!r}"


def test_monitored_run_emits_exact_phases() -> None:
    monitor = RecordingMonitor()
    with make_runner(local_backend(2), monitor=monitor) as runner:
        value = run_bounded(lambda: runner.run(concat_plan(6, "m66confmon"))).value
    assert value == expected_concat(6, "m66confmon")
    wait_for(lambda: len(monitor.events) >= 18)
    per_key = events_by_key(monitor.events)
    assert set(per_key) == set(range(6))
    for key in range(6):
        phases = [str(ev.phase) for ev in per_key[key]]
        assert sorted(phases) == ["finished", "started", "submitted"]
        assert phases.index("submitted") < phases.index("started") < phases.index("finished")
