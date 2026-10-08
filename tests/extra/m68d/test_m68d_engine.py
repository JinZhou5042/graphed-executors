"""m68d engine paths the frozen suite does not reach: a fixed run raising at a failed combine, not only at a
failed leaf; the re-check naming the first service that fails among several, and a run behind a
``RunControl`` window classified like the default path."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from graphed.core import RunControl
from graphed.core.execution import Partition, Plan, Task
from m68d_extra_parts import failing_combine, gated_leaf, no_leaves

from graphed_executors.htcondor_backend import HTCondorBackend, HTCondorRunner, LocalPilots
from graphed_executors.submit import SubmitRunner, ThreadBackend
from graphed_executors.submit.engine import _service_checked
from graphed_executors.submit.services import ServiceUnreachable, host_identity

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "frozen" / "m68d"))
from m68d_harness import SERVICE, Background, Web, gated_plan, given_spec, raised_by, run_bounded, wait_for

HERE = str(Path(__file__).resolve().parent)


def test_a_fixed_run_raises_at_a_failed_combine_while_other_leaves_still_run(tmp_path: Path) -> None:
    gate = tmp_path / "gate"
    tasks = tuple(Task(i, Partition(f"mem://m68d/{i}", str(gate), i, i + 1)) for i in range(4))
    plan = Plan(process=gated_leaf, combine=failing_combine, empty=no_leaves, tasks=tasks)
    # three pilots: leaves 2 and 3 hold two of them on the gate, the third runs combine(0, 1)
    backend = HTCondorBackend(LocalPilots(pythonpath=[HERE]), 3, host="127.0.0.1", port_range=(0, 0))
    runner = HTCondorRunner(backend, min_pilots=3)
    try:
        run_bounded(runner.wait_for_pilots, 120.0)
        err = raised_by(lambda: runner.run(plan), 50.0)  # within GATE_S: the gated leaves have not returned
    finally:
        gate.touch()
        run_bounded(runner.close, 120.0)
    assert type(err) is ValueError and str(err) == "m-combine-of-leaf-zero-failed", repr(err)


def _raises() -> None:
    raise LookupError("m-the-task-failed")


def test_the_first_service_whose_check_fails_is_named() -> None:
    with Web() as live:
        gone = Web()
        gone.close()
        checks = (
            ("live", live.endpoint, "http:/"),
            ("gone", gone.endpoint, "tcp"),
            ("also", gone.endpoint, "tcp"),
        )
        with pytest.raises(ServiceUnreachable) as info:
            _service_checked(checks, _raises)
        assert live.gets() == 1, "the live service was not re-checked first"
    err = info.value
    assert (err.name, err.endpoint, err.worker) == ("gone", gone.endpoint, host_identity()), err.args
    assert (
        err.reason.startswith(f"tcp connect to {gone.endpoint} failed") and "m-the-task-failed" in err.reason
    )
    assert isinstance(err.__cause__, LookupError)


def test_a_windowed_run_whose_service_is_gone_raises_service_unreachable(tmp_path: Path) -> None:
    web = Web()
    mark = tmp_path / "mark"
    with SubmitRunner(ThreadBackend(1), control=RunControl(), services={SERVICE: web.endpoint}) as runner:
        run = Background(lambda: runner.run(gated_plan(mark, given_spec())))
        assert wait_for((mark / "t0.done").exists, 60.0), "task 0 never finished"
        web.close()
        (mark / "killed").touch()
        err = run.error()
    assert isinstance(err, ServiceUnreachable) and (err.name, err.endpoint) == (SERVICE, web.endpoint), repr(
        err
    )
