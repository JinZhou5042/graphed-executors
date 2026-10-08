"""A failed task re-checks every service of the plan (plan §2 C2: checks built from the endpoints bound for
``plan.services``), not only the first."""

from __future__ import annotations

import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from graphed.core.execution import Partition, Plan, Task
from graphed.services import ServiceSpec

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "frozen" / "m68d"))
from m68d_harness import Background, Web, empty_tuple, given_spec, pair_concat, partitions, wait_for

from graphed_executors.submit import SubmitRunner, ThreadBackend
from graphed_executors.submit.services import ServiceUnreachable


@dataclass(frozen=True)
class FailsAtTheGate:
    gate: str

    def bind_services(self, endpoints: Mapping[str, str]) -> FailsAtTheGate:
        return self

    def __call__(self, partition: Partition, resources: object) -> tuple[int, ...]:
        Path(self.gate + ".started").touch()
        wait_for(Path(self.gate).exists, 60.0)
        raise LookupError("m-the-task-failed")


def test_a_failed_task_names_the_plan_s_second_service_when_only_it_is_gone(tmp_path: Path) -> None:
    first, second = Web(), Web()
    spec2 = ServiceSpec("web2", "http", check="http:/", ports=(40000, 40010), launch=None, timeout_s=30.0)
    gate = tmp_path / "gate"
    plan = Plan(
        FailsAtTheGate(str(gate)),
        pair_concat,
        empty_tuple,
        (Task(0, partitions(tmp_path, 1)[0]),),
        services=(given_spec(), spec2),
    )
    try:
        with SubmitRunner(
            ThreadBackend(1), services={"web": first.endpoint, "web2": second.endpoint}
        ) as runner:
            run = Background(lambda: runner.run(plan))
            assert wait_for(Path(str(gate) + ".started").exists, 60.0), "the task never started"
            second.close()
            gate.touch()
            err = run.error()
    finally:
        first.close()
    assert isinstance(err, ServiceUnreachable) and (err.name, err.endpoint) == ("web2", second.endpoint), (
        repr(err)
    )
