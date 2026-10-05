"""m66 D5: a dead pilot's lease is re-queued once to another pilot, then surfaces as a ``StageError``
attributed to the partition. ``server.LEASE_S`` is 2 s here, so a pilot counts as lost after 2 s.

Discriminates requeue-forever (the poisoned uri kills a third pilot, or the run hangs), fail-on-first-
loss (the die-once run errors), a second ``set_running_or_notify_cancel`` on requeue (the die-once run
errors), a raw ``WorkerLost`` escaping from a blocking dependency resolution in ``submit``, attribution
to the task key instead of the partition, and a hang when no pilot is left."""

from __future__ import annotations

import socket
from pathlib import Path

import pytest
from graphed.core.execution import Plan, Task
from graphed.debug import StageError
from htcondor_harness import (
    DieOnceProcess,
    PoisonUriProcess,
    RecordingLauncher,
    concat,
    empty_text,
    exit_process,
    expected_concat,
    htcondor_api,
    local_backend,
    make_runner,
    mem_partitions,
    prov_plan,
    run_bounded,
    server_api,
)


def shorten_lease(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(server_api(), "LEASE_S", 2.0)


def test_a_pilot_death_is_requeued_to_another_pilot(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    shorten_lease(monkeypatch)
    marker = tmp_path / "died.pid"
    poison = mem_partitions(6, "m66dieonce")[2].uri
    plan = prov_plan(6, "m66dieonce", process=DieOnceProcess(str(marker), poison))
    with make_runner(local_backend(3)) as runner:
        value = run_bounded(lambda: runner.run(plan)).value
    assert value.payload == expected_concat(6, "m66dieonce")
    assert marker.exists(), "the poisoned leaf never killed a pilot: the scenario did not run"
    dead_pid = int(marker.read_text())
    (rerun_pid,) = {pid for uri, pid, _worker in value.leaves if uri == poison}
    assert rerun_pid != dead_pid


def test_a_twice_lost_leaf_is_an_attributed_stage_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    shorten_lease(monkeypatch)
    attempts = tmp_path / "attempts"
    attempts.mkdir()
    parts = mem_partitions(4, "m66poison")
    poison = parts[1].uri
    plan = Plan(
        process=PoisonUriProcess(poison, str(attempts)),
        combine=concat,
        empty=empty_text,
        tasks=tuple(Task(i, p) for i, p in enumerate(parts)),
    )
    with make_runner(local_backend(3)) as runner, pytest.raises(StageError) as excinfo:
        run_bounded(lambda: runner.run(plan))
    err = excinfo.value
    assert type(err) is StageError
    assert poison in err.partition, err.partition
    assert err.cause_type == "KilledWorker"
    assert f"{socket.gethostname()}:" in err.cause_message, err.cause_message
    assert len(list(attempts.iterdir())) == 2, "the lost lease must run exactly twice (requeue once)"


def test_the_last_pilot_dying_fails_the_run_instead_of_hanging(monkeypatch: pytest.MonkeyPatch) -> None:
    shorten_lease(monkeypatch)
    parts = mem_partitions(2, "m66nopilot")
    plan = Plan(
        process=exit_process,
        combine=concat,
        empty=empty_text,
        tasks=tuple(Task(i, p) for i, p in enumerate(parts)),
    )
    with make_runner(local_backend(1)) as runner, pytest.raises(StageError) as excinfo:
        run_bounded(lambda: runner.run(plan), timeout_s=120.0)
    assert excinfo.value.cause_type == "KilledWorker"
    assert "no pilots left" in excinfo.value.cause_message, excinfo.value.cause_message


def test_describe_failure_maps_worker_lost_only() -> None:
    api = htcondor_api()
    backend = api.HTCondorBackend(RecordingLauncher(), 1, host="127.0.0.1")
    try:
        lost = api.WorkerLost("graphed-abc-leaf-3", "host.example:4242")
        assert (lost.key, lost.pilot) == ("graphed-abc-leaf-3", "host.example:4242")
        assert backend.describe_failure(lost) == ("graphed-abc-leaf-3", "host.example:4242")
        assert backend.describe_failure(ValueError("negative pt")) is None
    finally:
        backend.close()
