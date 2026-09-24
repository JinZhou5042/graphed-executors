"""TaskVine plan parity, worker errors, and adaptive execution."""

import builtins
import importlib
import os
import shutil
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import cloudpickle
import pytest
from graphed.core import Partition, Plan, Task
from graphed.core.execution import Executor, SequentialRunner, StopReason

import graphed_executors.taskvine_backend as backend
from graphed_executors.local import ThreadExecutor
from graphed_executors.taskvine_backend import (
    TaskVineExecutor,
    TaskVineWorkerError,
    _raise_worker_error,
    _task_runtime,
    _vinegraph_context,
)

pytest.importorskip("ndcctools.taskvine.vine_graph")
sys.path.insert(0, str(Path(__file__).resolve().parent))
import toy_plans

WORKER_MODE = os.environ.get("GTV_WORKER") == "1"


def count(partition: Partition, resources: object) -> int:
    return partition.n_entries


def add(left: int, right: int) -> int:
    return left + right


def zero() -> int:
    return 0


def test_local_plan_uses_public_taskvine_executor(tmp_path: Path) -> None:
    plan = Plan(
        process=count,
        combine=add,
        empty=zero,
        tasks=tuple(Task(i, Partition("demo", "", i, i + 1)) for i in range(4)),
    )
    with TaskVineExecutor(local=True, port=0, work_dir=tmp_path) as executor:
        assert isinstance(executor, Executor)
        result = executor.run(plan)
    assert (result.value, result.n_partitions, result.n_combines, result.stopped) == (
        4,
        4,
        3,
        StopReason.EXHAUSTED,
    )


@pytest.fixture(scope="module")
def executor(tmp_path_factory):
    work = tmp_path_factory.mktemp("vine")
    ex = TaskVineExecutor(
        local=not WORKER_MODE,
        manager_name=f"graphed-test-{os.getpid()}",
        port=0,
        libcores=2,
        wait_for_workers=1 if WORKER_MODE else 0,
        work_dir=work,
        run_info_path=str(work / "logs"),
        run_info_template="test",
        ship=[Path(toy_plans.__file__)],
    )
    worker = None
    output = None
    try:
        if WORKER_MODE:
            worker_path = shutil.which("vine_worker", path=os.path.dirname(sys.executable))
            if worker_path is None:
                pytest.fail("GTV_WORKER=1 requires vine_worker in the active environment")
            output = (work / "worker.out").open("w")
            worker = subprocess.Popen(
                [
                    worker_path,
                    "--cores",
                    "2",
                    "--memory",
                    "4000",
                    "--disk",
                    "8000",
                    "--timeout",
                    "600",
                    "-s",
                    str(work / "worker"),
                    "localhost",
                    str(ex.manager.port),
                ],
                env={**os.environ, "PATH": f"{os.path.dirname(sys.executable)}:{os.environ['PATH']}"},
                stdout=output,
                stderr=subprocess.STDOUT,
            )
        yield ex
    finally:
        if worker is not None:
            worker.terminate()
            worker.wait(timeout=30)
        if output is not None:
            output.close()
        ex.close()


def test_plan_matches_reference(executor):
    plan = toy_plans.width_plan(10)
    result = executor.run(plan)
    assert result.value == SequentialRunner().run(plan).value
    assert (result.n_partitions, result.n_combines, result.stopped) == (10, 9, StopReason.EXHAUSTED)
    assert executor.last_stats.n_graph_nodes == 19


def test_reduction_matches_thread_bitwise(executor):
    plan = toy_plans.float_plan(9)
    assert executor.run(plan).value.tobytes() == ThreadExecutor(max_workers=4).run(plan).value.tobytes()


def test_empty_and_single_partition(executor):
    assert executor.run(toy_plans.width_plan(0)).value == 0
    result = executor.run(toy_plans.width_plan(1))
    assert (result.value, result.n_partitions, result.n_combines) == (1, 1, 0)


def test_blind_partitions(executor):
    assert executor.run(toy_plans.blind_plan(4)).value == 10


def test_open_once_and_input_order(executor):
    toy_plans.OPEN_COUNT["n"] = 0
    assert executor.run(toy_plans.label_plan()).value == ["ALPHA:0", "BETA:1", "ALPHA:2"]
    if not WORKER_MODE:
        assert toy_plans.OPEN_COUNT["n"] <= 2


def test_relative_input_paths(executor, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data").mkdir()
    for name in ("a", "b"):
        (tmp_path / "data" / f"{name}.txt").write_text(name.upper())
    assert executor.run(toy_plans.relative_file_plan(["data/a.txt", "data/b.txt"])).value == ["A", "B"]


def test_worker_error_preserves_type(executor):
    with pytest.raises(ValueError, match="boom at entry 3") as info:
        executor.run(toy_plans.failing_plan(6))
    assert any("TaskVine worker" in note for note in getattr(info.value, "__notes__", []))


def test_adaptive_rounds(executor):
    result = executor.run(toy_plans.adaptive_plan(n=9, size=3))
    assert result.value == 90 and result.n_partitions == 9
    assert executor.last_stats.n_rounds == 3


def test_adaptive_stop_condition(executor):
    result = executor.run(toy_plans.adaptive_plan(n=30, size=3, target_events=50))
    assert result.stopped == StopReason.TARGET_EVENTS
    assert result.value == 60 and result.n_partitions == 6


def test_duplicate_task_keys_rejected_before_submission(executor):
    partition = Partition("demo", "", 0, 1)
    plan = Plan(
        process=count,
        combine=add,
        empty=zero,
        tasks=(Task(1, partition), Task(1, partition)),
    )
    with pytest.raises(ValueError, match="task keys must be unique"):
        executor.run(plan)


def test_empty_plan_cannot_be_lowered(executor):
    with pytest.raises(ValueError, match="cannot lower an empty task set"):
        executor.lower(toy_plans.width_plan(0))


def test_missing_shipped_file_and_duplicate_destination(tmp_path):
    with pytest.raises(FileNotFoundError):
        TaskVineExecutor(ship=[tmp_path / "absent.py"])
    first = tmp_path / "first" / "analysis.py"
    second = tmp_path / "second" / "analysis.py"
    first.parent.mkdir()
    second.parent.mkdir()
    first.touch()
    second.touch()
    with pytest.raises(ValueError, match="duplicate worker sandbox destination"):
        TaskVineExecutor(ship=[first, second])


def test_unserializable_remote_error_uses_worker_error():
    class UnpicklableError(Exception):
        def __reduce__(self):
            raise TypeError("cannot pickle")

    def fail(_left, _right):
        raise UnpicklableError("bad combine")

    blob = cloudpickle.dumps((count, fail, zero))
    result = _task_runtime.run_combine(blob, _task_runtime.Partial(value=1), _task_runtime.Partial(value=2))
    assert result.error[0] is None
    with pytest.raises(TaskVineWorkerError, match="bad combine"):
        _raise_worker_error(result)

    # A corrupt serialized exception also falls back to readable remote traceback text.
    result.error = (b"invalid pickle", "remote stack", 3)
    with pytest.raises(TaskVineWorkerError, match="remote stack"):
        _raise_worker_error(result)


def test_failed_partial_short_circuits_the_other_branch():
    blob = cloudpickle.dumps((count, add, zero))
    failed = _task_runtime.Partial(error=(None, "failure", 2))
    good = _task_runtime.Partial(value=1)
    assert _task_runtime.run_combine(blob, failed, good) is failed
    assert _task_runtime.run_combine(blob, good, failed) is failed


def test_task_runner_primes_each_plan_once(monkeypatch):
    blob = cloudpickle.dumps((count, add, zero))
    monkeypatch.delattr(sys, _task_runtime._FN_CACHE_ATTR, raising=False)
    monkeypatch.delattr(sys, _task_runtime._RESOURCES_ATTR, raising=False)
    graph = SimpleNamespace(task_dict={0: (None, (blob,), {}), 1: (None, (blob,), {}), 2: (None, (), {})})
    _task_runtime.prime(graph)
    cached = _task_runtime.plan_functions(blob)
    assert cached[0] is count
    assert _task_runtime.plan_functions(blob) is cached
    assert _task_runtime.worker_resources() is _task_runtime.worker_resources()


def test_context_loader_keeps_worker_sandbox_and_tolerates_priming_failure(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "path", [path for path in sys.path if path != str(tmp_path)])
    graph = SimpleNamespace(task_dict={})
    blob = cloudpickle.dumps(graph)
    assert _vinegraph_context.context_loader(blob)["graph"].task_dict == {}
    assert sys.path[0] == str(tmp_path)

    def fail(_graph):
        raise RuntimeError("prime unavailable")

    monkeypatch.setattr(_task_runtime, "prime", fail)
    assert _vinegraph_context.context_loader(blob)["graph"].task_dict == {}
    assert "priming skipped" in capsys.readouterr().err


def test_optional_backend_import_without_vinegraph(monkeypatch):
    original_import = builtins.__import__

    def missing_vinegraph(name, *args, **kwargs):
        if name.startswith("ndcctools.taskvine.vine_graph"):
            raise ModuleNotFoundError(name)
        return original_import(name, *args, **kwargs)

    try:
        with monkeypatch.context() as patch:
            patch.setattr(builtins, "__import__", missing_vinegraph)
            importlib.reload(backend)
            assert isinstance(backend._TASKVINE_IMPORT_ERROR, ModuleNotFoundError)
            with pytest.raises(ImportError, match="requires a build"):
                _ = backend.TaskVineExecutor().manager
    finally:
        importlib.reload(backend)
