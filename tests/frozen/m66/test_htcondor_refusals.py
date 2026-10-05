"""m66 D4: ``HTCondorRunner.run`` refuses a ``process``/``combine`` a pilot cannot import by name, with a
``ValueError`` naming ``user_modules``, before any task is submitted.

A ``__main__`` function pickles fine (about 35 B, by reference) and only fails to unpickle in another
interpreter, so a refusal that relies on ``pickle.dumps`` failing catches the lambda and misses it.

Discriminates letting the engine's ``PicklingError`` through, a refusal that only catches lambdas, and
a refusal that comes after work was submitted."""

from __future__ import annotations

import pickle
import sys
from collections.abc import Callable
from typing import Any

import pytest
from graphed.core.execution import Partition, Plan, Task
from htcondor_harness import (
    concat,
    concat_process,
    empty_text,
    expected_concat,
    local_backend,
    make_runner,
    mem_partitions,
    run_bounded,
)


def counting_submit(backend: Any) -> list[str]:
    """Instance-patch ``backend.submit`` to record each submitted key."""
    keys: list[str] = []
    inner = backend.submit

    def submit(fn: Any, /, *args: Any, key: str, **kwargs: Any) -> Any:
        keys.append(key)
        return inner(fn, *args, key=key, **kwargs)

    backend.submit = submit
    return keys


def plan_of(process: Callable[[Partition, object], str], combine: Callable[[str, str], str]) -> Plan[str]:
    tasks = tuple(Task(i, p) for i, p in enumerate(mem_partitions(3, "m66refuse")))
    return Plan(process=process, combine=combine, empty=empty_text, tasks=tasks)


def main_function(monkeypatch: pytest.MonkeyPatch) -> Callable[[Partition, object], str]:
    def m66_main_process(partition: Partition, resources: object) -> str:
        return concat_process(partition, resources)

    m66_main_process.__module__ = "__main__"
    m66_main_process.__qualname__ = "m66_main_process"
    monkeypatch.setattr(sys.modules["__main__"], "m66_main_process", m66_main_process, raising=False)
    return m66_main_process


def main_combine(monkeypatch: pytest.MonkeyPatch) -> Callable[[str, str], str]:
    class M66MainCombine:
        def __call__(self, a: str, b: str) -> str:
            return a + b

    M66MainCombine.__module__ = "__main__"
    M66MainCombine.__qualname__ = "M66MainCombine"
    monkeypatch.setattr(sys.modules["__main__"], "M66MainCombine", M66MainCombine, raising=False)
    return M66MainCombine()


def refused(plan: Plan[str], role: str) -> None:
    backend = local_backend(1)
    try:
        keys = counting_submit(backend)
        with pytest.raises(ValueError) as excinfo:
            run_bounded(lambda: make_runner(backend).run(plan))
    finally:
        backend.close()
    message = str(excinfo.value)
    assert "user_modules" in message, message
    assert f"plan.{role}" in message, message
    assert keys == [], f"the refusal came after {len(keys)} submits"


def test_a_lambda_process_is_refused() -> None:
    refused(plan_of(lambda partition, resources: concat_process(partition, resources), concat), "process")


def test_a_main_module_process_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    process = main_function(monkeypatch)
    assert len(pickle.dumps(process)) < 100  # pickles by reference; only an unpickle elsewhere fails
    refused(plan_of(process, concat), "process")


def test_a_main_module_combine_instance_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    combine = main_combine(monkeypatch)
    assert pickle.loads(pickle.dumps(combine))("a", "b") == "ab"
    refused(plan_of(concat_process, combine), "combine")


def test_importable_process_and_combine_run() -> None:
    backend = local_backend(1)
    try:
        keys = counting_submit(backend)
        value = run_bounded(lambda: make_runner(backend).run(plan_of(concat_process, concat))).value
    finally:
        backend.close()
    assert value == expected_concat(3, "m66refuse")
    assert len(keys) == 5  # 3 leaves + 2 combines: the spy sees real submits
