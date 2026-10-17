"""Plan pieces a spawned worker imports by name; only the values they make resist pickling."""

from __future__ import annotations

import pickle
from dataclasses import dataclass
from typing import Any

import awkward as ak
from graphed.awkward import AwkwardBackend
from graphed.core import Partition
from graphed.core.execution import WorkerResources

EVENTS = ak.Array({"Jet": [[{"pt": float(i % 7 + 1)}] * (i % 3 + 1) for i in range(64)]})


@dataclass
class Events:
    data: ak.Array

    def __call__(self) -> ak.Array:
        return self.data

    def partitions(self, steps_per_file: int = 1) -> tuple[Partition, ...]:
        return tuple(Partition.blind("mem://events", "", s, steps_per_file) for s in range(steps_per_file))

    def read_partition(self, partition: Partition, columns: Any, resources: WorkerResources) -> ak.Array:
        part = partition.resolve(len(self.data))
        return self.data[part.entry_start : part.entry_stop]


class Unpicklable:
    """A partial that refuses to pickle, as one carrying a lambda-holding behavior dict does."""

    def __reduce__(self) -> Any:
        raise pickle.PicklingError("this partial does not pickle")


def unpicklable_reduce(_values: Any) -> Unpicklable:
    return Unpicklable()


def keep_first(a: Any, _b: Any) -> Any:
    return a


def nothing() -> None:
    return None


@dataclass(frozen=True)
class FailFirst:
    """A task process that raises on its first ``failures`` attempts at each partition and returns 1
    after; each attempt leaves a marker file under ``marks``, so the count crosses worker processes."""

    marks: str
    failures: int = 1

    def __call__(self, partition: Partition, resources: WorkerResources) -> int:
        import os  # noqa: PLC0415

        name = partition.uri.rsplit("/", 1)[-1]
        done = sum(m.startswith(name + ".") for m in os.listdir(self.marks))
        open(os.path.join(self.marks, f"{name}.{done}"), "w").close()
        if done < self.failures:
            raise RuntimeError(f"attempt {done} at {partition.uri}")
        return 1


@dataclass(frozen=True)
class HangAt:
    """Sleeps ``hang_s`` on the first ``hangs`` attempts at ``uri`` and returns 1 otherwise; each
    attempt there leaves a file named by its pid under ``attempts``."""

    uri: str
    attempts: str
    hangs: int
    hang_s: float

    def __call__(self, partition: Partition, resources: WorkerResources) -> int:
        import os  # noqa: PLC0415
        import time  # noqa: PLC0415

        if partition.uri == self.uri:
            done = len(os.listdir(self.attempts))
            open(os.path.join(self.attempts, str(os.getpid())), "w").close()
            if done < self.hangs:
                time.sleep(self.hang_s)
        return 1


def scale_by(k: float) -> Any:
    """A closure over ``k``: pickle by reference cannot name it."""
    return lambda x: x * k


def sum_all(values: Any) -> float:
    return float(ak.sum(ak.Array(values[0])))


def add(a: float, b: float) -> float:
    return a + b


def zero() -> float:
    return 0.0


LAMBDA_BEHAVIOR: dict[Any, Any] = {"unused": lambda x: x}  # vector's behavior holds lambdas like this


def make_lambda_behavior_backend() -> AwkwardBackend:
    return AwkwardBackend(behavior=LAMBDA_BEHAVIOR)


def first(values: Any) -> Any:
    return values[0]


def concat(a: ak.Array, b: ak.Array) -> ak.Array:
    return ak.concatenate([a, b])


def empty_array() -> ak.Array:
    return ak.Array([])
