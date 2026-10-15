"""Plan pieces a spawned worker imports by name; only the values they make resist pickling."""

from __future__ import annotations

import pickle
from dataclasses import dataclass
from typing import Any

import awkward as ak
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
