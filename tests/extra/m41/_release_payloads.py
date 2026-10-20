"""Leaf work that records which worker process ran it, for the driver-death release test."""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import cast

import numpy as np
from graphed.core import Partition, Plan, Task
from graphed.core.execution import WorkerResources


def record_pid(partition: Partition, resources: WorkerResources) -> np.ndarray:
    Path(partition.uri + ".pid").write_text(str(os.getpid()))
    time.sleep(partition.n_entries / 1000.0)
    return np.ones(4)


def add(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    return cast(np.ndarray, a + b)


def zeros() -> np.ndarray:
    return np.zeros(4)


def pid_plan(directory: str, weights_ms: tuple[int, ...]) -> Plan[np.ndarray]:
    tasks = tuple(
        Task(k, Partition(f"{directory}/leaf{k}", "Events", 0, w)) for k, w in enumerate(weights_ms)
    )
    return Plan(process=record_pid, combine=add, empty=zeros, tasks=tasks)
