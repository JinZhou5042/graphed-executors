"""Module-level plan pieces for the executor tests (workers import this module, so no closures)."""

import numpy as np
from graphed.core.execution import Partition, Plan, StopCondition, Task

OPEN_COUNT = {"n": 0}


def width(partition, resources):
    return partition.entry_stop - partition.entry_start


def blind_width(partition, resources):
    return partition.resolve(10).n_entries


def huge_first(partition, resources):
    return np.array([1e16 if partition.entry_start == 0 else 1.0])


def opener(uri):
    OPEN_COUNT["n"] += 1
    return uri.upper()


def open_once_label(partition, resources):
    handle = resources.open_once(partition.uri, opener)
    return [f"{handle}:{partition.entry_start}"]


def fail_on_three(partition, resources):
    if partition.entry_start == 3:
        raise ValueError("boom at entry 3")
    return 1


def read_text(partition, resources):
    with open(partition.uri) as f:
        return [f.read().strip()]


def relative_file_plan(names):
    return Plan(
        process=read_text,
        combine=concat,
        empty=empty_list,
        tasks=tuple(Task(i, Partition(n, "", 0, 1)) for i, n in enumerate(names)),
    )


def add(a, b):
    return a + b


def concat(a, b):
    return [*a, *b]


def zero():
    return 0


def zero_array():
    return np.zeros(1, dtype=float)


def empty_list():
    return []


def width_plan(n=10, shuffle=True):
    tasks = [Task(i, Partition("file", "Events", i, i + 1 + i % 3)) for i in range(n)]
    if shuffle:
        tasks = tasks[::-1]
    return Plan(process=width, combine=add, empty=zero, tasks=tuple(tasks))


def float_plan(n=9):
    tasks = tuple(Task(i, Partition("data", "Events", i, i + 1)) for i in range(n))
    return Plan(process=huge_first, combine=add, empty=zero_array, tasks=tasks)


def blind_plan(n_steps=4):
    tasks = tuple(Task(i, Partition.blind("f", "Events", i, n_steps)) for i in range(n_steps))
    return Plan(process=blind_width, combine=add, empty=zero, tasks=tasks)


def label_plan():
    parts = [Partition("alpha", "T", 0, 1), Partition("beta", "T", 1, 2), Partition("alpha", "T", 2, 3)]
    return Plan(
        process=open_once_label,
        combine=concat,
        empty=empty_list,
        tasks=tuple(Task(i, p) for i, p in enumerate(parts)),
    )


def failing_plan(n=6):
    tasks = tuple(Task(i, Partition("f", "Events", i, i + 1)) for i in range(n))
    return Plan(process=fail_on_three, combine=add, empty=zero, tasks=tasks)


class Batches:
    """next_tasks hook: hand out tasks 3 at a time; picklable, keeps its own cursor."""

    def __init__(self, n=9, size=3):
        self.tasks = [Task(i, Partition("f", "Events", 0, 10)) for i in range(n)]
        self.size = size
        self.cursor = 0
        self.seen_durations = []

    def __call__(self, ctx):
        self.seen_durations.append(len(ctx.last_durations))
        batch = self.tasks[self.cursor : self.cursor + self.size]
        self.cursor += self.size
        return batch or None


def adaptive_plan(n=9, size=3, target_events=None):
    stop = StopCondition(target_events=target_events) if target_events else None
    return Plan(process=width, combine=add, empty=zero, next_tasks=Batches(n, size), stop=stop)
