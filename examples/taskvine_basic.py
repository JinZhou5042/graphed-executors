"""Run a small graphed Plan through the TaskVine backend."""

from __future__ import annotations

import argparse

from graphed.core import Partition, Plan, Task

from graphed_executors.taskvine_backend import TaskVineExecutor


def count(partition: Partition, resources: object) -> int:
    return partition.entry_stop - partition.entry_start


def add(left: int, right: int) -> int:
    return left + right


def zero() -> int:
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cluster", action="store_true", help="run on a connected vine_worker")
    parser.add_argument("--port", type=int, default=9123, help="manager port in cluster mode")
    args = parser.parse_args()

    plan = Plan(
        process=count,
        combine=add,
        empty=zero,
        tasks=tuple(Task(i, Partition("demo", "", i, i + 1)) for i in range(8)),
    )
    with TaskVineExecutor(
        local=not args.cluster,
        port=args.port if args.cluster else 0,
        libcores=1,
        wait_for_workers=1 if args.cluster else 0,
    ) as executor:
        result = executor.run(plan)
    print(result.value)  # 8


if __name__ == "__main__":
    main()
