"""The Ctrl-C row's child (``test_service_order.py``): one served plan under ``with htcondor_runner(...)
as runner:``, cluster-hosted, logging at INFO to stdout.

argv: form (``run``: ``runner.run(plan)``; ``result``: ``runner.submit(plan).result()``; ``close``:
``runner.submit(plan)`` and the block left normally), log dir, server MiB, ``POLL_S``, pilot MiB.
"""

from __future__ import annotations

import importlib
import logging
import sys

from m69b_harness import HARNESS_FILE, histserv_api, served_plan, unique


def main(form: str, log_dir: str, server_mb: str, poll_s: str, pilot_mb: str) -> None:
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(name)s %(message)s")
    importlib.import_module("graphed_executors.htcondor_backend.server").POLL_S = float(poll_s)
    backend = importlib.import_module("graphed_executors.htcondor_backend.backend")
    ctx = histserv_api().Context(memory_mb=int(server_mb), workers=1, name=unique("m69b-order-sigint"))
    plan = served_plan({"h": 8}, ctx)
    with backend.htcondor_runner(
        n_pilots=1,
        site="generic",
        service_hosts=("cluster",),
        log_dir=log_dir,
        request_memory_mb=int(pilot_mb),
        user_modules=[HARNESS_FILE],
    ) as runner:
        if form == "run":
            runner.run(plan)
        elif form == "result":
            runner.submit(plan).result()
        else:
            runner.submit(plan)


if __name__ == "__main__":
    main(*sys.argv[1:])
