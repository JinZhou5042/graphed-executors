"""Backend-generic engines shared by the dask and parsl adapters (plan §1.6, bootstrapped in m46).

``common/`` imports stdlib, ``graphed.*``, ``graphed_executors.local`` and
``graphed_executors.submit`` ONLY — never parsl, dask, or distributed (importable with none of the
three installed; frozen no-cross-import test). It hosts:

- :mod:`~graphed_executors.common.tasks_engine` — the m43 as-tasks engine moved verbatim; the
  ``dask_backend.shuffle`` re-export shim keeps every frozen m43/m45 dotted path alive.
- :mod:`~graphed_executors.common.relay_engine` — the m46 relay (as-tasks/workflow) engine: T maps
  + driver-local regroup + P gathers, the honest head-node shape for a broker without
  worker<->worker reachability (e.g. parsl HTEX).

It also holds :func:`refuse_staged`, the one refusal of a join or repartition plan by the runners
that take a single-stage ``Plan``.
"""

from __future__ import annotations

from graphed.core import DurablePlanV2


def refuse_staged(plan: object, runner: str, backend: str) -> None:
    """Refuse a ``DurablePlanV2`` in ``runner``, naming the ``SubmitRunner`` over ``backend`` that runs it."""
    if isinstance(plan, DurablePlanV2):
        raise TypeError(
            f"{runner} runs a single-stage Plan; a join or repartition plan (DurablePlanV2) runs "
            f"through SubmitRunner({backend})"
        )
