"""m69b: the H->gg diagnostics (plan-services.md, the m69b ``test_hgg_diagnostics.py`` row).

``examples/hgg/analysis.py`` always fills the named diagnostics over the selected diphotons; without a
context they are local boost histograms, with one they are histserv histograms on its servers, collated
across both datasets under that one context. The oracle is a direct boost-histogram fill of the original
processor's parts' columns, folded in part order; the MC fixture carries lognormal ± genWeights, so
cancellations and float32 weights reach every bin. The ``test-hgg`` job runs this file with
``GRAPHED_HGG_REQUIRED=1``; elsewhere it skips without coffea, higgs_dna or histserv.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import boost_histogram as bh
import numpy as np
import pytest
from graphed.core import SequentialRunner
from m69b_harness import histserv_api, run_bounded, unique
from m69b_hgg import (
    DIAGNOSTICS,
    RANGES,
    SERVER_MB,
    bitwise,
    close,
    direct,
    fileset,
    h,
    oracle_totals,
    part_path,
    read_part,
    totals,
)

from graphed_executors.submit import SubmitRunner, ThreadBackend

RUN_S = 900.0


def served_run(analysis: Any, fixtures: dict[str, Path], out: Path, workers: int) -> tuple[Any, Any, Any]:
    """(context, plan, value): one plan over both datasets under one context, run on
    ``SubmitRunner(ThreadBackend(workers))`` with its servers managed beside the driver."""
    ctx = histserv_api().Context(memory_mb=SERVER_MB, workers=workers, name=unique("m69b-hgg"))
    plan = analysis.plan(fileset(fixtures), year=h.YEAR, out=str(out), context=ctx)
    runner = SubmitRunner(ThreadBackend(workers))
    try:
        value = run_bounded(lambda: runner.run(plan), RUN_S).value
    finally:
        runner.close()
    return ctx, plan, value


@pytest.fixture(scope="module")
def one_worker(
    analysis: Any, fixtures: dict[str, Path], tmp_path_factory: pytest.TempPathFactory
) -> tuple[Any, Any, Any, Path]:
    out = tmp_path_factory.mktemp("one-worker")
    return (*served_run(analysis, fixtures, out, 1), out)


def test_without_a_context_the_diagnostics_are_local_and_equal_the_one_worker_served_run(
    analysis: Any, fixtures: dict[str, Path], one_worker: tuple[Any, Any, Any, Path], tmp_path: Path
) -> None:
    plan = analysis.plan(fileset(fixtures), year=h.YEAR, out=str(tmp_path))
    assert plan.services == ()
    local = run_bounded(lambda: SequentialRunner().run(plan), RUN_S).value
    _ctx, _plan, served, _out = one_worker
    assert set(analysis.DIAGNOSTICS) == set(DIAGNOSTICS)
    for ds in fixtures:
        assert set(local[ds]["diagnostics"]) == set(DIAGNOSTICS)
        for name in DIAGNOSTICS:
            hist = local[ds]["diagnostics"][name]
            assert isinstance(hist, bh.Histogram), (ds, name, type(hist))
            assert bitwise(served[ds]["diagnostics"][name], hist), (ds, name)


def test_one_context_over_both_datasets_fills_each_diagnostic_as_the_direct_fill(
    fixtures: dict[str, Path], oracle: dict[str, Any], one_worker: tuple[Any, Any, Any, Path]
) -> None:
    counters = oracle_totals({"MC": oracle["MC"]})["MC"]
    assert counters["nNeg"] > 0 and counters["nPos"] > 0, counters
    weights = np.concatenate([t.column("weight").to_numpy() for _, t in oracle["MC"].values()])
    assert weights.dtype == np.float32 and len(np.unique(np.abs(weights))) > len(weights) // 2, weights
    ctx, plan, value, out = one_worker
    assert len(plan.services) == len(ctx.servers()) >= 1
    assert sorted(s.name for s in plan.services) == sorted(s[0] for s in ctx.servers())
    for ds, parts in oracle.items():
        want = direct(parts)
        assert set(value[ds]["diagnostics"]) == set(DIAGNOSTICS)
        for name in DIAGNOSTICS:
            assert bitwise(value[ds]["diagnostics"][name], want[name]), (ds, name)
    assert totals(value) == oracle_totals(oracle)
    for ds, parts in oracle.items():
        for (start, stop), (counters, table) in parts.items():
            actual = read_part(part_path(out, ds, fixtures[ds], start, stop))
            assert h.compare_part((counters, table), (counters, actual)) == [], (ds, start, stop)
    assert len(list(out.rglob("*.parquet"))) == len(oracle) * len(RANGES)


def test_three_workers_under_their_own_context_agree_to_1e_12(
    analysis: Any, fixtures: dict[str, Path], oracle: dict[str, Any], tmp_path: Path
) -> None:
    ctx, plan, value = served_run(analysis, fixtures, tmp_path, 3)
    assert len(plan.services) == len(ctx.servers()) >= 1
    for ds, parts in oracle.items():
        want = direct(parts)
        for name in DIAGNOSTICS:
            assert close(value[ds]["diagnostics"][name], want[name]), (ds, name)
