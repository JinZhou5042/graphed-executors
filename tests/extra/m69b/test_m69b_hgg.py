"""m69b H->gg paths the frozen suite does not reach: ``run_local`` prints the counters and saves the
diagnostics as UHI JSON that reads back bit for bit, and the analysis's empty value is the identity of its
combine. Skips without coffea or higgs_dna unless ``GRAPHED_HGG_REQUIRED=1``, as ``hgg_harness`` does."""

from __future__ import annotations

import importlib
import json
from pathlib import Path
from types import ModuleType
from typing import Any

import boost_histogram as bh
import hgg_harness as h
import numpy as np
import pytest
import uhi.io.json
from graphed.core import SequentialRunner


@pytest.fixture(scope="module")
def modules() -> tuple[ModuleType, ModuleType]:
    h.place_higgs_dna_data()
    return importlib.import_module("analysis"), importlib.import_module("run_local")


def test_run_local_prints_the_counters_and_saves_the_diagnostics_as_uhi_json(
    modules: tuple[ModuleType, ModuleType], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    analysis, run_local = modules
    saved = tmp_path / "diagnostics.json"
    argv = [str(h.MC_FIXTURE), "--dataset", "MC", "--year", h.YEAR, "--parts", "2"]
    run_local.main([*argv, "--out", str(tmp_path / "out"), "--histograms", str(saved)])
    printed = json.loads(capsys.readouterr().out)

    fileset = run_local.fileset([str(h.MC_FIXTURE)], "MC", 2)
    want = SequentialRunner().run(analysis.plan(fileset, year=h.YEAR, out=str(tmp_path / "ref"))).value
    assert printed == {"MC": {k: v for k, v in want["MC"].items() if k != "diagnostics"}}
    read = json.loads(saved.read_text(), object_hook=uhi.io.json.object_hook)
    assert set(read["MC"]) == set(analysis.DIAGNOSTICS)
    for name, hist in want["MC"]["diagnostics"].items():
        back = bh.Histogram(read["MC"][name])
        view = np.asarray(back.view(flow=True))
        assert back.storage_type is bh.storage.Weight and view["value"].any(), name
        assert view.tobytes() == np.asarray(hist.view(flow=True)).tobytes(), name


def test_the_empty_value_is_the_identity_of_the_combine(modules: tuple[ModuleType, ModuleType]) -> None:
    analysis, _ = modules
    combine = analysis.HggCombine(lambda a, b: {k: a[k] + b[k] for k in a})
    empty = analysis.HggEmpty(lambda: {"m_gg": 0.0})
    chunk: dict[str, Any] = {"nTot": 3, "nPos": 2, "nNeg": 1, "nEff": 1, "genWeightSum": 1.5}
    chunk["diagnostics"] = {"m_gg": 2.0}
    assert combine(empty(), chunk) == chunk
    doubled = {"nTot": 6, "nPos": 4, "nNeg": 2, "nEff": 2, "genWeightSum": 3.0, "diagnostics": {"m_gg": 4.0}}
    assert combine(chunk, chunk) == doubled
