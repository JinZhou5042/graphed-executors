"""m69b session fixtures for the H->gg files: the higgs_dna data placed, the varied-weight MC fixture and
the oracle's parts. Each imports the coffea stack only when a test asks for it, so the files that need
no coffea collect and run without it."""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any

import pytest


@pytest.fixture(scope="session")
def placed() -> dict[str, Path]:
    result: dict[str, Path] = importlib.import_module("hgg_harness").place_higgs_dna_data()
    return result


@pytest.fixture(scope="session")
def fixtures(placed: dict[str, Path], tmp_path_factory: pytest.TempPathFactory) -> dict[str, Path]:
    """The two datasets: the MC fixture with varied ± genWeight, and m69a's data fixture."""
    m = importlib.import_module("m69b_hgg")
    varied: Path = m.build_varied_mc(tmp_path_factory.mktemp("varied"))
    return {"MC": varied, "DataC_2024": m.h.DATA_FIXTURE}


@pytest.fixture(scope="session")
def oracle(fixtures: dict[str, Path], tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    """The original processor's ``{dataset: {(start, stop): (counters, table)}}`` at the m69b ranges."""
    m = importlib.import_module("m69b_hgg")
    out = tmp_path_factory.mktemp("oracle")
    return {ds: m.h.oracle_parts(str(uri), ds, m.h.YEAR, m.RANGES, out / ds) for ds, uri in fixtures.items()}


@pytest.fixture(scope="session")
def analysis(placed: dict[str, Path]) -> Any:
    return importlib.import_module("analysis")
