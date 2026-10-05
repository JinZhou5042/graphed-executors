"""m69a: the 2024 golden and jet-ID JSONs placed where the original reads them, once per session."""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest


@pytest.fixture(scope="session", autouse=True)
def placed() -> dict[str, Path]:
    result: dict[str, Path] = importlib.import_module("hgg_harness").place_higgs_dna_data()
    return result
