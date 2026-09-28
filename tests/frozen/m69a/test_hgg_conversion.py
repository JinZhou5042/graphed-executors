"""m69a/m72: one graphed plan over a fileset (``examples/hgg/analysis.py``) writes every part the original
writes and returns the dataset totals coffea's Runner accumulates, on coffea NanoEvents."""

from __future__ import annotations

import importlib
import os
import re
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType
from typing import Any

import hgg_harness as h
import pyarrow.parquet as pq
import pytest
import uproot
from coffea.nanoevents import NanoEventsFactory
from coffea.nanoevents._graphed import GraphedNanoArray
from coffea.processor import accumulate
from graphed.core import SequentialRunner
from graphed.errors import GraphedTypeError

from graphed_executors.submit import SubmitRunner, ThreadBackend

RANGES = [(0, 100), (100, 200)]
FIXTURES = {"MC": h.MC_FIXTURE, "DataC_2024": h.DATA_FIXTURE}
UPROOT_USE = re.compile(r"import uproot|uproot\.")


def fileset(*datasets: str) -> dict[str, dict[str, Any]]:
    """coffea's ``{dataset: {file: {"object_path", "steps"}}}`` at RANGES."""
    steps = [list(r) for r in RANGES]
    return {ds: {str(FIXTURES[ds]): {"object_path": "Events", "steps": steps}} for ds in datasets}


def part_path(out: Path, dataset: str, start: int, stop: int) -> Path:
    return out / dataset / "nominal" / f"{FIXTURES[dataset].stem}_Events_{start}-{stop}.parquet"


def part_bytes(out: Path) -> dict[str, bytes]:
    return {p.relative_to(out).as_posix(): p.read_bytes() for p in out.rglob("*.parquet")}


@pytest.fixture(scope="module")
def analysis() -> ModuleType:
    return importlib.import_module("analysis")


@pytest.fixture(scope="module")
def oracle(tmp_path_factory: pytest.TempPathFactory) -> dict[str, dict[tuple[int, int], h.Part]]:
    out = tmp_path_factory.mktemp("oracle")
    return {ds: h.oracle_parts(str(uri), ds, h.YEAR, RANGES, out / ds) for ds, uri in FIXTURES.items()}


@pytest.fixture(params=["sequential", "thread-backend"])
def runner(request: pytest.FixtureRequest) -> Iterator[Any]:
    if request.param == "sequential":
        yield SequentialRunner()
        return
    submit = SubmitRunner(ThreadBackend(2))
    try:
        yield submit
    finally:
        submit.close()


def test_the_hand_written_process_and_totals_are_gone(analysis: ModuleType) -> None:
    assert [n for n in ("HggProcess", "totals", "_values", "_union") if hasattr(analysis, n)] == []
    assert callable(analysis.lumi_mask)


def test_one_plan_writes_every_part_and_returns_the_totals(
    runner: Any, analysis: ModuleType, oracle: dict[str, Any], tmp_path: Path
) -> None:
    plan = analysis.plan(fileset(*FIXTURES), year=h.YEAR, out=str(tmp_path))
    value = runner.run(plan).value
    expected = accumulate([counters for parts in oracle.values() for counters, _ in parts.values()])
    assert sorted(value) == sorted(FIXTURES)
    assert value == expected
    for ds, leaves in expected.items():
        assert {k: type(v) for k, v in value[ds].items()} == {k: type(v) for k, v in leaves.items()}
    expected_parts = {part_path(tmp_path, ds, s, e) for ds in FIXTURES for s, e in RANGES}
    assert set(tmp_path.rglob("*.parquet")) == expected_parts
    for ds, parts in oracle.items():
        for (start, stop), (counters, table) in parts.items():
            actual = pq.read_table(part_path(tmp_path, ds, start, stop))
            assert h.compare_part((counters, table), (counters, actual)) == [], (ds, start, stop)


def test_each_dataset_run_on_its_own_collects_into_the_same_product(
    analysis: ModuleType, tmp_path: Path
) -> None:
    one = (
        SequentialRunner()
        .run(analysis.plan(fileset(*FIXTURES), year=h.YEAR, out=str(tmp_path / "one")))
        .value
    )
    submit = SubmitRunner(ThreadBackend(2))
    try:
        futures = [
            submit.submit(analysis.plan(fileset(ds), year=h.YEAR, out=str(tmp_path / "each")))
            for ds in FIXTURES
        ]
        collected: dict[str, Any] = {}
        for future in futures:
            collected |= future.result().value
    finally:
        submit.close()
    assert collected == one
    each = part_bytes(tmp_path / "each")
    assert len(each) == len(FIXTURES) * len(RANGES)
    assert each == part_bytes(tmp_path / "one")


def test_a_file_without_steps_is_refused_at_plan_build(analysis: ModuleType, tmp_path: Path) -> None:
    unstepped = {"DataC_2024": {str(h.DATA_FIXTURE): {"object_path": "Events"}}}
    for bad in (unstepped, {**fileset("MC"), **unstepped}):
        with pytest.raises(ValueError, match="steps"):
            analysis.plan(bad, year=h.YEAR, out=str(tmp_path))
    assert os.listdir(tmp_path) == []


@pytest.fixture
def recorded_events(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    """Every events object a ``NanoEventsFactory`` hands out while the test runs (calls pass through)."""
    seen: list[Any] = []
    events = NanoEventsFactory.events

    def recording(self: NanoEventsFactory) -> Any:
        seen.append(events(self))
        return seen[-1]

    monkeypatch.setattr(NanoEventsFactory, "events", recording)
    return seen


def test_the_events_are_coffea_nanoevents(
    runner: Any, analysis: ModuleType, recorded_events: list[Any], tmp_path: Path
) -> None:
    plan = analysis.plan(fileset(*FIXTURES), year=h.YEAR, out=str(tmp_path))
    assert {events.metadata["dataset"] for events in recorded_events} == set(FIXTURES)
    # both runners execute in-process, so events a task builds eagerly are recorded too
    runner.run(plan)
    for events in recorded_events:
        assert isinstance(events, GraphedNanoArray)
        assert hasattr(events.Photon, "metric_table") and hasattr(events.Photon, "delta_r")
        assert not hasattr(events.Photon, "no_such_method_xyz")
        assert {type(s).__name__ for s in events.session.sources().values()} == {"_GraphedTTreeSource"}
    # the same source node backs a raw uproot array, which has no NanoEvents collections
    raw = uproot.graphed({str(h.MC_FIXTURE): "Events"})
    assert {type(s).__name__ for s in raw.session.sources().values()} == {"_GraphedTTreeSource"}
    with pytest.raises(GraphedTypeError):
        _ = raw.Photon
    # the analysis builds events through coffea, never by reading with uproot itself
    sources = sorted(h.EXAMPLE.rglob("*.py"))
    assert h.EXAMPLE / "analysis.py" in sources
    assert [p.name for p in sources if UPROOT_USE.search(p.read_text())] == []
    assert UPROOT_USE.search((h.DATA / "make_hgg_fixture.py").read_text())
