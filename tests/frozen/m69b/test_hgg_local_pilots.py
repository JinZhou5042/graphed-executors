"""m69b: the H->gg plan with histserv diagnostics over the HTCondor backend's pilots, no pool
(plan-services.md, the m69b ``test_hgg_local_pilots.py`` row).

``HTCondorBackend`` over ``LocalPilots`` takes the generic profile's service placement, whose first host is
the driver, so the context's servers start beside the driver; pilot processes fill them concurrently. The
bindings are the m68a recorder (no pool, no ``htcondor2``). The ``test-hgg`` job runs this file with
``GRAPHED_HGG_REQUIRED=1``; elsewhere it skips without coffea, higgs_dna or histserv.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pytest
from m69b_harness import histserv_api, port_free, run_bounded, unique
from m69b_hgg import DIAGNOSTICS, SERVER_MB, close, direct, fileset, h, oracle_totals, totals
from services_harness import (
    RecordingSchedd,
    backend_api,
    endpoint_port,
    launch_api,
    record_bindings,
    status_records,
)

RUN_S = 900.0
PILOTS = 2


def test_local_pilots_fill_servers_beside_the_driver(
    analysis: Any,
    fixtures: dict[str, Path],
    oracle: dict[str, Any],
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # the driver's identity is the bindings' FULL_HOSTNAME, and test-hgg installs no bindings
    record_bindings(monkeypatch, RecordingSchedd())
    ctx = histserv_api().Context(memory_mb=SERVER_MB, workers=PILOTS, name=unique("m69b-hgg-pilots"))
    plan = analysis.plan(fileset(fixtures), year=h.YEAR, out=str(tmp_path), context=ctx)
    pilots = launch_api().LocalPilots(pythonpath=[str(h.EXAMPLE)])
    backend = run_bounded(lambda: backend_api().HTCondorBackend(pilots, PILOTS, host="127.0.0.1"), RUN_S)
    runner = backend_api().HTCondorRunner(backend, min_pilots=PILOTS)
    try:
        with caplog.at_level(logging.INFO, logger="graphed_executors.services"):
            value = run_bounded(lambda: runner.run(plan), RUN_S).value
    finally:
        run_bounded(runner.close, RUN_S)
    statuses = status_records(caplog.records)
    assert sorted(s.name for s in statuses) == sorted(s.name for s in plan.services) != []
    assert {(s.leg, s.host) for s in statuses} == {("managed", "driver")}
    assert all(port_free(endpoint_port(s.endpoint)) for s in statuses), "a server outlived the run"
    for ds, parts in oracle.items():
        want = direct(parts)
        assert set(value[ds]["diagnostics"]) == set(DIAGNOSTICS)
        for name in DIAGNOSTICS:
            assert close(value[ds]["diagnostics"][name], want[name]), (ds, name)
    assert totals(value) == oracle_totals(oracle)
