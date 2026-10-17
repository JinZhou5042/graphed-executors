"""A plan whose op is a closure, or whose partials carry a backend behavior holding a lambda, runs on the process pool (GW33)."""

from __future__ import annotations

from typing import Any

import awkward as ak
import graphed
import pytest
from _hdna_payloads import (
    EVENTS,
    LAMBDA_BEHAVIOR,
    Events,
    add,
    concat,
    empty_array,
    first,
    make_lambda_behavior_backend,
    scale_by,
    sum_all,
    zero,
)
from graphed import Session
from graphed.aggregate import aggregate_plan
from graphed.awkward import AwkwardBackend, AwkwardForm, gak
from graphed.core.execution import SequentialRunner

import graphed_executors.local.executors as local_executors
from graphed_executors.local import ProcessPoolExecutor


def _events(backend: AwkwardBackend | None = None) -> Any:
    session = Session(backend or AwkwardBackend())
    form = AwkwardForm(ak.Array(EVENTS.layout.to_typetracer(forget_length=True)))
    return session.source("events", form=form, data=Events(EVENTS))


def _closure_plan() -> Any:
    pt = graphed.apply(scale_by(3.0), _events().Jet.pt)
    return aggregate_plan(gak.sum(pt, axis=1), reduce=sum_all, combine=add, empty=zero, steps_per_file=4)


def _behavior_plan() -> Any:
    return aggregate_plan(
        gak.sum(_events(make_lambda_behavior_backend()).Jet.pt, axis=1),
        reduce=first,
        combine=concat,
        empty=empty_array,
        steps_per_file=4,
        backend="_hdna_payloads:make_lambda_behavior_backend",
    )


@pytest.mark.parametrize("comms", [None, "ipc"])
def test_a_closure_op_runs_on_the_process_pool(comms: str | None, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(local_executors, "_PEER_ROOT_TIMEOUT_S", 60.0)
    got = ProcessPoolExecutor(max_workers=2, comms=comms).run(_closure_plan()).value
    assert got == 3.0 * float(ak.sum(EVENTS.Jet.pt))


@pytest.mark.parametrize("comms", [None, "ipc"])
def test_a_lambda_behavior_partial_runs_on_the_process_pool(
    comms: str | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(local_executors, "_PEER_ROOT_TIMEOUT_S", 60.0)
    plan = _behavior_plan()
    got = ProcessPoolExecutor(max_workers=2, comms=comms).run(plan).value
    assert ak.to_list(got) == ak.to_list(SequentialRunner().run(plan).value)
    assert got.behavior is not None and got.behavior["unused"] is LAMBDA_BEHAVIOR["unused"]
    assert sorted(ak.to_list(got)) == sorted(ak.to_list(ak.sum(EVENTS.Jet.pt, axis=1)))
