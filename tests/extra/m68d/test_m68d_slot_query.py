"""The pool's slot ads (A2's match): the collector drops the dynamic slots and returns whole ads, so a pool
whose dynamic slots outnumber the rest is never pulled through the driver."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from graphed_executors.htcondor_backend import launch
from graphed_executors.htcondor_backend.services import machine_ads

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "frozen" / "m68d"))
from m68d_harness import FAKE_POOL, FAKE_SCHEDD, FakeHTCondor, NodeSchedd, record_bindings

SLOTS = 'MyType == "Machine" && SlotType =!= "Dynamic"'


def test_the_slot_query_leaves_the_dynamic_slots_to_the_schedd_s_collector(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = record_bindings(monkeypatch, FakeHTCondor(NodeSchedd()))
    got = machine_ads((FAKE_POOL, FAKE_SCHEDD))
    assert [e for e in fake.log if e[0] == "collector-query"] == [
        ("collector-query", FAKE_POOL, "None", SLOTS)
    ]
    assert [ad["Name"] for ad in got] == [ad["Name"] for ad in fake.machines], got


@pytest.mark.parametrize(
    ("slot", "kept"),
    [("Partitionable", True), ("Static", True), (None, True), ("Dynamic", False)],
)
def test_the_slot_constraint_keeps_every_slot_but_a_dynamic_one(
    slot: str | None, kept: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    classad2: Any = pytest.importorskip(
        "classad2", reason="ships with the htcondor bindings (Linux wheels only)"
    )
    asked: list[str] = []

    def query(constraint: str) -> list[Any]:  # no projection: the match reads whole slot ads
        asked.append(constraint)
        return []

    collector = SimpleNamespace(query=query)
    monkeypatch.setattr(launch, "_htcondor", lambda: SimpleNamespace(Collector=lambda *pool: collector))
    machine_ads(None)
    ad = classad2.ClassAd({"MyType": "Machine", **({"SlotType": slot} if slot else {})})
    assert len(asked) == 1 and classad2.ExprTree(asked[0]).eval(ad) is kept, (asked, slot)
