"""M40 (B5) — bounded per-worker memory while joining a dest_pid strictly larger than the budget,
including OUTPUT-side duplication (plan §4.2 L629-642, trap 2, ADV-r5.2).

Relational duplication repeats probe columns k times per match, so a SINGLE hot key (all rows key=7 →
one dest) with ``n_left · n_right`` build/probe rows emits ``n_left*n_right`` OUTPUT rows — a joined
dest_pid far larger than any input-derived budget. The kernel must therefore radix-sub-partition and
SPILL (whole partitions to the node-local Store) and STREAM the duplicated output, keeping its live
working set within ``mem_budget_bytes`` — the budget covers OUTPUT too. A full-RAM ``concat`` that
materialises the whole dest in memory blows the budget and fails.

Witnesses:
- (kernel working set) ``witness.peak_join_bytes <= mem_budget_bytes`` — the kernel-measured live
  working set, the direct mirror of M39 ``peak_writer_buffer_bytes`` / M38 ``max_frontier`` (a
  counter, so it is deterministic and CI-stable per R0.10a — a wall-clock/RSS gate would be flaky, and
  the joined result is materialised in ``.value`` so whole-process RSS cannot isolate the kernel);
- (spill engaged, non-vacuous) ``witness.join_spilled_partitions > 0``;
- (real duplication) ``witness.join_output_rows == n_left * n_right`` AND the content equals the
  duplicating oracle — a dedup / grouped impl emits fewer rows and fails.
"""

from __future__ import annotations

import pytest
from join_backends import (
    JoinAdapter,
    adapters,
    expected_join_all,
    observed_join_all,
    one_dest_sides,
)

from graphed_exec_local.shuffle import run_join

ADAPTERS = adapters()

_N_LEFT = 200
_N_RIGHT = 200  # -> 40_000 duplicated output rows in a single dest


@pytest.fixture(params=ADAPTERS, ids=[a.name for a in ADAPTERS])
def adapter(request) -> JoinAdapter:  # type: ignore[no-untyped-def]
    return request.param


def test_join_spills_and_streams_within_a_budget_smaller_than_the_output(
    adapter: JoinAdapter, tmp_path
) -> None:  # type: ignore[no-untyped-def]
    be = adapter.backend
    left, right = one_dest_sides(adapter, _N_LEFT, _N_RIGHT)
    parts = 4
    oracle = expected_join_all(adapter, left, right, parts)
    assert sum(oracle.values()) == _N_LEFT * _N_RIGHT, "the scenario must duplicate to n_left*n_right rows"

    # a per-output-row byte estimate from the oracle-sized output: build a one-row joined-shaped block by
    # merging is not available yet, so bound the output from the input row width (key+lval+rval).
    row_bytes = be.estimated_bytes(left[0]) // _N_LEFT + be.estimated_bytes(right[0]) // _N_RIGHT
    output_bytes = _N_LEFT * _N_RIGHT * row_bytes
    budget = output_bytes // 8  # strictly smaller than the combined joined dest_pid (trap 2)
    assert budget < output_bytes, "the budget must be smaller than the joined dest_pid it covers"

    res = run_join(
        be, left, right, parts, broadcast=False, workers=2, store_root=tmp_path, mem_budget_bytes=budget
    )

    w = res.witness
    # (kernel working set) the streaming/spill gate — a full-RAM concat reports peak == output > budget.
    assert w.peak_join_bytes <= budget, (
        "the join kernel must keep its working set within the budget (spill+stream)"
    )
    assert w.join_spilled_partitions > 0, "spill must actually engage (else the budget was never exercised)"
    # (real duplication) every matching pair produced a row; content equals the duplicating oracle.
    assert w.join_output_rows == _N_LEFT * _N_RIGHT, "relational duplication must emit n_left*n_right rows"
    assert observed_join_all(adapter, res.value) == oracle
