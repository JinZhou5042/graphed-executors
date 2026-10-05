"""m69a: ``compare_part`` is silent on an identical part and names each kind of change by its own leg."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import hgg_harness as h
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

FLOAT_COL = "PTJ0"
INT_COL = "NJ"


@pytest.fixture(scope="module")
def oracle(tmp_path_factory: pytest.TempPathFactory) -> tuple[h.Part, Path]:
    out = tmp_path_factory.mktemp("oracle")
    part = h.oracle_parts(str(h.MC_FIXTURE), "MC", h.YEAR, [(0, 100)], out)[(0, 100)]
    (path,) = out.rglob("*.parquet")
    assert part[1].num_rows > 3
    assert pa.types.is_float64(part[1].schema.field(FLOAT_COL).type)
    assert pa.types.is_int64(part[1].schema.field(INT_COL).type)
    return part, path


def legs(diffs: list[str]) -> set[str]:
    return {d.split(":")[0].split(" ")[0] for d in diffs}


def _set(table: pa.Table, name: str, column: pa.Array) -> pa.Table:
    i = table.column_names.index(name)
    return table.set_column(i, table.schema.field(name).with_type(column.type), column)


def ulp(t: pa.Table) -> pa.Table:
    v = t.column(FLOAT_COL).to_numpy().copy()
    v[0] = np.nextafter(v[0], np.inf)
    return _set(t, FLOAT_COL, pa.array(v))


def narrow(t: pa.Table) -> pa.Table:
    return _set(t, INT_COL, pc.cast(t.column(INT_COL), pa.int32()).combine_chunks())


def nullify(t: pa.Table) -> pa.Table:
    v = t.column(INT_COL).to_numpy()
    return _set(t, INT_COL, pa.array(v, mask=np.arange(len(v)) == 3))


def swap(t: pa.Table) -> pa.Table:
    names = t.column_names
    return t.select([names[1], names[0], *names[2:]])


def kv(t: pa.Table) -> pa.Table:
    md = dict(t.schema.metadata)
    md[b"sum_genw_presel"] += b"0"
    return t.replace_schema_metadata(md)


def test_an_oracle_part_equals_itself_reread_from_disk(oracle: tuple[h.Part, Path]) -> None:
    (counters, table), path = oracle
    assert h.compare_part((counters, table), (counters, pq.read_table(path))) == []


def test_a_one_ulp_change_is_caught_by_the_values_leg(oracle: tuple[h.Part, Path]) -> None:
    (counters, table), _ = oracle
    assert h.compare_part((counters, table), (counters, ulp(table))) == [f"values {FLOAT_COL}"]


def test_a_dropped_row_is_caught_by_validity_and_values_in_every_column(oracle: tuple[h.Part, Path]) -> None:
    (counters, table), _ = oracle
    diffs = h.compare_part((counters, table), (counters, table.slice(1)))
    assert legs(diffs) == {"validity", "values"}
    for name in table.column_names:
        assert f"values {name}" in diffs
        assert any(d.startswith(f"validity {name}:") for d in diffs), name


def test_two_swapped_columns_are_caught_by_the_schema_leg(oracle: tuple[h.Part, Path]) -> None:
    (counters, table), _ = oracle
    diffs = h.compare_part((counters, table), (counters, swap(table)))
    assert legs(diffs) == {"schema"}
    (entry,) = diffs
    assert table.column_names[0] in entry and table.column_names[1] in entry


def test_int64_narrowed_to_int32_with_equal_values_is_caught_by_the_schema_leg(
    oracle: tuple[h.Part, Path],
) -> None:
    (counters, table), _ = oracle
    narrowed = narrow(table)
    assert narrowed.column(INT_COL).to_pylist() == table.column(INT_COL).to_pylist()
    diffs = h.compare_part((counters, table), (counters, narrowed))
    assert legs(diffs) == {"schema", "values"}
    (entry,) = [d for d in diffs if d.startswith("schema")]
    assert f"{INT_COL}: int32" in entry


def test_one_value_set_null_is_caught_by_the_validity_leg(oracle: tuple[h.Part, Path]) -> None:
    (counters, table), _ = oracle
    nulled = nullify(table)
    assert nulled.schema.equals(table.schema) and nulled.column(INT_COL).null_count == 1
    diffs = h.compare_part((counters, table), (counters, nulled))
    assert legs(diffs) == {"validity", "values"}
    assert any(d.startswith(f"validity {INT_COL}:") for d in diffs)


def test_one_kv_value_changed_is_caught_by_the_metadata_leg(oracle: tuple[h.Part, Path]) -> None:
    (counters, table), _ = oracle
    diffs = h.compare_part((counters, table), (counters, kv(table)))
    assert legs(diffs) == {"metadata"}


def _counter(counters: h.Counters, change: Callable[[Any], Any]) -> h.Counters:
    changed = {d: dict(inner) for d, inner in counters.items()}
    changed["MC"]["nTot"] = change(changed["MC"]["nTot"])
    return changed


@pytest.mark.parametrize("change", [lambda n: n + 1, float], ids=["off-by-one", "int-as-float"])
def test_a_changed_counter_is_caught_by_the_counters_leg(
    oracle: tuple[h.Part, Path], change: Callable[[Any], Any]
) -> None:
    (counters, table), _ = oracle
    assert legs(h.compare_part((counters, table), (_counter(counters, change), table))) == {"counters"}
