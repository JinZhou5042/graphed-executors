"""m68c §6 on dask: ``dask_runner`` runs a service join plan stage by stage over a process ``LocalCluster``;
the peer transport runner refuses a join plan."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest

pytest.importorskip("distributed")

import distributed
from m68c_harness import StandIn, closing, given_spec, join_v2, run_bounded, sequential

from graphed_executors.dask_backend import dask_runner
from graphed_executors.dask_backend.transport_peer import transport_run_plan


@pytest.fixture(scope="module")
def client() -> Iterator[Any]:
    with (
        distributed.LocalCluster(
            n_workers=2, threads_per_worker=1, processes=True, dashboard_address=":0"
        ) as cluster,
        distributed.Client(cluster) as c,
    ):
        yield c


def test_dask_runs_a_given_endpoint_join_equal_to_the_sequential_runner(client: Any) -> None:
    plan = join_v2("dask", spec=given_spec())
    with StandIn() as server:
        with closing(dask_runner(client, services={"sf": server.endpoint()})) as runner:
            got = run_bounded(lambda: runner.run(plan))
        calls = server.calls()
        ref = sequential(plan, {"sf": server.endpoint()})
    assert calls > 0, "no stage task called the server"
    assert got.value == ref.value and got.value, (got.value, ref.value)


def test_the_dask_peer_transport_refuses_a_join_plan_naming_submit_runner() -> None:
    plan = join_v2("refuse-dask", spec=given_spec())
    with pytest.raises(TypeError) as excinfo:
        run_bounded(lambda: transport_run_plan(plan, None))
    assert "SubmitRunner(" in str(excinfo.value) and "DurablePlanV2" in str(excinfo.value), excinfo.value
