"""m69b: the driver check on a dask runner, whose backend hosts nothing on a cluster. A managed service that fits
``driver_memory_mb`` starts beside the driver and serves tasks the dask workers run; one that does not, or one
above the host's physical memory when the backend sets no limit, is refused before it starts, naming both
sizes. Runs in the ``test-dask`` job (``importorskip("distributed")`` in the fixture, so the file collects
anywhere)."""

from __future__ import annotations

import logging
import socket
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import pytest
from graphed.core.execution import Partition, Plan, Task
from graphed.services import Launch, ServiceSpec, split_endpoint

from graphed_executors.submit.services import ServiceUnavailable, physical_memory_mb

# touches its marker, then listens: the marker is the witness that the service started
LISTEN = (
    "import pathlib, socket, sys, time; pathlib.Path(sys.argv[3]).touch(); "
    "s = socket.create_server((sys.argv[1], int(sys.argv[2]))); time.sleep(120)"
)


@pytest.fixture(scope="module")
def client() -> Iterator[Any]:
    distributed = pytest.importorskip("distributed")
    with (
        distributed.LocalCluster(
            n_workers=2, threads_per_worker=1, processes=False, dashboard_address=None
        ) as cluster,
        distributed.Client(cluster) as c,
    ):
        yield c


@pytest.fixture
def runner(client: Any) -> Iterator[Any]:
    from graphed_executors.dask_backend import dask_runner  # noqa: PLC0415  (needs distributed)

    made = dask_runner(client)
    yield made
    made.close()


@dataclass(frozen=True)
class Dial:
    """A task connects to the bound listener and names the dask worker it ran on."""

    endpoint: str | None = None

    def bind_services(self, endpoints: Mapping[str, str]) -> Dial:
        return replace(self, endpoint=endpoints["listener"])

    def __call__(self, partition: Partition, resources: object) -> tuple[str, ...]:
        import distributed  # noqa: PLC0415

        assert self.endpoint is not None
        host, port = split_endpoint(self.endpoint)[1].rsplit(":", 1)
        socket.create_connection((host, int(port)), timeout=5).close()
        return (distributed.get_worker().address,)


def cat(a: tuple[str, ...], b: tuple[str, ...]) -> tuple[str, ...]:
    return a + b


def none() -> tuple[str, ...]:
    return ()


def plan(memory_mb: int, marker: Path) -> Plan[tuple[str, ...]]:
    argv = ("{python}", "-c", LISTEN, "{host}", "{port}", str(marker))
    spec = ServiceSpec(
        "listener",
        "m69b",
        check="tcp",
        launch=Launch(argv, resources={"memory_mb": memory_mb}),
        timeout_s=30.0,
    )
    tasks = tuple(Task(i, Partition(f"mem://m69b-dask/{i}", "", i, i + 1)) for i in range(4))
    return Plan(process=Dial(), combine=cat, empty=none, tasks=tasks, services=(spec,))


def test_a_service_that_fits_driver_memory_starts_beside_the_driver(
    runner: Any, client: Any, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    runner.backend.driver_memory_mb = 64
    marker = tmp_path / "started"
    with caplog.at_level(logging.INFO, logger="graphed_executors.services"):
        value = runner.run(plan(64, marker)).value
    assert marker.exists()
    assert len(value) == 4 and set(value) <= set(client.scheduler_info()["workers"]), value
    statuses = [r.status for r in caplog.records if hasattr(r, "status")]
    assert [(s.leg, s.host) for s in statuses] == [("managed", "driver")]


def test_a_service_above_driver_memory_is_refused_before_it_starts(runner: Any, tmp_path: Path) -> None:
    runner.backend.driver_memory_mb = 64
    marker = tmp_path / "started"
    with pytest.raises(ServiceUnavailable) as err:
        runner.run(plan(1024, marker))
    managed = err.value.legs["managed"]
    assert "1024 MiB" in managed and "driver_memory_mb of 64 MiB" in managed, managed
    assert not marker.exists()


def test_without_a_limit_the_host_s_physical_memory_is_the_limit(runner: Any, tmp_path: Path) -> None:
    host = physical_memory_mb()
    marker = tmp_path / "started"
    with pytest.raises(ServiceUnavailable) as err:
        runner.run(plan(host + 1, marker))
    managed = err.value.legs["managed"]
    assert f"{host + 1} MiB" in managed and f"physical memory of {host} MiB" in managed, managed
    assert not marker.exists()
