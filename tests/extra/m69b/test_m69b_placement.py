"""m69b placement paths the frozen suite does not reach: ``service_hosts`` narrows an attached backend's hosts
in the row's order and gates the cluster capability with them, a driver job's hosts narrow the same way, a
refusal comes before the task server binds, and the engine's managed leg reads the narrowed tuple. No
bindings and no pool: the schedd is a stand-in that accepts a submit and lists no job, the launcher one that
starts nothing."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from graphed.services import Launch, ServiceSpec

from graphed_executors.htcondor_backend import SITES, CondorPilots, HTCondorBackend, launch
from graphed_executors.htcondor_backend import backend as backend_mod
from graphed_executors.submit.services import ServiceSet, ServiceUnavailable


def bindings() -> Any:
    """``htcondor2`` as ``CondorPilots`` starts and stops one cluster on a schedd that lists no job."""
    schedd = SimpleNamespace(
        submit=lambda desc, count=0, spool=False: SimpleNamespace(cluster=lambda: 77),
        query=lambda constraint="", projection=None: [],
    )
    return SimpleNamespace(param={"SCHEDD_HOST": "s1"}, Submit=dict, Schedd=lambda ad=None: schedd)


class NoPilots:
    """A launcher that starts nothing; ``profile``-less, so the backend takes the generic row."""

    def start(self, url: str, secret: bytes, n: int) -> None:
        pass

    def alive(self) -> int:
        return 0

    def stop(self) -> None:
        pass


@pytest.mark.parametrize(
    ("narrowed", "hosts"),
    [
        (None, ("driver", "cluster")),
        (("cluster",), ("cluster",)),
        (("driver",), ("driver",)),
        (("cluster", "driver"), ("driver", "cluster")),
        ((), ()),
    ],
)
def test_service_hosts_narrows_the_hosts_and_the_cluster_capability(
    narrowed: tuple[str, ...] | None, hosts: tuple[str, ...], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(launch, "_htcondor", bindings)
    backend = HTCondorBackend(
        CondorPilots("generic", log_dir=tmp_path), 1, host="127.0.0.1", service_hosts=narrowed
    )
    try:
        assert backend.service_hosts == hosts
        assert callable(getattr(backend, "host_service", None)) is ("cluster" in hosts)
    finally:
        backend.close()


def test_a_host_the_row_does_not_offer_is_refused_before_the_task_server_binds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def bound(*_args: Any) -> None:
        raise AssertionError("the task server started before the refusal")

    monkeypatch.setattr(backend_mod, "TaskServer", bound)
    with pytest.raises(ValueError, match=r"'gpu-node'.*site 'generic'.*\('driver', 'cluster'\)"):
        HTCondorBackend(NoPilots(), 1, host="127.0.0.1", service_hosts=("gpu-node",))
    with pytest.raises(ValueError, match=r"'cluster'.*a driver job.*\('driver',\)"):
        HTCondorBackend(NoPilots(), 1, host="127.0.0.1", in_job=SITES["generic"], service_hosts=("cluster",))


def test_a_driver_job_narrowed_to_no_host_keeps_its_own_row_otherwise() -> None:
    backend = HTCondorBackend(NoPilots(), 1, host="127.0.0.1", in_job=SITES["lpc"], service_hosts=())
    try:
        assert backend.service_hosts == ()
        assert backend.site_services == SITES["lpc"].services
        assert backend.service_ports is None
    finally:
        backend.close()


def test_the_managed_leg_reads_the_narrowed_hosts() -> None:
    spec = ServiceSpec("narrowed", kind="m69b", check="tcp", launch=Launch(("{python}", "-c", "pass")))
    backend = HTCondorBackend(NoPilots(), 1, host="127.0.0.1", service_hosts=("cluster",))
    try:
        with pytest.raises(ServiceUnavailable) as err:
            ServiceSet([spec], backend).start()
    finally:
        backend.close()
    managed = err.value.legs["managed"]
    assert "service_hosts=('cluster',)" in managed and "has no host_service" in managed, managed
