"""m66: the driver-side port range is site data (lxplus batch nodes reach only 8786 on a login node)."""

from __future__ import annotations

from typing import Any

import pytest

from graphed_executors.htcondor_backend import (
    SITES,
    CondorPilots,
    HTCondorBackend,
    LocalPilots,
    htcondor_runner,
)

LXPLUS_PORTS = (8786, 8786)
OTHER_PORTS = (10000, 10100)


class LxplusLocal(LocalPilots):
    profile = SITES["lxplus"]


def bound_port(url: str) -> int:
    return int(url.rsplit(":", 1)[1])


def test_the_lxplus_profile_drives_the_driver_port() -> None:
    assert SITES["lxplus"].driver_ports == LXPLUS_PORTS
    backend = HTCondorBackend(LxplusLocal(), 0, host="127.0.0.1")
    try:
        assert bound_port(backend._server.url) == 8786
    finally:
        backend.close()


class BindingsReached(Exception):
    pass


@pytest.mark.parametrize(("port_range", "expected"), [(None, LXPLUS_PORTS), (OTHER_PORTS, OTHER_PORTS)])
def test_htcondor_runner_binds_the_site_ports_unless_overridden(
    port_range: tuple[int, int] | None, expected: tuple[int, int], monkeypatch: pytest.MonkeyPatch
) -> None:
    assert (
        not OTHER_PORTS[0] <= SITES["lxplus"].driver_ports[0] <= OTHER_PORTS[1]
    )  # the override beats a real default
    seen: list[str] = []

    def prepare(self: Any, url: str, secret: bytes) -> None:
        seen.append(url)
        raise BindingsReached

    monkeypatch.setattr(CondorPilots, "prepare", prepare)
    with pytest.raises(BindingsReached):
        htcondor_runner(n_pilots=1, site="lxplus", image="img", host="127.0.0.1", port_range=port_range)
    assert expected[0] <= bound_port(seen[0]) <= expected[1], seen


def test_a_bind_failure_names_the_site_and_the_range() -> None:
    first = HTCondorBackend(LxplusLocal(), 0, host="127.0.0.1")
    try:
        with pytest.raises(OSError, match=r"site=lxplus ports=8786-8786"):
            HTCondorBackend(LxplusLocal(), 0, host="127.0.0.1")
    finally:
        first.close()
