"""Per-site HTCondor submit profiles and the schedd choice, as data: nothing here imports the bindings.

A :class:`SiteProfile` holds the submit keys a site needs (templates over ``{image}``, ``{uid}``,
``{user}`` and ``{home}``), whether its schedd needs spooled sandboxes, whether pilots get the driver's
venv shipped as ``env.tgz``, the directory trees its schedd and its jobs read, how to find a schedd, the
driver-side ports its execute nodes can reach, and the services the site hosts (kind -> endpoint), the
second leg of a run's service set.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, fields
from types import MappingProxyType
from typing import Any

from graphed.services import split_endpoint

# The attributes schedd_weight reads, projected in the collector query.
WEIGHT_ATTRS = ("Name", "RecentDaemonCoreDutyCycle", "ShadowsRunning", "MaxJobsRunning", "TotalIdleJobs")

_SPOOLING_INPUT = 16  # HoldReasonCode of a spooled job while its input sandbox uploads: not a real hold


@dataclass(frozen=True)
class SiteProfile:
    """``schedd_query`` is ``(param naming the collectors, constraint)``; ``None`` means the user's own
    ``SCHEDD_HOST``. ``sandbox_root`` (a template over ``{user}``) is the only tree the site's schedd
    reads, so ``log_dir`` must lie under it. ``driver_ports`` is the inclusive range the task server
    binds on the submit host. ``service_ports`` is the range of login-node ports execute nodes reach for
    a service beside the driver, ``worker_ports`` the range one execute node reaches on another; ``None``
    means no such port was measured open. ``jobs_can_submit`` is whether a job there may submit jobs.
    ``job_root`` is the tree the schedd and every job read and write directly, so a job-submitted
    cluster's ``initialdir`` and a DAG's directory and inputs must lie under it (``"/"``: every path;
    ``None``: no such tree).
    ``services`` maps a service ``kind`` to the endpoint (``scheme://host:port``) the site hosts it
    at; a value that is not one fails construction naming the row."""

    name: str
    submit: Mapping[str, str]
    spool: bool
    ship_env: bool
    sandbox_root: str | None
    schedd_query: tuple[str, str] | None
    driver_ports: tuple[int, int] = (10000, 10100)
    service_ports: tuple[int, int] | None = None
    worker_ports: tuple[int, int] | None = None
    jobs_can_submit: bool = True
    job_root: str | None = None
    services: Mapping[str, str] = field(default_factory=lambda: MappingProxyType({}))

    def __post_init__(self) -> None:
        for kind, endpoint in self.services.items():
            try:
                split_endpoint(endpoint)
            except ValueError as exc:
                raise ValueError(f"site {self.name!r}: services[{kind!r}]: {exc}") from None
        object.__setattr__(self, "services", MappingProxyType(dict(self.services)))

    def __reduce__(self) -> tuple[Any, ...]:
        # a mappingproxy neither pickles nor deep-copies; the constructor re-wraps the plain dict (as
        # graphed's Launch does)
        values = [getattr(self, f.name) for f in fields(self)]
        return (type(self), tuple(dict(v) if isinstance(v, MappingProxyType) else v for v in values))

    @property
    def service_hosts(self) -> tuple[str, ...]:
        """Where a managed service may run: beside the driver, on a cluster node, in that order."""
        return (*(("driver",) if self.service_ports else ()), *(("cluster",) if self.worker_ports else ()))


SITES: Mapping[str, SiteProfile] = {
    "lpc": SiteProfile(
        name="lpc",
        submit={
            "use_x509userproxy": "true",
            # without an explicit path the LPC schedd looks in /tmp and fails with "unable to read proxy"
            "x509userproxy": "{home}/x509up_u{uid}",
            "+DesiredOS": '"EL9"',
            "MY.SingularityImage": '"{image}"',
        },
        spool=True,
        ship_env=True,
        sandbox_root="/uscmst1b_scratch/lpc1/3DayLifetime/{user}",
        schedd_query=(
            "FERMIHTC_REMOTE_POOL",
            'FERMIHTC_DRAIN_LPCSCHEDD=?=FALSE && FERMIHTC_SCHEDD_TYPE=?="CMSLPC" && MaxJobsRunning!=0',
        ),
        driver_ports=(10000, 10100),
        # the task server binds driver_ports first, so a service scanning from 10001 cannot take its port
        service_ports=(10001, 10100),
        worker_ports=(10000, 10100),
        jobs_can_submit=False,
        # the EAF Triton, reached over gRPC+TLS from a batch worker (P8)
        services={"triton": "grpcs://triton.fnal.gov:443"},
    ),
    "lxplus": SiteProfile(
        name="lxplus",
        submit={
            "MY.SingularityImage": '"{image}"',
            "MY.SendCredential": "True",
            "+JobFlavour": '"longlunch"',
        },
        spool=True,
        ship_env=True,
        sandbox_root=None,
        schedd_query=None,
        # batch nodes are refused on 10000 at a login node; CERN opens 8786 there for a dask scheduler
        driver_ports=(8786, 8786),
        # 8786 is the only login port open to batch nodes, and the task server holds it
        service_ports=None,
        worker_ports=(10000, 10100),
        job_root="/afs",
    ),
    "generic": SiteProfile(
        name="generic",
        submit={},
        spool=False,
        ship_env=False,
        sandbox_root=None,
        schedd_query=None,
        driver_ports=(10000, 10100),
        service_ports=(10000, 10100),
        worker_ports=(10000, 10100),
        job_root="/",  # the one-host pool
    ),
}


def schedd_weight(ad: Mapping[str, Any]) -> float:
    """The LPC ``condor_submit`` wrapper's load score; lower is better."""
    return float(
        0.7 * ad["RecentDaemonCoreDutyCycle"] * 100
        + 0.2 * ad["ShadowsRunning"] / ad["MaxJobsRunning"] * 100
        + 0.1 * ad["TotalIdleJobs"]
    )


def choose_schedd(ads: list[Mapping[str, Any]]) -> str:
    return str(min(ads, key=schedd_weight)["Name"])


def counts_as_alive(ad: Mapping[str, Any]) -> bool:
    """Idle, running, or held only while its spooled input uploads."""
    status = ad.get("JobStatus")
    return status in (1, 2) or (status == 5 and ad.get("HoldReasonCode") == _SPOOLING_INPUT)
