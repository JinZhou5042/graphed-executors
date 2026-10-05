"""The direct HTCondor backend, behind the ``[htcondor]`` optional extra.

:class:`HTCondorBackend` launches pilot jobs (:class:`CondorPilots` through the ``htcondor2`` bindings, or
:class:`LocalPilots` as local subprocesses) that pull pickled tasks from a driver-side task server.
:func:`submit_driverless` runs a plan from inside one job instead, tracked by a :class:`RunHandle`.
Importing this package imports no bindings: ``CondorPilots.start`` does, on first use.
"""

from __future__ import annotations

from .backend import HTCondorBackend, HTCondorRunner, htcondor_runner
from .driverless import RunHandle, submit_driverless
from .launch import CondorPilots, LocalPilots, PilotLauncher
from .server import WorkerLost
from .sites import SITES, SiteProfile

__all__ = [
    "SITES",
    "CondorPilots",
    "HTCondorBackend",
    "HTCondorRunner",
    "LocalPilots",
    "PilotLauncher",
    "RunHandle",
    "SiteProfile",
    "WorkerLost",
    "htcondor_runner",
    "submit_driverless",
]
