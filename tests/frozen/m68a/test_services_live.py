"""m68a §3.1 live legs: a driver-hosted service over pool pilots, and the CI Triton container by legs 1 and 2.

(a) needs the ``htcondor2`` bindings and a pool (the ``test-htcondor`` job's personal HTCondor), gated as
``tests/frozen/m67/test_driverless_live.py`` gates: skipped where the bindings are not installed, and,
where they are, a missing schedd is a failure naming what is missing. (b) and (c) need a Triton server
serving ``tests/frozen/m68a/data/triton_models`` (``graphed_identity``: INPUT0 -> OUTPUT0 FP32, the name
and I/O the EAF serves, P9) on gRPC only, started with ``recipes.triton``'s flags; they are gated on
``GRAPHED_TRITON_GRPC=host:port`` (unset: skipped), as graphed gates its live Triton tests, and fail when
it is set and the server is not ready. The ``test-htcondor`` CI step must export
``GRAPHED_TRITON_GRPC=localhost:8001``.

(a) every probe answer carries the pool host's ``Machine``, equal to ``backend.host_identity()``, so the
probe passes; a pilot task GETs the service; its port is free when ``run`` returns. (b) the ``grpc:``
check and the probe pass on the container and the §3 External params give output == input; the port
answers only gRPC, and the reduce reports ``tritonclient.grpc`` loaded in the worker. (c) a profile copy
whose ``services`` names the container resolves the requirement by leg 2, starting nothing.
"""

from __future__ import annotations

import dataclasses
import logging
import os
from pathlib import Path
from typing import Any

import numpy as np
import pytest
from m68a_triton import X, live_identity_plan
from services_harness import (
    HARNESS_DIR,
    HARNESS_FILE,
    Resolved,
    SpyProcess,
    backend_api,
    child_spec,
    endpoint_port,
    free_range,
    htcondor_api,
    is_probe_key,
    launch_api,
    port_free,
    recipes_api,
    require_module,
    run_bounded,
    services_api,
    spy_events,
    spy_plan,
    status_records,
)

POOL_PROBE_S = 30.0
LIVE_S = 300.0
TRITON = os.environ.get("GRAPHED_TRITON_GRPC")
needs_triton = pytest.mark.skipif(
    not TRITON, reason="no Triton container (set GRAPHED_TRITON_GRPC=host:port; the test-htcondor job does)"
)


def require_pool() -> Any:
    """The bindings and a schedd of a reachable pool, or a skip (no bindings) / failure (no pool)."""
    htcondor2: Any = pytest.importorskip(
        "htcondor2",
        reason="the htcondor bindings are not installed (Linux wheels only; the test-htcondor job)",
    )
    try:
        ads = run_bounded(
            lambda: htcondor2.Collector().query(htcondor2.AdType.Schedd, projection=["Name"]), POOL_PROBE_S
        )
    except Exception as exc:
        pytest.fail(f"htcondor2 is installed but no pool answers ({exc}): start a personal HTCondor")
    assert ads, "htcondor2 is installed but the collector lists no schedd: start a personal HTCondor"
    return htcondor2


def require_triton() -> str:
    """``grpc://<GRAPHED_TRITON_GRPC>``, checked ready with the plan's own ``grpc:`` check."""
    assert TRITON
    require_module("grpc")
    endpoint = f"grpc://{TRITON}"
    reason = run_bounded(lambda: services_api().check_ready(endpoint, "grpc:", 30.0), 60.0)
    assert reason is None, f"GRAPHED_TRITON_GRPC={TRITON} is set but the server is not ready: {reason}"
    return endpoint


def record_probe_answers(backend: Any) -> list[tuple[str, Any]]:
    """Patch ``backend.submit`` (on the instance) to keep each probe task's answer; the task itself is
    submitted unchanged, since pilots unpickle it."""
    real = backend.submit
    answers: list[tuple[str, Any]] = []

    def submit(fn: Any, /, *args: Any, key: str, **kwargs: Any) -> Any:
        fut = real(fn, *args, key=key, **kwargs)
        if is_probe_key(key):
            fut.add_done_callback(
                lambda f: (
                    None if f.cancelled() or f.exception() is not None else answers.append((key, f.result()))
                )
            )
        return fut

    backend.submit = submit
    return answers


def test_a_driver_hosted_service_over_pool_pilots(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    require_pool()
    log_dir = tmp_path / "pilots"
    runner = run_bounded(
        lambda: backend_api().htcondor_runner(
            n_pilots=2, site="generic", log_dir=log_dir, user_modules=[HARNESS_FILE], min_pilots=2
        ),
        LIVE_S,
    )
    answers = record_probe_answers(runner.backend)
    report = tmp_path / "child.json"
    spec = child_spec("web", report, ports=free_range(), timeout_s=120.0)
    plan = spy_plan(SpyProcess("live-pool"), 4, [spec])
    try:
        with caplog.at_level(logging.INFO):
            result = run_bounded(lambda: runner.run(plan), LIVE_S)
        identity = run_bounded(runner.backend.host_identity, LIVE_S)
        assert answers, "no probe answered"
        assert all(answer[0] == identity for _key, answer in answers), (identity, answers)
        (status,) = status_records(caplog.records)
        assert (status.leg, status.host, status.identity) == ("managed", "driver", identity)
        assert isinstance(result.value, Resolved) and len(result.value.value) == 4
        (bind,) = spy_events("live-pool", "bind")
        assert port_free(endpoint_port(bind[1]["web"])), "the service's port is still bound after run"
    finally:
        run_bounded(runner.close, LIVE_S)


def _local_runner(profile: Any = None) -> Any:
    """An attached runner over two local pilots; ``profile`` (a ``SiteProfile``) is the launcher's."""
    launch, backend_mod = launch_api(), backend_api()
    pilots = launch.LocalPilots(pythonpath=[HARNESS_DIR])
    if profile is not None:
        pilots.profile = profile
    backend = run_bounded(
        lambda: backend_mod.HTCondorBackend(pilots, 2, host="127.0.0.1", port_range=(0, 0)), LIVE_S
    )
    return backend_mod.HTCondorRunner(backend, min_pilots=2)


@needs_triton
def test_the_triton_container_by_a_given_endpoint(caplog: pytest.LogCaptureFixture) -> None:
    endpoint = require_triton()
    spec = recipes_api().triton("triton", "nvcr.io/nvidia/tritonserver:25.11-pyt-python-py3", "models/")
    plan = live_identity_plan(dataclasses.replace(spec, timeout_s=120.0))
    runner = _local_runner()
    runner.services = {"triton": endpoint}
    try:
        with caplog.at_level(logging.INFO):
            (row,), grpc_used = run_bounded(lambda: runner.run(plan), LIVE_S).value
    finally:
        run_bounded(runner.close, LIVE_S)
    assert np.asarray(row, dtype="float32").tobytes() == X.astype("float32").tobytes()
    assert grpc_used is True, "the rows did not come through tritonclient.grpc"
    (status,) = status_records(caplog.records)
    assert (status.leg, status.endpoint) == ("user", endpoint)


@needs_triton
def test_the_triton_container_by_the_site_leg(caplog: pytest.LogCaptureFixture) -> None:
    endpoint = require_triton()
    profile = dataclasses.replace(htcondor_api().SITES["generic"], services={"triton": endpoint})
    spec = recipes_api().triton("triton", "nvcr.io/nvidia/tritonserver:25.11-pyt-python-py3", "models/")
    plan = live_identity_plan(dataclasses.replace(spec, timeout_s=120.0))
    runner = _local_runner(profile)
    try:
        assert dict(runner.backend.site_services) == {"triton": endpoint}
        with caplog.at_level(logging.INFO):
            (row,), grpc_used = run_bounded(lambda: runner.run(plan), LIVE_S).value
    finally:
        run_bounded(runner.close, LIVE_S)
    assert np.asarray(row, dtype="float32").tobytes() == X.astype("float32").tobytes()
    assert grpc_used is True
    (status,) = status_records(caplog.records)
    assert (status.leg, status.host, status.endpoint) == ("site", None, endpoint)
