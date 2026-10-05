"""m68a D7 through the engine: a graphed plan whose Triton External names ``service="triton"`` runs through
``SubmitRunner(ThreadBackend, services={"triton": "grpc://<fake>"})``, and the evaluator connects to the
endpoint the run bound. Every OS; no ``tritonclient``.

The fake transport is graphed's ``fake_triton.py`` copied (``m68a_triton``); its connect log records the
``url`` each evaluator was built with, and a fake "server" answers only at the endpoint it was served
at. The declared spec's ``check`` is ``tcp``, so the harness binds a TCP listener at that endpoint (its
accept counter shows the engine checked and probed it; ``tcp`` passes on every scheme).

Discriminates: a URL-blind cache (two nodes on two services must connect twice), a service name in the
engine (the literal-``url`` plan carries no service and starts no set), and a replaced given endpoint.
"""

from __future__ import annotations

import logging
import secrets
from typing import Any

import numpy as np
import pytest
from m68a_triton import connects, descriptor, expected, serve, triton_service_plan, triton_url_plan
from services_harness import (
    CountingListener,
    RecordingBackend,
    closing_bounded,
    run_bounded,
    status_records,
    submit_api,
)

BOUND_S = 120.0


def runner_over(backend: Any, **kwargs: Any) -> Any:
    return submit_api().SubmitRunner(backend, **kwargs)


def test_a_service_ref_connects_to_the_bound_endpoint(caplog: pytest.LogCaptureFixture) -> None:
    tag = f"one-{secrets.token_hex(4)}"
    with CountingListener() as listener:
        endpoint = listener.endpoint("grpc")
        serve(endpoint, _payload(tag))
        plan = triton_service_plan(tag, [("triton", 0.45, -0.1)])
        assert [s.name for s in plan.services] == ["triton"]
        before = len(connects())
        with (
            caplog.at_level(logging.INFO),
            closing_bounded(
                runner_over(submit_api().ThreadBackend(2), services={"triton": endpoint})
            ) as runner,
        ):
            (row,) = run_bounded(lambda: runner.run(plan), BOUND_S).value
        assert np.allclose(row, expected(0.45, -0.1), rtol=1e-6)
        assert set(connects()[before:]) == {endpoint}, connects()[before:]
        assert listener.settled() >= 1, "the given endpoint was never checked"
    (status,) = status_records(caplog.records)
    assert (status.name, status.leg, status.endpoint) == ("triton", "user", endpoint)


def test_two_nodes_on_two_services_bind_two_endpoints() -> None:
    tag = f"two-{secrets.token_hex(4)}"
    with CountingListener() as a, CountingListener() as b:
        ep_a, ep_b = a.endpoint("grpc"), b.endpoint("grpc")
        serve(ep_a, _payload(tag, 0.45, -0.1))
        serve(ep_b, _payload(tag, 0.8, 0.2))
        plan = triton_service_plan(tag, [("svc-a", 0.45, -0.1), ("svc-b", 0.8, 0.2)])
        assert [s.name for s in plan.services] == ["svc-a", "svc-b"]
        before = len(connects())
        given = {"svc-a": ep_a, "svc-b": ep_b}
        with closing_bounded(runner_over(submit_api().ThreadBackend(2), services=given)) as runner:
            row_a, row_b = run_bounded(lambda: runner.run(plan), BOUND_S).value
        assert np.allclose(row_a, expected(0.45, -0.1), rtol=1e-6)
        assert np.allclose(row_b, expected(0.8, 0.2), rtol=1e-6)
        assert set(connects()[before:]) == {ep_a, ep_b}, connects()[before:]
        assert a.settled() >= 1 and b.settled() >= 1


def test_the_literal_url_plan_starts_no_service_set(caplog: pytest.LogCaptureFixture) -> None:
    tag = f"url-{secrets.token_hex(4)}"
    url = f"grpc://literal-{tag}.m68a.example:8001"
    serve(url, _payload(tag))
    plan = triton_url_plan(tag, url)
    assert plan.services == ()
    backend = RecordingBackend(submit_api().ThreadBackend(2))
    before = len(connects())
    with CountingListener() as listener:
        with (
            caplog.at_level(logging.INFO),
            closing_bounded(runner_over(backend, services={"triton": listener.endpoint("grpc")})) as runner,
        ):
            (row,) = run_bounded(lambda: runner.run(plan), BOUND_S).value
        assert listener.settled() == 0, "a service set checked an endpoint the plan does not name"
    assert np.allclose(row, expected(0.45, -0.1), rtol=1e-6)
    assert set(connects()[before:]) == {url}
    assert backend.log.probe_keys() == [], backend.log.probe_keys()
    assert status_records(caplog.records) == []


def _payload(tag: str, w: float = 0.45, b: float = -0.1) -> bytes:
    """A served model's descriptor: the fake server at an endpoint computes with these weights."""
    return descriptor(tag, w, b)
