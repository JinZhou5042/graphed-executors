"""graphed plans for the m68a suite: a partitioned in-memory awkward source, the shipped Triton External
recorded by ``service=`` or ``url=``, a fake Triton transport, and an ``aggregate_plan`` whose ``reduce``
is a spy.

The fake transport is graphed's ``tests/frozen/preserve/m26/fake_triton.py`` copied (the calling surface
``tritonclient`` offers: ``client.infer(model, inputs, outputs=...)`` returning ``.as_numpy(name)``;
the served model computes ``sigmoid(w * x0 + b)`` from the descriptor), plus a connect log: every
``transport(params)`` call appends ``params["url"]``, the endpoint the evaluator was bound to. A plan
reaches it through ``params["transport"] = "m68a_triton:transport"``, which D7 keeps overriding the
endpoint's scheme, so the fake stands in for ``tritonclient.grpc`` without the package.

Module-level and importable by name: workers (and pilots, which get this directory on their path)
unpickle the plan parts from here.
"""

from __future__ import annotations

import json
import sys
import threading
from collections.abc import Sequence
from typing import Any

import awkward as ak
import numpy as np
from graphed import Session, aggregate_plan
from graphed.awkward import AwkwardBackend, AwkwardForm
from graphed.core import Partition, Plan, WorkerResources
from graphed.preserve import TRITON_PLUGIN, record_external, register_plugin
from graphed.services import ServiceSpec

X = np.array([0.0, 0.5, 1.0, 2.0, 3.5, 5.0, 8.0, 13.0])
TRANSPORT = "m68a_triton:transport"

# ---- the fake transport (graphed tests/frozen/preserve/m26/fake_triton.py, copied) ---------------

SERVERS: dict[str, FakeTritonClient] = {}
CONNECTS: list[str] = []
_LOCK = threading.Lock()


class _Result:
    def __init__(self, outputs: dict[str, np.ndarray]) -> None:
        self._outputs = outputs

    def as_numpy(self, name: str) -> np.ndarray:
        return self._outputs[name]


class FakeTritonClient:
    def __init__(self, descriptor: dict[str, Any]) -> None:
        self.descriptor = descriptor
        self.closed = False
        self.infer_calls = 0

    def infer(self, model_name: str, inputs: list[Any], outputs: list[Any] | None = None) -> _Result:
        assert model_name == self.descriptor["model"], "client asked for a model this server does not host"
        self.infer_calls += 1
        x = inputs[0]._raw_data  # the numpy the plugin set via set_data_from_numpy
        w, b = float(self.descriptor["weights"]["w"]), float(self.descriptor["weights"]["b"])
        y = 1.0 / (1.0 + np.exp(-(w * x[:, 0] + b)))
        name = outputs[0].name() if outputs else "y"
        return _Result({name: y.astype("float32")})

    def close(self) -> None:
        self.closed = True


class _FakeInferInput:
    """Stands in for tritonclient's InferInput."""

    def __init__(self, name: str, shape: list[int], datatype: str) -> None:
        self._name, self._shape, self._datatype = name, shape, datatype
        self._raw_data: np.ndarray | None = None

    def set_data_from_numpy(self, arr: Any) -> None:
        self._raw_data = np.asarray(arr)


class _FakeRequestedOutput:
    def __init__(self, name: str) -> None:
        self._name = name

    def name(self) -> str:
        return self._name


def serve(url: str, payload: bytes) -> FakeTritonClient:
    """Start a fake server at ``url`` (the endpoint a run binds) hosting the descriptor's model."""
    client = FakeTritonClient(json.loads(payload.decode()))
    SERVERS[url] = client
    return client


def transport(params: Any) -> FakeTritonClient:
    """The injectable transport factory: the connection for ``params['url']``, logged."""
    url = str(params["url"])
    with _LOCK:
        CONNECTS.append(url)
    return SERVERS[url]


def connects() -> list[str]:
    with _LOCK:
        return list(CONNECTS)


InferInput = _FakeInferInput
InferRequestedOutput = _FakeRequestedOutput

# ---- a partitioned in-memory source (graphed tests/frozen/preserve/m68 fixtures, copied) ---------


class Chunks:
    """A ``PartitionedSource`` over ``X`` in ``steps_per_file`` blind partitions."""

    def __init__(self, uri: str = "mem://m68a-events") -> None:
        self.uri = uri
        self.data = ak.Array({"x": X})

    def partitions(self, steps_per_file: int) -> tuple[Partition, ...]:
        return tuple(Partition.blind(self.uri, "", s, steps_per_file) for s in range(steps_per_file))

    def read_partition(self, partition: Partition, columns: Any, resources: WorkerResources) -> Any:
        part = partition.resolve(len(self.data))
        return self.data[part.entry_start : part.entry_stop]


def new_events(uri: str = "mem://m68a-events") -> Any:
    register_plugin(TRITON_PLUGIN, validate=False)
    session = Session(AwkwardBackend())
    source = Chunks(uri)
    form = AwkwardForm(ak.Array(source.data.layout.to_typetracer(forget_length=True)))
    return session.source("events", form=form, data=source)


def descriptor(tag: str, w: float = 0.45, b: float = -0.1) -> bytes:
    """A served-model descriptor; a distinct ``tag`` per scenario keeps connection caches apart."""
    return json.dumps(
        {"model": "scorer", "version": tag, "weights": {"w": w, "b": b}}, sort_keys=True
    ).encode()


def expected(w: float, b: float) -> np.ndarray:
    return np.asarray(1.0 / (1.0 + np.exp(-(w * X + b))))


def score(ev: Any, payload: bytes, **where: str) -> Any:
    """The shipped Triton External over ``ev.x`` through the fake transport; ``where`` is
    ``service=<name>`` or ``url=<literal>``."""
    params = {"model": "scorer", "transport": TRANSPORT, "input_name": "x", "output_name": "y", **where}
    return record_external(ev.session, TRITON_PLUGIN, payload, [ev.x], params=params)


def _rows(values: list[Any]) -> list[list[float]]:
    return [np.asarray(ak.to_numpy(ak.Array(v)), dtype="float64").tolist() for v in values]


def _cat(a: list[list[float]], b: list[list[float]]) -> list[list[float]]:
    return [x + y for x, y in zip(a, b, strict=True)] if a else b


def values_plan(*outputs: Any) -> Plan[Any]:
    """One output row per output Array, over two partitions."""
    return aggregate_plan(*outputs, reduce=_rows, combine=_cat, empty=list, steps_per_file=2)


def triton_service_plan(
    tag: str, services: Sequence[tuple[str, float, float]], check: str = "tcp"
) -> Plan[Any]:
    """One Triton node per ``(service name, w, b)``, each naming its declared ``kind="triton"`` spec."""
    ev = new_events(f"mem://m68a-{tag}")
    outputs = []
    for name, _w, _b in services:
        ev.session.declare_service(
            ServiceSpec(name, "triton", check=check, ports=(40000, 40010), timeout_s=30.0)
        )
        outputs.append(score(ev, descriptor(tag), service=name))
    return values_plan(*outputs)


def triton_url_plan(tag: str, url: str) -> Plan[Any]:
    ev = new_events(f"mem://m68a-{tag}")
    return values_plan(score(ev, descriptor(tag), url=url))


# ---- the plan's §3 params on a live server --------------------------------------------------------

LIVE_PAYLOAD = json.dumps({"model": "graphed_identity", "served_by": "m68a"}, sort_keys=True).encode()


def identity_rows_and_wire(values: list[Any]) -> tuple[list[list[float]], bool]:
    """The live plan's reduce: the output rows, and whether ``tritonclient.grpc`` served them here."""
    return _rows(values), "tritonclient.grpc" in sys.modules


def identity_cat(a: tuple[Any, bool] | None, b: tuple[Any, bool] | None) -> tuple[Any, bool] | None:
    if a is None:
        return b
    if b is None:
        return a
    return _cat(a[0], b[0]), a[1] and b[1]


def no_rows() -> None:
    return None


def live_identity_plan(spec: ServiceSpec) -> Plan[Any]:
    """The §3 External params on ``graphed_identity`` (``INPUT0`` -> ``OUTPUT0``, no ``transport``),
    naming ``spec``; its value is ``(rows, grpc client used)``, the rows equal to ``X``."""
    ev = new_events(f"mem://m68a-live-{spec.name}")
    ev.session.declare_service(spec)
    params = {
        "service": spec.name,
        "model": "graphed_identity",
        "input_name": "INPUT0",
        "output_name": "OUTPUT0",
    }
    out = record_external(ev.session, TRITON_PLUGIN, LIVE_PAYLOAD, [ev.x], params=params)
    return aggregate_plan(
        out, reduce=identity_rows_and_wire, combine=identity_cat, empty=no_rows, steps_per_file=2
    )


# ---- an aggregate_plan whose reduce is a spy ------------------------------------------------------


def spy_aggregate_plan(reduce: Any, spec: ServiceSpec, combine: Any, empty: Any) -> Plan[Any]:
    """``aggregate_plan(ev.x, reduce=<spy>, services=(spec.name,))`` over two partitions."""
    ev = new_events(f"mem://m68a-aggregate-{spec.name}")
    ev.session.declare_service(spec)
    return aggregate_plan(
        ev.x, reduce=reduce, combine=combine, empty=empty, steps_per_file=2, services=(spec.name,)
    )
