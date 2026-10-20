"""Carry a worker-produced value across a process boundary: stdlib pickle, or cloudpickle when stdlib
pickle refuses it (a lambda or local class the user's value holds, e.g. in an awkward ``behavior``)."""

from __future__ import annotations

import io
import pickle
from typing import Any

import cloudpickle

PICKLE_ERRORS = (pickle.PicklingError, AttributeError, TypeError)


def from_value(value: Any) -> Any:
    return value


def from_cloudpickle(data: bytes) -> Any:
    """Load a cloudpickled value; a separate name from :func:`from_value` so the fallback is visible."""
    return pickle.loads(data)


class Portable:
    """Pickles as its value when stdlib pickle can, else as cloudpickle bytes; unpickles to the value."""

    __slots__ = ("cloud", "value")

    def __init__(self, value: object) -> None:
        try:
            # out-of-band buffers: the trial walks the value without copying its array data
            pickle.Pickler(io.BytesIO(), 5, buffer_callback=lambda _buf: None).dump(value)
            self.value, self.cloud = value, False
        except PICKLE_ERRORS:
            self.value, self.cloud = cloudpickle.dumps(value, pickle.HIGHEST_PROTOCOL), True

    def __reduce__(self) -> tuple[Any, tuple[object]]:
        return (from_cloudpickle if self.cloud else from_value, (self.value,))
