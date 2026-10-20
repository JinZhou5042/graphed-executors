"""The worker side of :class:`~graphed_executors.vine_graph_backend.VineGraphBackend`.

``VineGraphBackend.submit`` runs every engine task fn through ``_vine_task_shim`` (module level, so vine_graph sends it
by reference and workers import it from the installed package). The shim installs a :class:`WorkerEnv` whose
``open_once`` resources live as long as the process (vine_graph forks each call from its library process, so a handle
opened in one call is not seen by the next) and whose ``emit`` collects monitor events. vine_graph has no channel for
them, so they travel with the task's value (``_Emitted``) or on its exception (``graphed_events``), only when the
task emitted any.
"""

from __future__ import annotations

import contextlib
import socket
from collections.abc import Callable

from graphed.core.execution import LocalResources, WorkerResources

from graphed_executors.submit.engine import _reset_worker_env, set_worker_env

_RESOURCES: list[WorkerResources] = []

# The monitor events of one call, as (topic, events) pairs in emission order.
_Events = list[tuple[str, list[dict[str, object]]]]


class _Emitted:
    """A task's value with the monitor events its fn emitted, as ``(topic, events)`` pairs."""

    __slots__ = ("events", "value")

    def __init__(self, value: object, events: _Events) -> None:
        self.value = value
        self.events = events

    def __getstate__(self) -> tuple[object, _Events]:
        return (self.value, self.events)

    def __setstate__(self, state: tuple[object, _Events]) -> None:
        self.value, self.events = state


class _VineEnv:
    """The :class:`WorkerEnv` of one call on a TaskVine worker."""

    def __init__(self) -> None:
        if not _RESOURCES:
            _RESOURCES.append(LocalResources(max_open=128))
        self.resources = _RESOURCES[0]
        self.worker = socket.gethostname()
        self.events: _Events | None = None

    def emit(self, topic: str, events: list[dict[str, object]]) -> None:
        if self.events is None:
            self.events = []
        self.events.append((topic, events))


def _vine_task_shim(fn: Callable[..., object], /, *args: object) -> object:
    env = _VineEnv()
    token = set_worker_env(env)
    try:
        value = fn(*(arg.value if type(arg) is _Emitted else arg for arg in args))
    except BaseException as exc:
        if env.events:
            with contextlib.suppress(Exception):  # an exception class with __slots__ takes no attribute
                setattr(exc, "graphed_events", env.events)  # noqa: B010  (an attribute BaseException does not declare)
        raise
    finally:
        _reset_worker_env(token)
    return value if env.events is None else _Emitted(value, env.events)
