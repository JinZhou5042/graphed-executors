"""graphed-executors: pluggable executors for a :class:`graphed.core.Plan`.

The reference single-machine executor is :mod:`graphed_executors.local` — thread/process pools,
deterministic straggler-tolerant tree reduction, ``open_once`` file-locality, peer reduction,
work-stealing, and the repartition/shuffle substrate. Additional executors (e.g. distributed
backends) are added as sibling subpackages under their own optional-dependency extras; nothing here
imports a specific backend, so installing the base package pulls only the local one.
"""

from __future__ import annotations

__version__ = "0.0.3"
