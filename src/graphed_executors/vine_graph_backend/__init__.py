"""Run graphed plans on TaskVine through vine_graph (``ndcctools.taskvine.vine_graph``).

:class:`VineGraphBackend` is a :class:`~graphed_executors.submit.SubmitBackend` over a vine_graph ``Executor``, so the
shared :class:`~graphed_executors.submit.SubmitRunner` runs every plan shape on it: fixed plans as graphed's
``plan_tree`` future graph (bit for bit equal to a local run), adaptive plans, ``DurablePlanV2`` stages, monitors,
and services. ``vine_graph_runner(executor)`` returns that runner::

    from ndcctools.taskvine import vine_graph as vg
    from graphed_executors.vine_graph_backend import vine_graph_runner

    with vg.Executor(9123, "my-analysis") as executor, vine_graph_runner(executor) as runner:
        result = runner.run(plan)
"""

from .backend import VineGraphBackend, vine_graph_runner

__all__ = ["VineGraphBackend", "vine_graph_runner"]
