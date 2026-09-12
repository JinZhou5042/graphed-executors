"""Sphinx configuration for graphed-executors."""

from __future__ import annotations

project = "graphed-executors"
author = "graphed-org"
release = "0.0.2"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
]
templates_path = ["_templates"]
exclude_patterns = ["_build"]
html_theme = "furo"
html_title = "graphed-executors"
autodoc_typehints = "description"
# autosummary recursively generates the API reference (docs/api.rst) from the package itself, so it
# can never drift from the code. Imported re-exports are documented in their defining module only.
autosummary_generate = True
autosummary_imported_members = False
# The docs envs (CI `.[docs]`, RTD requirements.txt) install neither dask nor parsl on purpose — the
# backends are optional extras. Every dask_backend module is import-safe without it EXCEPT `transport`
# (its worker plugin subclasses `distributed.WorkerPlugin` at class-definition time); the parsl_backend
# modules are all import-safe without parsl (the import is deferred to construction), but parsl is
# mocked too so autodoc never needs the extra installed.
autodoc_mock_imports = ["dask", "distributed", "parsl"]
