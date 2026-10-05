"""m66 D3: ``graphed_executors.htcondor_backend`` imports, with all its public names, where the htcondor
bindings cannot be imported, and it never attempts to import them at module load. ``submit/`` never
names htcondor.

A fresh interpreter blocks ``htcondor``, ``htcondor2`` and ``classad2`` with a meta-path finder that
records each attempt, so an eager import fails the probe even when it is wrapped in ``try``.

Discriminates an eager bindings import anywhere on the package import path, a missing public name,
and an htcondor reference in the backend-neutral engine."""

from __future__ import annotations

import subprocess
import sys

from htcondor_harness import REPO_ROOT

PROBE = """
import sys

BLOCKED = ("htcondor", "htcondor2", "classad2")
attempts = []


class Block:
    def find_spec(self, name, path=None, target=None):
        if name.split(".")[0] in BLOCKED:
            attempts.append(name)
            raise ImportError(f"blocked for the m66 probe: {name}")
        return None


sys.meta_path.insert(0, Block())
import graphed_executors.htcondor_backend as pkg
from graphed_executors.htcondor_backend import (
    CondorPilots, HTCondorBackend, HTCondorRunner, LocalPilots, PilotLauncher, SITES, SiteProfile,
    WorkerLost, htcondor_runner,
)
assert attempts == [], f"module load attempted {attempts}"
loaded = sorted(m for m in sys.modules if m.split(".")[0] in BLOCKED)
assert loaded == [], f"module load imported {loaded}"
print("CLEAN")
"""


def test_the_package_imports_without_the_bindings() -> None:
    proc = subprocess.run(
        [sys.executable, "-c", PROBE], capture_output=True, text=True, timeout=120, check=False
    )
    assert proc.returncode == 0, f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    assert "CLEAN" in proc.stdout


def test_submit_names_htcondor_nowhere() -> None:
    texts = {
        p.name: p.read_text(encoding="utf-8")
        for p in (REPO_ROOT / "src/graphed_executors/submit").glob("*.py")
    }
    assert any("SubmitBackend" in t for t in texts.values()), f"the scan read nothing useful: {sorted(texts)}"
    hits = sorted(name for name, text in texts.items() if "htcondor" in text.lower())
    assert hits == [], hits
