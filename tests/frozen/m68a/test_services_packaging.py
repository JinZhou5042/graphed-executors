"""m68a §3.1/§6 packaging facts, read from the repository's own files (text and structure, no network).

- ``ci.yml`` ``test-htcondor`` installs ``tritonclient[grpc]`` and ``grpcio-health-checking``, starts the
  Triton container with ``recipes.triton``'s flags on 8001 serving ``tests/frozen/m68a/data/triton_models``,
  waits on ``check_ready``, exports ``GRAPHED_TRITON_GRPC=localhost:8001`` for the live file, runs
  ``tests/frozen/m67`` and ``tests/frozen/m68a``, and widens its diff-cover include to
  ``src/graphed_executors/submit/services.py``; ``test-dask`` lists ``test_scope_dask_memory.py``.
- ``.coveragerc-htcondor`` sources include ``graphed_executors.submit.services``.
- No service is named in the engine: a case-insensitive search for ``triton``/``histserv`` over
  ``submit/services.py``, ``submit/engine.py``, ``submit/protocol.py`` and ``htcondor_backend/`` (less
  ``sites.py``, whose site data may name a site's Triton) finds nothing; ``submit/recipes.py`` (control)
  names one.
- The frozen README states the positive leg by test file.

Discriminates: a service name in the engine, and a CI leg that never runs the Triton or m68a legs.
"""

from __future__ import annotations

import configparser
import importlib
import re
from pathlib import Path
from typing import Any

import pytest
from services_harness import HARNESS_DIR, recipes_api

ROOT = Path(HARNESS_DIR).parents[2]
CI = ROOT / ".github" / "workflows" / "ci.yml"
SRC = ROOT / "src" / "graphed_executors"
NAMES = re.compile(r"triton|histserv", re.IGNORECASE)


def _jobs() -> dict[str, Any]:
    """``ci.yml``'s jobs, parsed with PyYAML when it is installed, else split by job header."""
    text = CI.read_text()
    try:
        yaml = importlib.import_module("yaml")
    except ImportError:
        jobs: dict[str, Any] = {}
        body = text.split("\njobs:\n", 1)[1]
        for match in re.finditer(r"(?m)^  ([\w-]+):\n((?:(?:    .*)?\n)*)", body):
            jobs[match.group(1)] = {"text": match.group(2)}
        return jobs
    doc = yaml.safe_load(text)
    return {
        name: {"doc": job, "text": yaml.safe_dump(job, width=10_000)} for name, job in doc["jobs"].items()
    }


def job_text(name: str) -> str:
    jobs = _jobs()
    assert name in jobs, sorted(jobs)
    return str(jobs[name]["text"])


def test_the_htcondor_job_installs_the_grpc_clients() -> None:
    text = job_text("test-htcondor")
    assert re.search(r"tritonclient\[grpc[\],]", text), "test-htcondor does not install tritonclient[grpc]"
    assert "grpcio-health-checking" in text


def test_the_htcondor_job_serves_graphed_identity_with_the_recipe_flags() -> None:
    text = job_text("test-htcondor")
    argv = recipes_api().triton("triton", "image", "/models").launch.argv
    flags = [a.replace("{port}", "8001") for a in argv[1:] if not a.startswith("--model-repository")]
    assert flags and all(flag in text for flag in flags), (flags, text)
    assert argv[0] in text and "docker run" in text
    assert re.search(r"8001:8001", text), "the container's gRPC port is not published on 8001"
    assert "tests/frozen/m68a/data/triton_models" in text
    assert "check_ready" in text, "the job does not wait on check_ready"
    assert re.search(r"GRAPHED_TRITON_GRPC\W+localhost:8001", text), "GRAPHED_TRITON_GRPC is not exported"


def test_the_model_repository_holds_graphed_identity() -> None:
    model = Path(HARNESS_DIR) / "data" / "triton_models" / "graphed_identity"
    config = (model / "config.pbtxt").read_text()
    assert re.search(r'name:\s*"graphed_identity"', config)
    assert re.search(r'backend:\s*"python"', config)
    assert re.search(r'name:\s*"INPUT0"', config) and re.search(r'name:\s*"OUTPUT0"', config)
    assert config.count("TYPE_FP32") == 2
    assert (model / "1" / "model.py").is_file()


def test_the_all_os_job_installs_the_health_checking_package() -> None:
    """§6: the all-OS ``test`` job installs ``grpcio-health-checking`` (the reference servicer the
    gRPC cases of ``test_service_checks.py`` serve), so those cases run there instead of failing."""
    assert "grpcio-health-checking" in job_text("test")


def test_the_htcondor_job_runs_m67_and_m68a() -> None:
    text = job_text("test-htcondor")
    assert re.search(r"tests/frozen/m67\b", text) and re.search(r"tests/frozen/m68a\b", text), text


def test_the_htcondor_diff_cover_includes_the_service_set() -> None:
    text = job_text("test-htcondor")
    assert "diff-cover" in text and "src/graphed_executors/submit/services.py" in text


def test_the_dask_job_lists_the_scope_memory_file() -> None:
    assert "tests/frozen/m68a/test_scope_dask_memory.py" in job_text("test-dask")


def test_the_htcondor_coverage_sources_include_the_service_set() -> None:
    parser = configparser.ConfigParser()
    parser.read(ROOT / ".coveragerc-htcondor")
    sources = parser.get("run", "source").split()
    assert "graphed_executors.submit.services" in sources, sources
    assert "graphed_executors.htcondor_backend" in sources


def _engine_files() -> list[Path]:
    files = [SRC / "submit" / "services.py", SRC / "submit" / "engine.py", SRC / "submit" / "protocol.py"]
    files += sorted(p for p in (SRC / "htcondor_backend").rglob("*") if p.is_file() and p.name != "sites.py")
    return files


def test_no_service_is_named_in_the_engine() -> None:
    files = _engine_files()
    assert (SRC / "submit" / "services.py") in files and (SRC / "submit" / "services.py").is_file()
    hits = {
        str(p.relative_to(SRC)): len(NAMES.findall(p.read_text(errors="replace")))
        for p in files
        if p.suffix in (".py", ".sh", ".txt", ".json", "")
    }
    assert sum(hits.values()) == 0, {k: v for k, v in hits.items() if v}


def test_the_recipes_do_name_a_service() -> None:
    assert (SRC / "submit" / "recipes.py").is_file(), "submit/recipes.py does not exist"
    assert len(NAMES.findall((SRC / "submit" / "recipes.py").read_text())) > 0


@pytest.mark.parametrize(
    "hosting_file",
    [
        "test_services_protocol.py",
        "test_driverless_endpoints.py",
        "test_cluster_services_live.py",
        "test_dask_hosted_service.py",
        "test_parsl_hosted_service.py",
    ],
)
def test_the_readme_states_the_positive_leg_by_test_file(hosting_file: str) -> None:
    readme = (Path(HARNESS_DIR) / "README.md").read_text()
    (sentence,) = [line for line in readme.splitlines() if "third recipe" in line]
    assert "http_server" in sentence and hosting_file in sentence, sentence
    assert "§" not in sentence, "the positive-leg sentence cites plan coordinates instead of test files"
