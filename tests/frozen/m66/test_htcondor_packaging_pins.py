"""m66 packaging and CI content pins (plan §2 Packaging, §3 test 7, §4 commit 1). Reads files only.

Discriminates a drifted ``htcondor`` floor or a smuggled marker, a main matrix that measures the
optional backend (it would sit near 0% there), a ``.coveragerc-htcondor`` that measures the wrong
package or cannot combine and flush pilot subprocess data, and a CI job whose pytest step would let
coverage go silently unmeasured or never runs the frozen suite on a live pool."""

from __future__ import annotations

import configparser
import re
import tomllib
from typing import Any

from htcondor_harness import REPO_ROOT


def pyproject() -> dict[str, Any]:
    with open(REPO_ROOT / "pyproject.toml", "rb") as f:
        return tomllib.load(f)


def test_htcondor_extra_is_the_measured_floor() -> None:
    extras = pyproject()["project"]["optional-dependencies"]
    assert extras.get("htcondor") == ["htcondor>=25.13"], extras.get("htcondor")


def test_main_coverage_omits_the_htcondor_backend() -> None:
    cov = pyproject()["tool"]["coverage"]
    assert "*/htcondor_backend/*" in cov["run"]["omit"], cov["run"]["omit"]
    assert cov["report"]["fail_under"] == 90


def test_coveragerc_htcondor_gates_exactly_the_backend() -> None:
    path = REPO_ROOT / ".coveragerc-htcondor"
    assert path.is_file(), ".coveragerc-htcondor is missing"
    parser = configparser.ConfigParser()
    parser.read(path)
    sources = {line.strip() for line in parser.get("run", "source").splitlines() if line.strip()}
    # amended at m68a (owner ruling, 2026-09-28): plan-services §6 adds the engine's service set, which
    # these suites drive through pilots and driver jobs; nothing else may join the backend
    assert "graphed_executors.htcondor_backend" in sources, sources
    assert sources - {"graphed_executors.htcondor_backend"} <= {"graphed_executors.submit.services"}, sources
    assert parser.getboolean("run", "branch") is True
    assert parser.getboolean("run", "parallel") is True
    assert parser.getboolean("run", "sigterm") is True
    assert parser.getint("report", "fail_under") == 90


def ci_text() -> str:
    return (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text()


def job_block(text: str, job: str) -> str:
    match = re.search(rf"(?ms)^  {re.escape(job)}:\n(.*?)(?=^  \w[\w-]*:$|\Z)", text)
    assert match, f"job {job!r} not found in ci.yml"
    return match.group(0)


def pytest_commands(block: str) -> list[str]:
    folded = block.replace("\\\n", " ")
    return [line.strip() for line in folded.splitlines() if "pytest " in line]


def test_ci_htcondor_job_runs_m66_on_a_personal_pool_with_scoped_coverage() -> None:
    block = job_block(ci_text(), "test-htcondor")
    assert "get.htcondor.org" in block
    commands = [c for c in pytest_commands(block) if "tests/frozen/m66" in c]
    assert commands, "no pytest command over tests/frozen/m66 in the test-htcondor job"
    for command in commands:
        assert "--cov-config=.coveragerc-htcondor" in command, command
        assert "--cov=" not in command, f"a path-valued --cov= overrides the config source: {command!r}"
        assert re.search(r"--cov(\s|$)", command), f"a bare --cov is needed to measure at all: {command!r}"


def test_ci_required_waits_for_the_htcondor_job() -> None:
    block = job_block(ci_text(), "ci-required")
    needs = re.search(r"needs:\s*\[([^\]]*)\]", block)
    assert needs, block
    assert "test-htcondor" in {n.strip() for n in needs.group(1).split(",")}, needs.group(1)
