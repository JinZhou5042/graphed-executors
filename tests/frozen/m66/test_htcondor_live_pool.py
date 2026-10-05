"""m66 D3: the backend against a real HTCondor pool (the personal pool of the ``test-htcondor`` CI job).

The module needs the ``htcondor2`` bindings and is skipped with that reason where they are not
installed. Where they are installed, each test first requires a schedd in the collector within a
bound and fails with the reason when there is none: a missing pool is reported, never passed over.

(a) the generic profile: the plan is bit-for-bit on two pilot pids, the harness reached the pilots
through ``user_modules`` (its file sits in the job scratch dir), the full job ads never carry the
secret, and ``close()`` removes the jobs, leaves two history rows and keeps ``pilot.0.out``.
(b) a spooled profile shipping a venv the test builds: the pilot's ``sys.prefix`` is under its
scratch dir, and ``close()`` retrieves ``pilot.0.out`` and empties the queue.

Discriminates a launcher that never submits, a missing bookkeeping, retrieve or remove step, a secret
leaking into the job ad, a shipped env that is ignored, and dead transfer of ``user_modules``."""

from __future__ import annotations

import os
import subprocess
import sys
import sysconfig
from pathlib import Path
from typing import Any

import pytest
from htcondor_harness import (
    HARNESS_FILE,
    REPO_ROOT,
    concat_plan,
    expected_concat,
    harness_origin,
    htcondor_api,
    paced_prov_process,
    pilot_prefix,
    prov_plan,
    run_bounded,
    wait_for,
)

htcondor2: Any = pytest.importorskip(
    "htcondor2", reason="the htcondor bindings are not installed (Linux wheels only; the test-htcondor job)"
)

POOL_PROBE_S = 30.0
BATCH = 'regexp("^graphed-pilots-", JobBatchName)'


def require_pool() -> Any:
    """The schedd of a reachable pool, or a failure naming what is missing."""
    ads = run_bounded(
        lambda: htcondor2.Collector().query(htcondor2.AdType.Schedd, projection=["Name"]), POOL_PROBE_S
    )
    assert ads, "htcondor2 is installed but the collector lists no schedd: start a personal HTCondor"
    return htcondor2.Schedd()


def graphed_clusters(schedd: Any) -> set[int]:
    return {int(ad["ClusterId"]) for ad in schedd.query(constraint=BATCH, projection=["ClusterId"])}


def in_queue(schedd: Any, cluster: int) -> list[Any]:
    return list(schedd.query(constraint=f"ClusterId == {cluster}", projection=["ProcId"]))


def history(schedd: Any, cluster: int) -> list[Any]:
    return list(schedd.history(constraint=f"ClusterId == {cluster}", projection=["ProcId"]))


def test_generic_pool_run(tmp_path: Path) -> None:
    schedd = require_pool()
    before = graphed_clusters(schedd)
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    runner = htcondor_api().htcondor_runner(
        n_pilots=2, site="generic", log_dir=log_dir, user_modules=[HARNESS_FILE], min_pilots=2
    )
    try:
        seq = run_bounded(lambda: runner.run(concat_plan(8, "m66live"))).value
        prov = run_bounded(lambda: runner.run(prov_plan(8, "m66livep", process=paced_prov_process))).value
        origin = runner.backend.submit(harness_origin, key="graphed-m66live-origin").result(timeout=120)
        secret = (log_dir / "graphed-secret").read_bytes().strip()
        (cluster,) = graphed_clusters(schedd) - before
        ads_text = "\n".join(str(ad) for ad in schedd.query(constraint=f"ClusterId == {cluster}"))
    finally:
        runner.close()
    assert seq == expected_concat(8, "m66live")
    assert prov.payload == expected_concat(8, "m66livep")
    pids = {pid for _uri, pid, _worker in prov.leaves}
    assert len(pids) >= 2 and os.getpid() not in pids, pids
    module_file, pythonpath, scratch = origin
    assert scratch and Path(module_file).resolve().is_relative_to(Path(scratch).resolve()), origin
    assert not pythonpath, pythonpath
    assert "graphed-secret" in ads_text, "the ad query returned none of the pilot job ads"
    assert secret and secret not in ads_text.encode(), "the secret is in the job ad"
    assert secret.hex() not in ads_text, "the secret's hex is in the job ad"
    wait_for(lambda: not in_queue(schedd, cluster), 60.0)
    assert not in_queue(schedd, cluster)
    wait_for(lambda: len(history(schedd, cluster)) >= 2, 90.0)
    assert len(history(schedd, cluster)) == 2
    assert (log_dir / "pilot.0.out").is_file()


def build_env(root: Path) -> Path:
    """A non-editable venv over the driver's site-packages: ``venv --system-site-packages`` plus
    ``pip install --no-deps <repo>``; a driver running in a venv also lends its site-packages."""
    env = root / "env"
    subprocess.run(
        [sys.executable, "-m", "venv", "--system-site-packages", str(env)], check=True, timeout=300
    )
    if sys.prefix != sys.base_prefix:
        vars_ = {"base": str(env), "platbase": str(env)}
        purelib = Path(sysconfig.get_path("purelib", scheme="venv", vars=vars_))
        lent = {sysconfig.get_path("purelib"), sysconfig.get_path("platlib")}
        (purelib / "graphed_m66_driver_site.pth").write_text("\n".join(sorted(lent)) + "\n")
    py = env / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
    subprocess.run(
        [str(py), "-m", "pip", "install", "-q", "--no-deps", str(REPO_ROOT)], check=True, timeout=600
    )
    return env


def test_spooled_site_ships_the_env_and_retrieves_logs(tmp_path: Path) -> None:
    schedd = require_pool()
    before = graphed_clusters(schedd)
    env = build_env(tmp_path)
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    api = htcondor_api()
    site = api.SiteProfile(
        name="ci-spool",
        spool=True,
        ship_env=True,
        sandbox_root=None,
        submit={},
        schedd_query=("COLLECTOR_HOST", "true"),
    )
    runner = api.htcondor_runner(n_pilots=1, site=site, env=env, log_dir=log_dir, user_modules=[HARNESS_FILE])
    try:
        value = run_bounded(lambda: runner.run(concat_plan(4, "m66spool"))).value
        prefix, scratch = runner.backend.submit(pilot_prefix, key="graphed-m66spool-prefix").result(
            timeout=120
        )
        clusters = graphed_clusters(schedd) - before
    finally:
        runner.close()
    assert value == expected_concat(4, "m66spool")
    assert scratch and Path(prefix).resolve().is_relative_to(Path(scratch).resolve()), (prefix, scratch)
    assert Path(prefix).resolve() != Path(sys.prefix).resolve()
    (cluster,) = clusters
    wait_for(lambda: not in_queue(schedd, cluster), 60.0)
    assert not in_queue(schedd, cluster)
    assert (log_dir / "pilot.0.out").is_file(), sorted(p.name for p in log_dir.iterdir())
