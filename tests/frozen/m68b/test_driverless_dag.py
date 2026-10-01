"""m68b part B2 on data (every OS, bindings recorded): `SiteProfile.job_root`, the driverless DAG with
SERVICE nodes, the in-job backend that waits for their announces, and `RunHandle(dag=True)`
(plan-services.md §3.3 "B2 — DAG driverless, `job_root`, lxplus", the `test_driverless_dag.py` row).

`submit_driverless` runs over `m68b_dag_harness.record_bindings`; what it wrote (`run.dag`, the node
`.sub` files, `service-svc<i>/`, `run.json`, `driver.sh`) is read back from the run directory. The
in-job legs build `driver._runner(...)` with a machine ad and no bindings. The fixture comparison
(`data/from_dag-generic.txt`, the description `probes/m68b/probe_dag_service.txt` printed) needs real
bindings and `condor_dagman` on `PATH` (the `test-htcondor` job) and is skipped elsewhere.
"""

from __future__ import annotations

import json
import logging
import os
import pickle
import re
import shutil
import stat
import subprocess
import sys
import tempfile
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import pytest
from graphed.core.execution import ExecResult, StopReason
from m68b_dag_harness import (
    DAG_OPTIONS,
    DATA_DIR,
    DRIVER_MODULE,
    FAKE_SCHEDD,
    IMAGE,
    PLACEHOLDER_TEXT,
    SERVICES_LOGGER,
    TRITON_IMAGE,
    DagSchedd,
    MarkerBomb,
    cpu_spec,
    dag_statements,
    driver_api,
    driverless_api,
    fake_venv,
    free_range,
    gpu_spec,
    htcondor_api,
    imaged_spec,
    is_node_constraint,
    job_attr,
    logged,
    ok_server,
    post,
    read_sub,
    recipes_api,
    record_bindings,
    run_bounded,
    service_plan,
    services_api,
    site_copy,
    squashed,
    submitted,
    tree_bytes,
    write_machine_ad,
)

DAGMAN = 7  # the DAGMan cluster of the recorded ads; its driver tries are 9, 10, 11
MACHINE = "localhost"  # the driver job's slot, from its machine ad
SERVICE_IDENTITY = "svc-node.m68b.example"

# what probe_dag_service.txt's description embeds of its own run: DAG dir, bindings version, condor_dagman
PROBE_DAG_DIR = "/home/submituser/m68b-dag-R"
PROBE_CSD = "$CondorVersion:' '25.13.2' '2026-08-19' 'BuildID:' '943576' 'PackageID:' '25.13.2-1' 'GitSHA:' '589a5b38' '$"
PROBE_DAGMAN = "/usr/bin/condor_dagman"


def _can_symlink() -> bool:
    with tempfile.TemporaryDirectory() as d:
        target = Path(d, "target")
        target.write_text("t")
        try:
            Path(d, "link").symlink_to(target)
        except OSError:
            return False
        return True


needs_sh = pytest.mark.skipif(shutil.which("sh") is None, reason="no sh to run driver.sh with")
needs_symlinks = pytest.mark.skipif(not _can_symlink(), reason="this host cannot make a symlink")
needs_dagman = pytest.mark.skipif(
    shutil.which("condor_dagman") is None,
    reason="Submit.from_dag needs condor_dagman on PATH (test-htcondor)",
)


def submit(monkeypatch: pytest.MonkeyPatch, plan: Any, **kwargs: Any) -> tuple[Any, Any]:
    """``submit_driverless`` under a fresh recorder: (handle, the fake bindings)."""
    fake = record_bindings(monkeypatch, DagSchedd(), kwargs.pop("real", None))
    kwargs.setdefault("request_memory_mb", 1024)
    handle = run_bounded(lambda: htcondor_api().submit_driverless(plan, **kwargs))
    return handle, fake


def refused(monkeypatch: pytest.MonkeyPatch, plan: Any, **kwargs: Any) -> str:
    """The message of the ``ValueError`` ``submit_driverless`` raises before any bindings call."""
    fake = record_bindings(monkeypatch, DagSchedd())
    kwargs.setdefault("request_memory_mb", 1024)
    with pytest.raises(ValueError) as excinfo:
        run_bounded(lambda: htcondor_api().submit_driverless(plan, **kwargs))
    assert fake.log == [], f"refused after touching the bindings: {fake.log}"
    return str(excinfo.value)


def inputs_of(sub: Mapping[str, str]) -> set[str]:
    return {Path(e.strip()).name for e in sub.get("transfer_input_files", "").split(",") if e.strip()}


# ---- job_root ------------------------------------------------------------------------------------------


def test_sites_carry_job_root_and_no_name_keyed_root_is_left() -> None:
    sites = htcondor_api().SITES
    assert {name: sites[name].job_root for name in ("lpc", "lxplus", "generic")} == {
        "lpc": None,
        "lxplus": "/afs",
        "generic": "/",
    }
    assert not hasattr(driverless_api(), "_SELF_SUBMIT_ROOT")


def test_self_submission_reads_job_root_from_the_row(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    plan = service_plan(())
    site_copy(monkeypatch, "m68b-nojr", "generic", name="m68b-nojr", job_root=None)
    assert "job_root" in refused(monkeypatch, plan, site="m68b-nojr", pilots="condor", log_dir=tmp_path / "a")
    _, fake = submit(monkeypatch, plan, site="m68b-nojr", pilots="local", log_dir=tmp_path / "b")
    assert len(submitted(fake.log)) == 1, "control: the same row runs a fat slot"
    venv = fake_venv(tmp_path / "env")
    lxplus = {"site": "lxplus", "pilots": "condor", "image": IMAGE, "env": venv}
    assert "/afs" in refused(monkeypatch, plan, log_dir=tmp_path / "c", **lxplus)
    root = tmp_path / "root"
    root.mkdir()
    site_copy(monkeypatch, "m68b-jr", "generic", name="m68b-jr", job_root=str(root))
    # log_dir=None: the temporary directory it makes lies outside any root but "/"
    assert str(root) in refused(monkeypatch, plan, site="m68b-jr", pilots="condor", log_dir=None)
    site_copy(monkeypatch, "lxplus", "lxplus", job_root=str(tmp_path))
    handle, fake = submit(monkeypatch, plan, log_dir=tmp_path / "d", **lxplus)
    assert len(submitted(fake.log)) == 1, "an lxplus row whose job_root holds log_dir must self-submit"
    assert json.loads((Path(handle.log_dir) / "run.json").read_text())["pilots"] == "condor"


# ---- which specs become SERVICE nodes -----------------------------------------------------------------


def test_a_kind_the_site_serves_is_not_a_service_node(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "models").mkdir()  # the recipe's relative input, where a submitter would have it
    sandbox = tmp_path / "sandbox"
    sandbox.mkdir()
    spec = recipes_api().triton("triton", image=TRITON_IMAGE, model_repository="models")
    plan = service_plan((spec,))
    kwargs = {
        "site": "lpc",
        "image": IMAGE,
        "env": fake_venv(tmp_path / "env"),
        "n_pilots": 2,
        "pilots": "local",
    }
    site_copy(monkeypatch, "lpc", "lpc", sandbox_root=str(sandbox))
    handle, fake = submit(monkeypatch, plan, log_dir=sandbox / "a", **kwargs)
    assert logged(fake.log, "from_dag") == [], "the EAF row serves kind 'triton': no SERVICE node"
    (desc,) = submitted(fake.log)
    assert Path(desc["executable"]).name == "driver.sh" and str(desc["max_retries"]) == "2", desc
    assert handle.dag is False
    run = json.loads((Path(handle.log_dir) / "run.json").read_text())
    assert run.get("announce_only", {}) == {}, run
    # control: the same row without the site's service makes it a SERVICE node, which lpc refuses
    site_copy(monkeypatch, "lpc", "lpc", services={})
    assert "job_root" in refused(monkeypatch, plan, log_dir=sandbox / "b", **kwargs)


def test_service_nodes_are_named_by_derived_ids_in_name_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # declared out of name order; "0web" sorts first but the driver job hosts it; "given" has an endpoint
    specs = (gpu_spec("driver"), imaged_spec("a b"), cpu_spec("0web"), gpu_spec("given"))
    handle, fake = submit(
        monkeypatch,
        service_plan(specs),
        site="generic",
        n_pilots=2,
        log_dir=tmp_path / "logs",
        services={"given": "http://given.m68b.example:80"},
    )
    assert handle.dag is True
    run_dir = Path(handle.log_dir)
    run = json.loads((run_dir / "run.json").read_text())
    assert run["announce_only"] == {"a b": "svc0", "driver": "svc1"}, run
    assert os.path.isabs(run["dag_dir"]) and Path(run["dag_dir"]) == run_dir, run

    rows = dag_statements(run_dir / "run.dag")
    assert [r for r in rows if r[0] == "JOB"] == [("JOB", "driver", "driver.sub")], rows
    assert sorted(r for r in rows if r[0] == "SERVICE") == [
        ("SERVICE", "svc0", "svc0.sub"),
        ("SERVICE", "svc1", "svc1.sub"),
    ], rows
    assert ("RETRY", "driver", "2", "UNLESS-EXIT", "3") in rows, rows

    driver = read_sub(run_dir / "driver.sub")
    assert Path(driver["executable"]).name == "driver.sh", driver
    assert driver["transfer_output_files"].replace(" ", "") == "result.pkl,driver.log", driver
    assert driver["jobbatchname"].strip('"').startswith("graphed-driverless-"), driver
    assert driver["request_cpus"] == "2" and {"plan.pkl", "run.json"} <= inputs_of(driver), driver
    assert "periodic_remove" not in driver, driver
    nodes = {node: read_sub(run_dir / f"{node}.sub") for node in ("svc0", "svc1")}
    assert job_attr(nodes["svc0"], "SingularityImage") == '"registry.m68b.example/service:1"', nodes["svc0"]
    assert nodes["svc1"].get("request_gpus") == "1", nodes["svc1"]
    for node, sub in nodes.items():
        node_dir = run_dir / f"service-{node}"
        assert Path(sub["initialdir"]) == node_dir, sub
        assert squashed(sub.get("periodic_remove")) == "JobStatus == 5", sub
        assert "graphed-secret" not in inputs_of(sub), sub
        cfg = json.loads((node_dir / "service.json").read_text())
        assert (cfg["key"], cfg["url"], Path(cfg["watch"])) == (node, None, run_dir), cfg
    for sub in (driver, *nodes.values()):
        assert not {"max_retries", "retry_until"} & set(sub), f"a second retry owner: {sub}"
    assert list(run_dir.rglob("graphed-secret")) == [], "a secret before the driver job minted one"

    (call,) = logged(fake.log, "from_dag")
    assert os.path.isabs(call[1]) and Path(call[1]) == run_dir / "run.dag", call
    assert call[2] == {**DAG_OPTIONS, "dagman": "/usr/bin/condor_dagman"}, call
    (entry,) = logged(fake.log, "submit")
    assert entry[1] == {"dag_file": call[1]} and entry[3] is False, entry


def test_an_lxplus_service_node_keeps_its_credential_and_env_and_the_dag_is_not_spooled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    profile = site_copy(monkeypatch, "lxplus", "lxplus", job_root=str(tmp_path))
    assert profile.spool is True
    handle, fake = submit(
        monkeypatch,
        service_plan((gpu_spec("web"),)),
        site="lxplus",
        image=IMAGE,
        env=fake_venv(tmp_path / "env"),
        n_pilots=2,
        log_dir=tmp_path / "logs",
    )
    run_dir = Path(handle.log_dir)
    assert job_attr(read_sub(run_dir / "svc0.sub"), "SendCredential") == "True"
    assert os.path.isfile(run_dir / "service-svc0" / "env.tgz"), "the node's env.tgz link does not resolve"
    (call,) = logged(fake.log, "from_dag")
    assert Path(call[1]) == run_dir / "run.dag" and call[2] == {**DAG_OPTIONS, "dagman": "/usr/bin/condor_dagman"}, call
    (entry,) = logged(fake.log, "submit")
    assert entry[3] is False and logged(fake.log, "spool") == [], fake.log


@pytest.mark.parametrize("mode", ["plain", "dag"])
def test_a_user_module_path_holding_a_comma_is_refused(
    mode: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan = service_plan(() if mode == "plain" else (gpu_spec("web"),))
    mods = tmp_path / "mods"
    mods.mkdir()
    bad, good = mods / "a,b.py", mods / "ab.py"
    bad.write_text("X = 1\n")
    good.write_text("X = 1\n")
    msg = refused(monkeypatch, plan, site="generic", log_dir=tmp_path / "a", user_modules=[bad])
    assert "a,b.py" in msg or "user_modules" in msg, msg
    _, fake = submit(monkeypatch, plan, site="generic", log_dir=tmp_path / "b", user_modules=[good])
    assert len(submitted(fake.log)) == 1, "control: the same module without the comma submits"


# ---- the placeholder result --------------------------------------------------------------------------


def driver_sh_lines(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[list[str], int]:
    """The plain driver job's ``driver.sh`` lines and the index of its ``exec`` line."""
    _, fake = submit(monkeypatch, service_plan(()), site="generic", log_dir=tmp_path / "logs")
    (desc,) = submitted(fake.log)
    script = Path(desc.get("initialdir") or tmp_path / "logs", desc["executable"])
    lines = script.read_text().splitlines()
    (i,) = [n for n, line in enumerate(lines) if line.lstrip().startswith("exec ")]
    assert f"-m {DRIVER_MODULE}" in lines[i], lines[i]
    return lines, i


def test_driver_sh_writes_the_placeholder_before_it_execs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lines, i = driver_sh_lines(tmp_path, monkeypatch)
    assert any("result.pkl" in line for line in lines[:i]), lines
    assert any("driver.log" in line for line in lines[:i]), lines


@needs_sh
def test_the_placeholder_loads_as_a_runtime_error_naming_the_log(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lines, i = driver_sh_lines(tmp_path, monkeypatch)
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    (scratch / "pre-exec.sh").write_text("\n".join(lines[:i]) + "\n")
    # its status is the last pre-exec command's (the env.tgz test gives 1 without one): not asserted
    proc = subprocess.run(["sh", "pre-exec.sh"], cwd=scratch, capture_output=True, timeout=60, check=False)
    assert (scratch / "result.pkl").is_file(), (lines[:i], proc.stderr)
    ok, exc = pickle.loads((scratch / "result.pkl").read_bytes())
    assert ok is False and type(exc) is RuntimeError, repr(exc)
    assert "driver.log" in str(exc) and PLACEHOLDER_TEXT in str(exc), str(exc)
    assert (scratch / "driver.log").is_file(), "a missing driver.log holds the job like a missing result.pkl"


# ---- the run directory -----------------------------------------------------------------------------------


def test_each_dag_submission_gets_a_new_run_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    log_dir = tmp_path / "logs"
    plan = service_plan((gpu_spec("web"),))
    first, _ = submit(monkeypatch, plan, site="generic", log_dir=log_dir)
    run1 = Path(first.log_dir)
    assert run1.parent == log_dir and re.fullmatch(r"graphed-[0-9a-f]{8}", run1.name), run1
    names = {p.name for p in run1.iterdir()}
    assert {"run.dag", "driver.sub", "svc0.sub", "service-svc0", "run.json", "plan.pkl", "driver.sh"} <= names
    batch = read_sub(run1 / "driver.sub")["jobbatchname"].strip('"')
    assert run1.name == "graphed-" + batch.removeprefix("graphed-driverless-"), (run1.name, batch)
    before = tree_bytes(run1)
    second, fake = submit(monkeypatch, plan, site="generic", log_dir=log_dir)
    run2 = Path(second.log_dir)
    assert run2 != run1 and run2.parent == log_dir, (run1, run2)
    assert sorted(log_dir.iterdir()) == sorted([run1, run2]), "a submission wrote outside its run directory"
    assert tree_bytes(run1) == before, "the second submission changed the first run's files"
    (call,) = logged(fake.log, "from_dag")
    assert Path(call[1]) == run2 / "run.dag"


REFUSALS = ["no-worker-ports", "lxplus-outside-afs", "module-outside-root", "input-outside-root"]


@pytest.mark.parametrize("case", REFUSALS)
def test_a_dag_is_refused_before_any_bindings_call(
    case: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, outside = tmp_path / "root", tmp_path / "outside"
    (outside / "models").mkdir(parents=True)
    (outside / "models" / "model.onnx").write_text("m")
    (outside / "mod.py").write_text("X = 1\n")
    root.mkdir()
    site_copy(monkeypatch, "m68b-root", "generic", name="m68b-root", job_root=str(root))
    kwargs: dict[str, Any] = {"site": "m68b-root", "log_dir": root / "logs"}
    specs: tuple[Any, ...] = (gpu_spec("web"),)
    if case == "no-worker-ports":
        site_copy(monkeypatch, "m68b-noports", "generic", name="m68b-noports", worker_ports=None)
        kwargs = {"site": "m68b-noports", "log_dir": tmp_path / "logs"}
        words = ["worker_ports"]
    elif case == "lxplus-outside-afs":
        kwargs = {
            "site": "lxplus",
            "image": IMAGE,
            "env": fake_venv(tmp_path / "env"),
            "log_dir": tmp_path / "logs",
        }
        words = ["/afs", "log_dir"]
    elif case == "module-outside-root":
        kwargs["user_modules"] = [outside / "mod.py"]
        words = [str(outside / "mod.py"), "job_root", str(root)]
    else:
        specs = (gpu_spec("web", inputs=(str(outside / "models"),)),)
        words = [str(outside / "models"), "job_root", str(root)]
    msg = refused(monkeypatch, service_plan(specs), **kwargs)
    assert all(word in msg for word in words), (words, msg)


def test_paths_under_job_root_submit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "root"
    (root / "models").mkdir(parents=True)
    (root / "models" / "model.onnx").write_text("m")
    (root / "mod.py").write_text("X = 1\n")
    site_copy(monkeypatch, "m68b-root", "generic", name="m68b-root", job_root=str(root))
    spec = gpu_spec("web", inputs=(str(root / "models"),))
    _, fake = submit(
        monkeypatch,
        service_plan((spec,)),
        site="m68b-root",
        log_dir=root / "logs",
        user_modules=[root / "mod.py"],
    )
    assert len(logged(fake.log, "from_dag")) == 1 and len(submitted(fake.log)) == 1, fake.log


@needs_symlinks
def test_the_job_root_check_is_lexical(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root, outside = tmp_path / "root", tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    (outside / "model.onnx").write_text("m")
    (root / "model.onnx").symlink_to(outside / "model.onnx")
    site_copy(monkeypatch, "m68b-root", "generic", name="m68b-root", job_root=str(root))
    spec = gpu_spec("web", inputs=(str(root / "model.onnx"),))
    _, fake = submit(monkeypatch, service_plan((spec,)), site="m68b-root", log_dir=root / "logs")
    assert len(logged(fake.log, "from_dag")) == 1 and len(submitted(fake.log)) == 1, fake.log


# ---- in the driver job ---------------------------------------------------------------------------------


def in_job_run(site: str, tmp_path: Path, *, dag: bool = False) -> dict[str, Any]:
    """A ``run.json`` as the submitter writes it: m68a's keys, plus ``announce_only``/``dag_dir`` for a DAG."""
    run: dict[str, Any] = {
        "pilots": "local",
        "n_pilots": 1,
        "site": site,
        "image": None,
        "log_dir": str(tmp_path / "pilots"),
        "request_memory_mb": 1024,
        "min_pilots": 1,
        "retries": 0,
        "max_in_flight": 1,
        "schedd_locate": None,
        "user_modules": [],
        "endpoints": {},
    }
    if dag:
        (tmp_path / "dag").mkdir()
        run["announce_only"] = {"web": "svc0"}
        run["dag_dir"] = str(tmp_path / "dag")
    return run


@contextmanager
def in_job_runner(run: dict[str, Any], job: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    """``driver._runner`` as the driver job calls it: a machine ad whose ``Machine`` is ``localhost``, no
    bindings; closed at exit."""
    job.mkdir()
    monkeypatch.setitem(sys.modules, "htcondor2", None)
    monkeypatch.setenv("_CONDOR_MACHINE_AD", str(write_machine_ad(job / "machine.ad", MACHINE)))
    with open(job / "driver.log", "a") as log:
        runner = run_bounded(lambda: driver_api()._runner(run, job, log))
    try:
        yield runner
    finally:
        run_bounded(runner.close)


def in_job_site(monkeypatch: pytest.MonkeyPatch) -> tuple[int, int]:
    """A generic row whose ``worker_ports`` is a free range apart from its ``driver_ports``."""
    ports = free_range(3)
    site_copy(monkeypatch, "m68b-injob", "generic", name="m68b-injob", worker_ports=ports)
    return ports


def test_the_driver_job_publishes_an_announce_secret_then_its_url(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    low, high = in_job_site(monkeypatch)
    replaced: list[Path] = []
    real_replace = os.replace

    def spy_replace(src: Any, dst: Any, *args: Any, **kwargs: Any) -> None:
        replaced.append(Path(dst))
        real_replace(src, dst, *args, **kwargs)

    monkeypatch.setattr(os, "replace", spy_replace)
    dag_dir = tmp_path / "dag"
    with in_job_runner(in_job_run("m68b-injob", tmp_path, dag=True), tmp_path / "job", monkeypatch) as runner:
        assert [p.name for p in replaced if p.parent == dag_dir] == ["graphed-secret", "driver.url"], replaced
        secret_file = dag_dir / "graphed-secret"
        if sys.platform != "win32":
            assert stat.S_IMODE(secret_file.stat().st_mode) == 0o600
        url = (dag_dir / "driver.url").read_text().strip().rstrip("/")
        parts = urlsplit(url)
        assert parts.hostname == MACHINE and parts.port is not None and low <= parts.port <= high, url
        announce = bytes.fromhex(secret_file.read_text().strip())
        pilots = bytes.fromhex((Path(runner.backend.launcher.log_dir) / "graphed-secret").read_text().strip())
        signed_by_announce, signed_by_pilots = tmp_path / "announce.marker", tmp_path / "pilots.marker"
        assert post(url + "/result", pickle.dumps(MarkerBomb(str(signed_by_announce))), announce) == 403
        post(url + "/result", pickle.dumps(MarkerBomb(str(signed_by_pilots))), pilots)
        assert signed_by_pilots.exists(), "control: a pickle the pilots' secret signs is loaded"
        assert not signed_by_announce.exists(), "the announce secret signed a pickle"
    # a run.json without the two keys builds as m67's
    with in_job_runner(in_job_run("m68b-injob", tmp_path / "p"), tmp_path / "plain", monkeypatch) as runner:
        assert runner.backend.advertise_host == "127.0.0.1"
        assert not hasattr(runner.backend, "host_service")
        assert not (tmp_path / "plain" / "driver.url").exists()


def test_the_in_job_backend_resolves_a_service_node_by_its_announce(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    in_job_site(monkeypatch)
    dag_dir = tmp_path / "dag"
    spec = gpu_spec("web", timeout_s=60.0)
    run = in_job_run("m68b-injob", tmp_path, dag=True)
    with ok_server() as port, in_job_runner(run, tmp_path / "job", monkeypatch) as runner:
        backend = runner.backend
        url = (dag_dir / "driver.url").read_text().strip().rstrip("/")
        secret = bytes.fromhex((dag_dir / "graphed-secret").read_text().strip())
        body = f"svc0 127.0.0.1:{port} {SERVICE_IDENTITY}".encode()
        endpoint = f"http://127.0.0.1:{port}"
        assert post(url + "/announce", body, secret) == 200
        got = run_bounded(lambda: backend.host_service(spec, "m68b-scope"))
        assert tuple(got) == (endpoint, SERVICE_IDENTITY, "svc0")
        with pytest.raises(ValueError, match="announce_only"):
            run_bounded(lambda: backend.host_service(gpu_spec("other"), "m68b-scope"))
        assert post(url + "/announce", body, secret) == 200
        service_set = services_api().ServiceSet((spec,), backend)
        with caplog.at_level(logging.INFO, logger=SERVICES_LOGGER):
            endpoints = run_bounded(service_set.start)
            try:
                assert dict(endpoints) == {"web": endpoint}
            finally:
                run_bounded(service_set.close)
    statuses = [r.status for r in caplog.records if r.name == SERVICES_LOGGER and hasattr(r, "status")]
    assert [(s.name, s.leg, s.host, s.identity) for s in statuses] == [
        ("web", "managed", "cluster", SERVICE_IDENTITY)
    ]


# ---- RunHandle(dag=True) ---------------------------------------------------------------------------------


def dag_handle(log_dir: Path, *, site: str = "generic", dag: bool = True) -> Any:
    return htcondor_api().RunHandle(
        site=site, schedd=FAKE_SCHEDD, cluster=DAGMAN, log_dir=log_dir, submitted_at=0.0, dag=dag
    )


def names_the_driver_node(constraint: str) -> bool:
    return bool(
        re.search(rf"DAGManJobId\s*(==|=\?=)\s*{DAGMAN}\b", constraint)
        and re.search(r'DAGNodeName\s*(==|=\?=)\s*"driver"', constraint)
    )


def assert_no_dag_counters(log: list[tuple[Any, ...]]) -> None:
    for entry in logged(log, "query") + logged(log, "history"):
        assert "DAG_" not in entry[1] and not any("DAG_" in str(a) for a in entry[2]), entry


def driver_try(cluster: int, exit_code: int | None) -> dict[str, Any]:
    ad: dict[str, Any] = {"ClusterId": cluster, "JobStatus": 4, "DAGNodeName": "driver"}
    if exit_code is None:
        ad.update(ExitBySignal=True, ExitSignal=9)
    else:
        ad["ExitCode"] = exit_code
    return ad


# driver-node history in recorded order (Q-03: not ClusterId order), and the outcome of the latest try
OUTCOMES: dict[str, tuple[list[dict[str, Any]], str]] = {
    "latest-last-exit-0": ([driver_try(9, 1), driver_try(10, 0)], "done"),
    "latest-between-exit-0": ([driver_try(9, 1), driver_try(11, 0), driver_try(10, 1)], "done"),
    "latest-first-exit-1": ([driver_try(10, 1), driver_try(9, 0)], "failed"),
    "latest-killed": ([driver_try(9, 0), driver_try(10, None)], "failed"),
    "no-driver-ad": ([], "failed"),
}


@pytest.mark.parametrize("case", sorted(OUTCOMES))
def test_a_finished_dag_takes_its_outcome_from_the_latest_driver_try(
    case: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tries, expected = OUTCOMES[case]
    dagman = {"ClusterId": DAGMAN, "JobStatus": 4, "ExitCode": 1, "JobUniverse": 7}  # never the outcome
    schedd = DagSchedd(dagman_history=[dagman], node_history=tries)
    record_bindings(monkeypatch, schedd)
    assert run_bounded(dag_handle(tmp_path).status) == expected
    node_history = [e for e in logged(schedd.log, "history") if is_node_constraint(e[1])]
    assert node_history and all(names_the_driver_node(e[1]) and e[3] == 1 for e in node_history), node_history
    assert_no_dag_counters(schedd.log)


def test_a_held_driver_node_reads_held_and_wait_times_out_naming_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ads = {
        "dagman": [{"ClusterId": DAGMAN, "JobStatus": 2, "DAG_JobsHeld": 0}],
        "node": [{"ClusterId": 9, "JobStatus": 5, "HoldReasonCode": 12}],
    }
    schedd = DagSchedd(**ads)
    record_bindings(monkeypatch, schedd)
    handle = dag_handle(tmp_path)
    assert run_bounded(handle.status) == "held"
    node_queries = [e for e in logged(schedd.log, "query") if is_node_constraint(e[1])]
    assert node_queries and all(names_the_driver_node(e[1]) for e in node_queries), schedd.log
    with pytest.raises(TimeoutError, match="held"):
        run_bounded(lambda: handle.wait(timeout=0.3, poll_s=0.02), 60)
    assert_no_dag_counters(schedd.log)
    record_bindings(monkeypatch, DagSchedd(**ads))
    assert run_bounded(dag_handle(tmp_path, dag=False).status) == "running", "control: m67's mapping"


RUNNING_NODE = [{"ClusterId": 9, "JobStatus": 2}]
DAGMAN_STATUS: dict[str, tuple[int, list[dict[str, Any]], str]] = {
    "1-no-driver-ad": (1, [], "queued"),
    "2-driver-running": (2, RUNNING_NODE, "running"),
    "3-removed": (3, RUNNING_NODE, "removed"),
    "5-held": (5, [], "held"),
    "6": (6, [], "running"),
    "7": (7, [], "running"),
}


@pytest.mark.parametrize("case", sorted(DAGMAN_STATUS))
def test_every_dagman_job_status_is_mapped(
    case: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    status, node, expected = DAGMAN_STATUS[case]
    schedd = DagSchedd(dagman=[{"ClusterId": DAGMAN, "JobStatus": status, "HoldReasonCode": 3}], node=node)
    record_bindings(monkeypatch, schedd)
    assert run_bounded(dag_handle(tmp_path).status) == expected
    assert_no_dag_counters(schedd.log)


def test_a_finished_dag_still_queued_on_a_spooled_site_is_read_without_a_retrieve(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    value = ExecResult(value=("m68b-b2",), n_partitions=1, n_combines=0, stopped=StopReason.EXHAUSTED)
    (tmp_path / "result.pkl").write_bytes(pickle.dumps((True, value)))
    ads = {
        "dagman": [{"ClusterId": DAGMAN, "JobStatus": 4, "ExitCode": 1}],
        "node_history": [driver_try(9, 0)],
    }
    assert htcondor_api().SITES["lxplus"].spool is True
    schedd = DagSchedd(**ads)
    record_bindings(monkeypatch, schedd)
    handle = dag_handle(tmp_path, site="lxplus")
    assert run_bounded(handle.status) == "done"
    assert run_bounded(handle.result) == value
    assert logged(schedd.log, "retrieve") == [], schedd.log
    control = DagSchedd(**ads)
    record_bindings(monkeypatch, control)
    run_bounded(dag_handle(tmp_path, site="lxplus", dag=False).result)
    assert len(logged(control.log, "retrieve")) == 1, "control: m67's plain handle retrieves here"


def test_a_failed_dag_without_a_result_names_the_dagman_log(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    schedd = DagSchedd(dagman_history=[{"ClusterId": DAGMAN, "JobStatus": 4, "ExitCode": 1}], node_history=[])
    record_bindings(monkeypatch, schedd)
    handle = dag_handle(tmp_path)
    assert run_bounded(handle.status) == "failed"
    with pytest.raises(RuntimeError, match=r"run\.dag\.dagman\.out"):
        run_bounded(handle.result)


def test_an_m67_saved_handle_loads_as_a_plain_job(tmp_path: Path) -> None:
    api = htcondor_api()
    saved = tmp_path / "m67-handle.json"
    fields = {
        "site": "generic",
        "schedd": FAKE_SCHEDD,
        "cluster": 12,
        "log_dir": str(tmp_path),
        "submitted_at": 1.0,
    }
    saved.write_text(json.dumps(fields))
    assert api.RunHandle.load(saved).dag is False
    dag_handle(tmp_path).save(tmp_path / "dag-handle.json")
    assert api.RunHandle.load(tmp_path / "dag-handle.json").dag is True


# ---- the DAGMan description (test-htcondor) ----------------------------------------------------------


def description_lines(desc: Mapping[str, Any], subs: Mapping[str, str]) -> list[str]:
    lines = []
    for key, value in desc.items():
        line = f"{key} = {value}"
        for old, new in subs.items():
            line = line.replace(old, new)
        lines.append(line)
    return sorted(lines)


@needs_dagman
def test_the_dag_description_is_the_probe_s(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    real = pytest.importorskip(
        "htcondor2", reason="the fixture comparison needs real bindings (test-htcondor)"
    )
    dagman = shutil.which("condor_dagman")
    assert dagman is not None
    handle, fake = submit(
        monkeypatch, service_plan((gpu_spec("web"),)), site="generic", log_dir=tmp_path / "logs", real=real
    )
    assert len(logged(fake.log, "from_dag")) == 1, f"no DAG was submitted: {fake.log}"
    (desc,) = submitted(fake.log)
    csd = str(real.version()).replace(" ", "' '")  # the shell-escaped form -CsdVersion carries
    got = description_lines(desc, {str(handle.log_dir): "<DAG>", csd: "<CSD>", dagman: "<DAGMAN>"})
    fixture = dict(
        line.partition(" = ")[::2] for line in (DATA_DIR / "from_dag-generic.txt").read_text().splitlines()
    )
    want = description_lines(fixture, {PROBE_DAG_DIR: "<DAG>", PROBE_CSD: "<CSD>", PROBE_DAGMAN: "<DAGMAN>"})
    assert got == want
