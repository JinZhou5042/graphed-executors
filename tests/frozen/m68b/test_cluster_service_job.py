"""m68b B1: ``ServiceJob``, ``announce.py`` and the attached backend's ``host_service``/``release_service``
(plan-services.md §3.3 "B1 — attached cluster hosting", the ``test_cluster_service_job.py`` row).

Three groups. The submit keys and the job dir ``ServiceJob`` builds, and ``host_service`` over a recorded
schedd (``record_bindings``), in process on every OS. ``announce.py``'s SIGTERM handler and its ``_Stop``
exit path, in process on every OS (the module keeps the prototype's ``main``/``serve``/``on_sigterm``/
``_Stop``/``CHILD``). The subprocess legs, POSIX only (Windows delivers SIGTERM as ``TerminateProcess``;
``announce.py`` only runs in a Linux job): ``python -m graphed_executors.htcondor_backend.announce
service.json`` in a job dir laid out as the job's, against a real ``TaskServer``, with ``$_CONDOR_MACHINE_AD``
naming a file whose ``Machine`` is ``localhost`` and identities compared with ``host_identity()`` under
that env.
"""

from __future__ import annotations

import ast
import dataclasses
import functools
import json
import os
import re
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

import pytest
from m68b_harness import (
    CHILD_SCRIPT,
    MARGIN_S,
    REAP_S,
    AnnounceRun,
    RecordingSchedd,
    announce_api,
    announce_body,
    announce_env,
    backend_api,
    child_argv,
    child_starts,
    closed_port,
    cluster_api,
    fake_venv,
    free_range,
    get_within,
    htcondor_api,
    http_get,
    http_status,
    in_background,
    input_entries,
    job_config,
    keys_of,
    launch_api,
    pid_gone,
    post_announce,
    record_bindings,
    run_bounded,
    server_api,
    services_api,
    spy_method,
    submits,
    task_server,
    wait_announce,
    wait_for,
    web_spec,
    wildcard_listener,
    write_job,
    write_machine_ad,
)

BOUND_S = 120.0
URL = "http://login.m68b.example:10007"  # the task server a ServiceJob announces to
SECRET = bytes(range(32))  # the announce secret a ServiceJob carries
JOB_FILES = {"announce.py", "service.json", "graphed-secret", "env.tgz", "service"}
LAUNCHER_IMAGE = "/cvmfs/unpacked.example/launcher:1"
RECIPE_IMAGE = "/cvmfs/unpacked.example/recipe:2"

posix = pytest.mark.skipif(
    sys.platform == "win32", reason="announce.py runs in a Linux job; Windows has no POSIX SIGTERM"
)


def _can_symlink() -> bool:
    """Whether this host creates file and directory symlinks (Windows needs the symlink privilege)."""
    with tempfile.TemporaryDirectory() as d:
        (Path(d) / "dir").mkdir()
        (Path(d) / "file").write_text("f")
        try:
            os.symlink(Path(d) / "dir", Path(d) / "dir-link", target_is_directory=True)
            os.symlink(Path(d) / "file", Path(d) / "file-link")
        except OSError:
            return False
    return True


symlinks = pytest.mark.skipif(not _can_symlink(), reason="cannot create a symlink here")


def _defpath_python() -> tuple[str | None, str]:
    """The default-path ``python3`` and the ``sys.executable`` it prints when run without ``PATH``."""
    defpy = shutil.which("python3", path=os.defpath)
    if not defpy:
        return defpy, ""
    no_path = {k: v for k, v in os.environ.items() if k != "PATH"}
    done = subprocess.run(
        [defpy, "-c", "import sys; print(sys.executable)"],
        env=no_path,
        capture_output=True,
        text=True,
        timeout=60,
    )
    return defpy, done.stdout.strip()


defpath_python3, defpath_printed = _defpath_python()
defpath_python_distinct = pytest.mark.skipif(
    not (defpath_printed and os.path.isabs(defpath_printed) and defpath_printed != sys.executable),
    reason=f"the default-path python3 ({defpath_python3}) does not tell {defpath_printed!r} from this interpreter",
)


def generic(**changes: Any) -> Any:
    return dataclasses.replace(htcondor_api().SITES["generic"], **changes)


def started_pilots(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    profile: Any,
    *,
    schedd: RecordingSchedd | None = None,
    log_dir: Path | str | None = None,
    image: str | None = None,
) -> tuple[Any, RecordingSchedd]:
    """A ``CondorPilots`` on ``profile`` started under the recorder (a fake venv when it ships one)."""
    schedd = schedd if schedd is not None else RecordingSchedd()
    record_bindings(monkeypatch, schedd)
    venv = tmp_path / "venv"
    if profile.ship_env and not venv.exists():
        fake_venv(venv)
    pilots = launch_api().CondorPilots(
        profile,
        image=image,
        log_dir=log_dir if log_dir is not None else tmp_path / "logs",
        env=venv if profile.ship_env else None,
    )
    run_bounded(lambda: pilots.start("http://127.0.0.1:1", os.urandom(32), 1), BOUND_S)
    return pilots, schedd


def service_job(spec: Any, pilots: Any, key: str = "k1", **kwargs: Any) -> Any:
    if "watch" not in kwargs:
        kwargs = {"url": URL, "secret": SECRET, **kwargs}
    return cluster_api().ServiceJob(spec, pilots, key=key, **kwargs)


def submitted(job: Any, schedd: RecordingSchedd) -> tuple[dict[str, Any], Path]:
    """``job.submit()`` under the recorder: its description and its ``initialdir``."""
    before = len(submits(schedd.log))
    run_bounded(job.submit, BOUND_S)
    descs = submits(schedd.log)
    assert len(descs) == before + 1, f"submit() made {len(descs) - before} schedd.submit calls"
    desc = descs[-1]
    return desc, Path(desc["initialdir"])


def files_of(job: Any, root: Path) -> dict[str, Any]:
    root.mkdir(parents=True, exist_ok=True)
    keys: dict[str, Any] = run_bounded(lambda: dict(job.files(root)), BOUND_S)
    return keys


def symlink(target: Path, link: Path) -> None:
    os.symlink(target, link, target_is_directory=target.is_dir())


def names_code(text: str, code: int) -> bool:
    """``code`` appears as a number of its own (not inside a port, pid, address or clock time)."""
    return re.search(rf"(?<![\w:.]){code}(?![\w:.])", text) is not None


def execs_announce_with(script: str, python: str) -> bool:
    """An ``exec <python> [./]announce.py`` line, ``<python>`` literal or a variable the script sets to it."""
    literal = rf"[\"']?{re.escape(python)}[\"']?"
    line = re.search(
        rf"^\s*exec\s+(?:{literal}|\"?\$\{{?(\w+)\}}?\"?)\s+[\"']?(?:\./)?announce\.py\b",
        script,
        re.MULTILINE,
    )
    if line is None or line[1] is None:
        return line is not None
    return re.search(rf"^\s*{line[1]}={literal}\s*$", script, re.MULTILINE) is not None


# ---- the attribute ------------------------------------------------------------------------------------------


def test_host_service_exists_iff_condor_pilots_and_a_cluster_host(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    record_bindings(monkeypatch, RecordingSchedd())
    launch, backend_mod, sites = launch_api(), backend_api(), htcondor_api().SITES
    cases = [
        ("condor generic", lambda: launch.CondorPilots(generic(), log_dir=tmp_path / "a"), {}, True),
        (
            "condor cluster only",
            lambda: launch.CondorPilots(generic(service_ports=None), log_dir=tmp_path / "b"),
            {},
            True,
        ),
        (
            "condor driver only",
            lambda: launch.CondorPilots(generic(worker_ports=None), log_dir=tmp_path / "c"),
            {},
            False,
        ),
        ("local pilots", lambda: launch.LocalPilots(), {}, False),
        (
            "condor in a driver job",
            lambda: launch.CondorPilots(generic(), log_dir=tmp_path / "d"),
            {"in_job": sites["generic"]},
            False,
        ),
    ]
    wrong = []
    for label, make, kwargs, expected in cases:
        launcher = make()
        n = 0 if isinstance(launcher, launch.LocalPilots) else 1
        build = functools.partial(
            backend_mod.HTCondorBackend, launcher, n, host="127.0.0.1", port_range=(0, 0), **kwargs
        )
        backend = run_bounded(build, BOUND_S)
        try:
            has = callable(getattr(backend, "host_service", None)) and callable(
                getattr(backend, "release_service", None)
            )
            if has != expected:
                wrong.append((label, has))
        finally:
            run_bounded(backend.close, BOUND_S)
    assert wrong == [], f"host_service/release_service present (True) or absent (False) wrongly: {wrong}"


# ---- ServiceJob: the job dir and the submit keys --------------------------------------------------------------


def test_files_mirror_each_input_under_service_by_basename(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pilots, _ = started_pilots(monkeypatch, tmp_path, generic())
    src = tmp_path / "src"
    (src / "models" / "a").mkdir(parents=True)
    (src / "models" / "a" / "b.txt").write_text("b")
    (src / "models" / "c.txt").write_text("c")
    (src / "weights.bin").write_text("w")
    spec = web_spec(inputs=(str(src / "models"), str(src / "weights.bin")))
    keys = files_of(service_job(spec, pilots), tmp_path / "job")
    service = tmp_path / "job" / "service"
    assert sorted(os.listdir(service)) == ["models", "weights.bin"]
    for rel in ("weights.bin", "models/a/b.txt", "models/c.txt"):
        link = service / rel
        assert link.is_symlink(), f"{rel} is not a symlink"
        assert os.path.isabs(os.readlink(link)), f"{rel} links to a relative path"
        assert os.path.samefile(link, src / rel)
    for rel in ("models", "models/a"):
        assert (service / rel).is_dir() and not (service / rel).is_symlink(), f"{rel} is not a real directory"
    entries = input_entries(keys)
    assert entries.count("service") == 1, entries
    assert not {"models", "weights.bin"} & set(entries), entries
    assert all(not os.path.isabs(e) for e in entries), entries
    for name in ("service.sh", "service.json", "announce.py", "graphed-secret"):
        assert (tmp_path / "job" / name).is_file(), f"files() wrote no {name}"


def test_inputs_named_like_job_files_land_only_under_service(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pilots, _ = started_pilots(monkeypatch, tmp_path, generic())
    src = tmp_path / "src"
    src.mkdir()
    names = ["service.json", "tmp", ".job.ad", ".machine.ad", "a,b"]
    for name in names:
        if name == "tmp":
            (src / name).mkdir()
            (src / name / "x.txt").write_text("x")
        else:
            (src / name).write_text(f"user {name}")
    spec = web_spec(inputs=tuple(str(src / n) for n in names))
    job_dir = tmp_path / "job"
    keys = files_of(service_job(spec, pilots), job_dir)
    assert sorted(os.listdir(job_dir / "service")) == sorted(names)
    for name in names:
        placed = job_dir / "service" / name
        assert placed.exists(), f"input {name} was not placed under service/"
    assert (job_dir / "service" / "tmp" / "x.txt").is_file()
    assert "argv" in json.loads((job_dir / "service.json").read_text()), (
        "the job's own service.json was replaced"
    )
    for name in ("tmp", ".job.ad", ".machine.ad", "a,b"):
        assert not (job_dir / name).exists(), f"input {name} landed beside the job's files"
    entries = input_entries(keys)
    assert entries.count("service") == 1 and set(entries) <= JOB_FILES, entries


def test_every_transfer_entry_is_relative_so_a_comma_in_log_dir_splits_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    schedd = RecordingSchedd()
    plain, _ = started_pilots(monkeypatch, tmp_path, generic(), schedd=schedd, log_dir=tmp_path / "plain")
    comma, _ = started_pilots(monkeypatch, tmp_path, generic(), schedd=schedd, log_dir=tmp_path / "lo,gs")
    (tmp_path / "model.txt").write_text("m")
    spec = web_spec(inputs=(str(tmp_path / "model.txt"),))
    plain_desc, _ = submitted(service_job(spec, plain), schedd)
    comma_desc, comma_dir = submitted(service_job(spec, comma), schedd)
    assert "," in str(comma_dir)
    assert input_entries(comma_desc) == input_entries(plain_desc)
    assert comma_desc["transfer_input_files"] == plain_desc["transfer_input_files"]
    assert all(not os.path.isabs(e) for e in input_entries(comma_desc)), comma_desc["transfer_input_files"]


@pytest.mark.parametrize(
    ("resources", "expected"),
    [
        (
            {"cpus": 2.0, "memory_mb": 1536.0, "gpus": 1.0},
            {"request_cpus": "2", "request_memory": "1536", "request_gpus": "1"},
        ),
        ({"gpus": 0.0}, {"request_cpus": "1", "request_memory": "2048", "request_gpus": None}),
    ],
    ids=["resources", "defaults"],
)
def test_the_submit_keys(
    resources: dict[str, float],
    expected: dict[str, str | None],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pilots, schedd = started_pilots(monkeypatch, tmp_path, generic())
    pilot_secret = (Path(pilots.log_dir) / "graphed-secret").read_text().strip()
    desc, initialdir = submitted(service_job(web_spec(resources=resources), pilots), schedd)
    assert os.path.isabs(desc["initialdir"])
    assert os.path.samefile(initialdir, Path(pilots.log_dir) / "service-k1")
    assert os.path.isabs(desc["executable"])
    assert os.path.samefile(desc["executable"], initialdir / "service.sh")
    assert desc["arguments"] == "service.json"
    assert (desc["output"], desc["error"], desc["log"]) == ("service.out", "service.err", "service.log")
    assert desc["JobBatchName"] == "graphed-service-k1"
    assert desc["job_max_vacate_time"] == "30"
    assert desc["transfer_output_files"] == '""'
    for name, value in expected.items():
        assert desc.get(name) == value, (name, desc.get(name))
    leaked = [
        (k, v) for k, v in desc.items() if URL in str(v) or SECRET.hex() in str(v) or pilot_secret in str(v)
    ]
    assert leaked == [], f"a secret or the task server's url is in the job ad: {leaked}"
    assert (initialdir / "graphed-secret").read_text().strip() == SECRET.hex()
    assert [e[2] for e in schedd.log if e[0] == "submit"][-1] == 1, "not one cluster of one job"


def test_service_json_carries_the_plan_fields_and_no_secret(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    server = server_api()
    monkeypatch.setattr(server, "LEASE_S", 7.5)
    monkeypatch.setattr(server, "POLL_S", 2.5)
    pilots, _ = started_pilots(monkeypatch, tmp_path, generic(worker_ports=(12000, 12050)))
    spec = web_spec(check="tcp", timeout_s=45.0, env={"M68B_RECIPE_VAR": "r"})
    job_dir = tmp_path / "job"
    files_of(service_job(spec, pilots), job_dir)
    cfg = json.loads((job_dir / "service.json").read_text())
    assert cfg["ports"] == [12000, 12050]
    assert (cfg["lease_s"], cfg["beat_s"]) == (7.5, 2.5)
    assert (cfg["key"], cfg["url"], cfg["watch"], cfg["check"]) == ("k1", URL, None, "tcp")
    assert cfg["timeout_s"] == 45.0
    assert cfg["argv"] == list(spec.launch.argv) and cfg["env"] == {"M68B_RECIPE_VAR": "r"}
    assert cfg["python"] == sys.executable
    assert SECRET.hex() not in json.dumps(cfg)
    secret_file = job_dir / "graphed-secret"
    assert secret_file.read_text().strip() == SECRET.hex()
    if sys.platform != "win32":
        assert stat.S_IMODE(secret_file.stat().st_mode) == 0o600


@pytest.mark.parametrize(
    ("site", "ship_env", "recipe_image", "python", "image_key", "ships_env"),
    [
        ("generic", True, None, "./env/bin/python", None, True),
        ("generic", False, None, "SYS", None, False),
        ("lxplus", True, None, "./env/bin/python", f'"{LAUNCHER_IMAGE}"', True),
        ("lxplus", True, RECIPE_IMAGE, "python3", f'"{RECIPE_IMAGE}"', False),
    ],
    ids=["ship-env", "no-ship-env", "lxplus-imageless", "lxplus-imaged"],
)
def test_the_interpreter_image_and_env_per_recipe(
    site: str,
    ship_env: bool,
    recipe_image: str | None,
    python: str,
    image_key: str | None,
    ships_env: bool,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    python = sys.executable if python == "SYS" else python
    profile = dataclasses.replace(htcondor_api().SITES[site], ship_env=ship_env)
    image = LAUNCHER_IMAGE if site == "lxplus" else None
    pilots, schedd = started_pilots(monkeypatch, tmp_path, profile, image=image)
    desc, initialdir = submitted(service_job(web_spec(image=recipe_image), pilots), schedd)
    assert desc.get("MY.SingularityImage") == image_key
    assert ("env.tgz" in input_entries(desc)) is ships_env, desc["transfer_input_files"]
    if ships_env:
        link = initialdir / "env.tgz"
        assert link.is_symlink() and os.path.isabs(os.readlink(link))
        assert os.path.samefile(link, Path(pilots.log_dir) / "env.tgz")
    assert json.loads((initialdir / "service.json").read_text())["python"] == python
    script = (initialdir / "service.sh").read_text()
    assert execs_announce_with(script, python), script
    assert [e[3] for e in schedd.log if e[0] == "submit"][-1] == profile.spool


def test_send_credential_is_dropped_on_an_attached_job_and_kept_in_watch_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lxplus = htcondor_api().SITES["lxplus"]
    pilots, schedd = started_pilots(monkeypatch, tmp_path, lxplus, image=LAUNCHER_IMAGE)
    assert submits(schedd.log)[0].get("MY.SendCredential") == "True", (
        "control: the pilots keep the credential"
    )
    desc, _ = submitted(service_job(web_spec(), pilots), schedd)
    assert "MY.SendCredential" not in desc
    watch = tmp_path / "dag"
    watch.mkdir()
    watch_dir = tmp_path / "svc0"
    keys = files_of(service_job(web_spec(), pilots, key="svc0", watch=str(watch)), watch_dir)
    assert keys.get("MY.SendCredential") == "True"
    assert not (watch_dir / "graphed-secret").exists()
    assert "graphed-secret" not in input_entries(keys)
    cfg = json.loads((watch_dir / "service.json").read_text())
    assert (cfg["url"], cfg["watch"]) == (None, str(watch))


def test_a_relative_log_dir_and_a_relative_input_resolve_where_they_were_written(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    a, b = tmp_path / "A", tmp_path / "B"
    a.mkdir()
    (b / "logs").mkdir(parents=True)
    (b / "logs" / "env.tgz").write_text("B's env")
    (b / "logs" / "graphed-secret").write_text("B's secret")
    (b / "data.txt").write_text("input")
    fake_venv(tmp_path / "venv")
    monkeypatch.chdir(a)
    pilots, schedd = started_pilots(monkeypatch, tmp_path, generic(ship_env=True), log_dir="logs")
    monkeypatch.chdir(b)
    _, initialdir = submitted(service_job(web_spec(inputs=("data.txt",)), pilots), schedd)
    assert initialdir.is_absolute()
    assert initialdir.resolve().is_relative_to((a / "logs").resolve()), initialdir
    assert os.path.samefile(initialdir / "env.tgz", a / "logs" / "env.tgz")
    links = [p for p in (initialdir / "service").rglob("*") if p.is_symlink()]
    assert links, "no input link under service/"
    assert all(os.path.isfile(p) for p in links), [(p, os.readlink(p)) for p in links]
    run_bounded(pilots.stop, BOUND_S)
    assert not (a / "logs" / "graphed-secret").exists(), "the pilots' secret was left behind"
    assert (b / "logs" / "graphed-secret").read_text() == "B's secret", "stop() unlinked another dir's secret"


@pytest.mark.parametrize(
    "case",
    [
        "missing",
        "shared-basename",
        pytest.param("dir-symlink", marks=symlinks),
        pytest.param("holds-dir-symlink", marks=symlinks),
    ],
)
def test_construction_refuses_a_bad_input_naming_it(
    case: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pilots, _ = started_pilots(monkeypatch, tmp_path, generic())
    src = tmp_path / "src"
    (src / "real").mkdir(parents=True)
    (src / "real" / "f.txt").write_text("f")
    if case == "missing":
        inputs, named = (str(src / "absent.onnx"),), "absent.onnx"
    elif case == "shared-basename":
        for d in ("x", "y"):
            (src / d).mkdir()
            (src / d / "model.pt").write_text(d)
        inputs, named = (str(src / "x" / "model.pt"), str(src / "y" / "model.pt")), "model.pt"
    elif case == "dir-symlink":
        symlink(src / "real", src / "linked")
        inputs, named = (str(src / "linked"),), "linked"
    else:
        (src / "models").mkdir()
        symlink(src / "real", src / "models" / "nested")
        inputs, named = (str(src / "models"),), "nested"
    with pytest.raises((ValueError, OSError)) as refused:
        service_job(web_spec(inputs=inputs), pilots)
    assert named in str(refused.value), str(refused.value)


@symlinks
def test_construction_accepts_a_directory_holding_a_file_symlink(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pilots, _ = started_pilots(monkeypatch, tmp_path, generic())
    (tmp_path / "models").mkdir()
    (tmp_path / "weights.txt").write_text("w")
    symlink(tmp_path / "weights.txt", tmp_path / "models" / "w.txt")
    keys = files_of(service_job(web_spec(inputs=(str(tmp_path / "models"),)), pilots), tmp_path / "job")
    assert os.path.isfile(tmp_path / "job" / "service" / "models" / "w.txt")
    assert input_entries(keys).count("service") == 1


@pytest.mark.parametrize(
    ("site", "ad", "retrieved"),
    [("generic", {"JobStatus": 2}, False), ("lxplus", {"JobStatus": 4}, True)],
    ids=["running-no-drain", "spooled-completed"],
)
def test_stop_removes_at_once_and_unlinks_only_the_job_s_secret(
    site: str, ad: dict[str, int], retrieved: bool, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    schedd = RecordingSchedd(queue=[[ad]])
    image = LAUNCHER_IMAGE if site == "lxplus" else None
    pilots, _ = started_pilots(monkeypatch, tmp_path, htcondor_api().SITES[site], schedd=schedd, image=image)
    job = service_job(web_spec(), pilots)
    _, initialdir = submitted(job, schedd)
    assert (initialdir / "graphed-secret").is_file()
    mark = len(schedd.log)
    run_bounded(job.stop, 10.0)
    tail = [e[0] for e in schedd.log[mark:] if e[0] in ("retrieve", "act")]
    assert tail == (["retrieve", "act"] if retrieved else ["act"]), schedd.log[mark:]
    assert "Remove" in next(e for e in schedd.log[mark:] if e[0] == "act")[1]
    assert not (initialdir / "graphed-secret").exists(), "stop() left the job's secret"
    assert (Path(pilots.log_dir) / "graphed-secret").is_file(), "stop() unlinked the pilots' secret"


def test_announce_py_is_stdlib_only_python_3_9_and_the_job_runs_the_module_s_code(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module_file = Path(announce_api().__file__)
    source = module_file.read_text()
    tree = ast.parse(source, feature_version=(3, 9))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            assert node.level == 0, f"relative import in announce.py: {ast.unparse(node)}"
            imported.add(str(node.module).split(".")[0])
    assert imported, "the parse found no import at all"
    assert imported <= set(sys.stdlib_module_names) | {"__future__"}, imported - set(sys.stdlib_module_names)
    pilots, _ = started_pilots(monkeypatch, tmp_path, generic())
    files_of(service_job(web_spec(), pilots), tmp_path / "job")
    assert (tmp_path / "job" / "announce.py").read_bytes() == module_file.read_bytes()


@posix
def test_service_sh_exits_3_naming_an_interpreter_it_cannot_find(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pilots, _ = started_pilots(monkeypatch, tmp_path, generic(ship_env=True))
    job_dir = tmp_path / "job"
    files_of(service_job(web_spec(), pilots), job_dir)
    done = subprocess.run(
        ["sh", "service.sh", "service.json"], cwd=job_dir, capture_output=True, text=True, timeout=60
    )
    assert done.returncode == 3, (done.returncode, done.stdout, done.stderr)
    assert "./env/bin/python" in done.stdout + done.stderr


# ---- announce.py in process, every OS -----------------------------------------------------------------------


def test_the_sigterm_handler_never_waits_and_ignores_further_sigterms(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mod = announce_api()
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    previous = signal.getsignal(signal.SIGTERM)
    calls: list[str] = []
    saved = mod.CHILD[0]
    mod.CHILD[0] = child
    try:
        with monkeypatch.context() as m:
            m.setattr(subprocess.Popen, "wait", lambda self, *a, **k: calls.append("wait"))
            m.setattr(subprocess.Popen, "poll", lambda self, *a, **k: calls.append("poll"))
            with pytest.raises(mod._Stop):
                mod.on_sigterm(signal.SIGTERM, None)
            ignored = signal.getsignal(signal.SIGTERM) is signal.SIG_IGN
    finally:
        mod.CHILD[0] = saved
        signal.signal(signal.SIGTERM, previous)
        child.kill()
        child.wait(30)
    assert ignored, "the handler left SIGTERM handled"
    assert calls == [], f"the SIGTERM handler called Popen.{calls}"


def _stop_path(mod: Any, child: Any, monkeypatch: pytest.MonkeyPatch) -> tuple[Any, list[tuple[int, int]]]:
    """``main()`` with ``serve`` raising ``_Stop`` over ``CHILD[0] = child``: its exit code and every
    ``os.kill`` it made (spied, not sent)."""
    kills: list[tuple[int, int]] = []

    def serve(*args: Any, **kwargs: Any) -> Any:
        raise mod._Stop()

    previous, saved = signal.getsignal(signal.SIGTERM), mod.CHILD[0]
    mod.CHILD[0] = child
    try:
        with monkeypatch.context() as m:
            m.setattr(mod, "serve", serve)
            m.setattr(os, "kill", lambda pid, sig: kills.append((pid, sig)))
            try:
                code = mod.main()
            except SystemExit as exc:
                code = exc.code
    finally:
        mod.CHILD[0] = saved
        signal.signal(signal.SIGTERM, previous)
    return code, kills


def test_the_stop_path_signals_no_child_popen_already_reaped(monkeypatch: pytest.MonkeyPatch) -> None:
    mod = announce_api()
    reaped = subprocess.Popen([sys.executable, "-c", "pass"])
    reaped.wait(60)
    code, kills = _stop_path(mod, reaped, monkeypatch)
    assert code == 143
    assert kills == [], f"a reaped child's pid was signalled: {kills}"


@posix
def test_the_stop_path_signals_an_unreaped_child_once(monkeypatch: pytest.MonkeyPatch) -> None:
    mod = announce_api()
    live = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(0.3)"])
    try:
        code, kills = _stop_path(mod, live, monkeypatch)
    finally:
        live.wait(30)
    assert code == 143
    assert kills == [(live.pid, signal.SIGTERM)], kills


# ---- host_service over the recorder, every OS -----------------------------------------------------------------


def condor_backend(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    profile: Any = None,
    schedd: RecordingSchedd | None = None,
) -> tuple[Any, Any, RecordingSchedd]:
    """``HTCondorBackend`` over a ``CondorPilots`` started under the recorder, with spies on the announce
    secret's registry and on ``ServiceJob.stop``."""
    schedd = schedd if schedd is not None else RecordingSchedd()
    record_bindings(monkeypatch, schedd)
    pilots = launch_api().CondorPilots(
        profile if profile is not None else generic(), log_dir=tmp_path / "logs"
    )
    backend = run_bounded(
        lambda: backend_api().HTCondorBackend(pilots, 1, host="127.0.0.1", port_range=(0, 0)), BOUND_S
    )
    return backend, pilots, schedd


def close_backend(backend: Any, schedd: RecordingSchedd) -> None:
    schedd.queue = [[]]  # nothing left alive: close() does not wait out a pilot drain
    run_bounded(backend.close, BOUND_S)


def spy_registry(monkeypatch: pytest.MonkeyPatch) -> list[tuple[Any, ...]]:
    events: list[tuple[Any, ...]] = []
    task_server_cls = server_api().TaskServer
    spy_method(monkeypatch, task_server_cls, "announce_secret", events)
    spy_method(monkeypatch, task_server_cls, "forget_announce", events)
    spy_method(monkeypatch, cluster_api().ServiceJob, "stop", events)
    return events


def test_host_service_forgets_its_announce_secret_when_construction_refuses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events = spy_registry(monkeypatch)
    backend, _, schedd = condor_backend(monkeypatch, tmp_path)
    try:
        spec = web_spec(inputs=(str(tmp_path / "absent-model"),))
        with pytest.raises((ValueError, OSError), match="absent-model"):
            run_bounded(lambda: backend.host_service(spec, "scope1"), BOUND_S)
        minted = keys_of(events, "announce_secret")
        assert len(minted) == 1 and minted[0].startswith("scope1-"), events
        assert keys_of(events, "forget_announce") == minted
        services = [d for d in submits(schedd.log) if str(d["JobBatchName"]).startswith("graphed-service-")]
        assert services == [], "a service job was submitted for a refused input"
    finally:
        close_backend(backend, schedd)


@pytest.mark.parametrize("fault", ["spool", "submit"])
def test_a_failed_submit_or_spool_leaves_no_cluster_and_no_registered_secret(
    fault: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events = spy_registry(monkeypatch)
    schedd = RecordingSchedd(queue=[[{"JobStatus": 5}]])
    backend, _, _ = condor_backend(monkeypatch, tmp_path, generic(spool=True), schedd)
    mark = len(schedd.log)
    try:
        if fault == "spool":
            schedd.spool_raises = RuntimeError("spool: connection reset (injected)")
        else:

            def refuse(*args: Any, **kwargs: Any) -> Any:
                schedd.log.append(("submit-refused",))
                raise RuntimeError("submit: schedd refused (injected)")

            monkeypatch.setattr(schedd, "submit", refuse)
        with pytest.raises(RuntimeError, match=f"{fault}: .*injected"):
            run_bounded(lambda: backend.host_service(web_spec(), "scope1"), BOUND_S)
        tail = schedd.log[mark:]
        minted = keys_of(events, "announce_secret")
        assert len(minted) == 1 and keys_of(events, "forget_announce") == minted, events
        acts = [e for e in tail if e[0] == "act"]
        if fault == "spool":
            assert any(e[0] == "submit" for e in tail), tail
            assert any("Remove" in e[1] and re.search(r"ClusterId\s*==\s*4242\b", e[2]) for e in acts), tail
        else:
            assert ("submit-refused",) in tail and acts == [], tail
    finally:
        close_backend(backend, schedd)


@pytest.mark.parametrize(("check", "scheme"), [("http:/", "http"), ("tcp", "tcp"), ("grpc:", "grpc")])
def test_host_service_returns_the_announced_endpoint_and_releases_in_order(
    check: str, scheme: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events = spy_registry(monkeypatch)
    monkeypatch.setattr(server_api(), "POLL_S", 0.5)
    schedd = RecordingSchedd(queue=[[{"JobStatus": 2}]])
    backend, pilots, _ = condor_backend(monkeypatch, tmp_path, schedd=schedd)
    log_dir = Path(pilots.log_dir)
    try:
        pending = in_background(lambda: backend.host_service(web_spec(check=check), "scope1"))

        def job_dir() -> Path | None:
            for d in log_dir.glob("service-scope1-*"):
                if (d / "service.json").is_file() and (d / "graphed-secret").is_file():
                    return d
            return None

        wait_for(lambda: pending.ready() or job_dir() is not None, 60.0)
        if pending.ready():
            pending.get()  # host_service returned or raised before any announce: re-raise it here
            raise AssertionError("host_service returned before its service announced")
        found = job_dir()
        assert found is not None, "host_service wrote no service-<key>/ with service.json and graphed-secret"
        cfg = json.loads((found / "service.json").read_text())
        key = cfg["key"]
        assert re.fullmatch(r"scope1-[0-9a-f]{16}", key), key
        assert found.name == f"service-{key}"
        assert keys_of(events, "announce_secret") == [key]
        announce_hex = (found / "graphed-secret").read_text().strip()
        pilot_hex = (log_dir / "graphed-secret").read_text().strip()
        assert announce_hex != pilot_hex, "the service job carries the pilots' secret"
        (service_desc,) = submits(schedd.log)
        leaked = [k for k, v in service_desc.items() if announce_hex in str(v) or pilot_hex in str(v)]
        assert leaked == [], leaked
        body = announce_body(key, "wn9.example:10042", "wn9.example")
        assert post_announce(cfg["url"], body, bytes.fromhex(announce_hex)) == 200
        got = get_within(pending, 30.0, "host_service after its announce")
        assert got == (f"{scheme}://wn9.example:10042", "wn9.example", key)
        mark = len(schedd.log)
        run_bounded(lambda: backend.release_service(key), BOUND_S)
        order = [e[0] for e in events if e[0] in ("forget_announce", "stop")]
        assert order == ["forget_announce", "stop"], events
        assert keys_of(events, "forget_announce") == [key]
        assert any(e[0] == "act" and "Remove" in e[1] for e in schedd.log[mark:]), schedd.log[mark:]
        assert not (found / "graphed-secret").exists()
        assert post_announce(cfg["url"], body, bytes.fromhex(announce_hex)) == 403
    finally:
        close_backend(backend, schedd)


FAILURES = {
    "gone": (
        [[]],
        [{"JobStatus": 4, "ExitCode": 3}],
        60.0,
        RuntimeError,
        [r"JobStatus\W*4\b", r"ExitCode\W*3\b"],
    ),
    "held": (
        [[{"JobStatus": 5, "HoldReasonCode": 13, "HoldReason": "Transfer input files failure (injected)"}]],
        [],
        60.0,
        RuntimeError,
        [r"JobStatus\W*5\b", r"HoldReasonCode\W*13\b", r"Transfer input files failure \(injected\)"],
    ),
    "idle": ([[{"JobStatus": 1}], [{"JobStatus": 2}]], [], 1.5, TimeoutError, [r"1\.5", r"JobStatus\W*2\b"]),
    "spooling": (
        [[{"JobStatus": 5, "HoldReasonCode": 16}], [{"JobStatus": 2}]],
        [],
        1.5,
        TimeoutError,
        [r"1\.5", r"JobStatus\W*2\b"],
    ),
}


@pytest.mark.parametrize("case", list(FAILURES))
def test_host_service_fails_a_gone_held_or_late_job_and_forgets_its_key(
    case: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    queue, history, timeout_s, error, named = FAILURES[case]
    events = spy_registry(monkeypatch)
    monkeypatch.setattr(server_api(), "POLL_S", 0.5)
    schedd = RecordingSchedd(queue=queue, history=history)
    running: list[float] = []
    answer = schedd.query

    def query(*args: Any, **kwargs: Any) -> list[Any]:
        ads = answer(*args, **kwargs)
        if not running and any(ad.get("JobStatus") == 2 for ad in ads):
            running.append(time.monotonic())
        return ads

    monkeypatch.setattr(schedd, "query", query)
    backend, _, _ = condor_backend(monkeypatch, tmp_path, schedd=schedd)
    mark = len(schedd.log)
    try:
        with pytest.raises(error) as failed:
            run_bounded(lambda: backend.host_service(web_spec(timeout_s=timeout_s), "scope1"), BOUND_S)
        raised = time.monotonic()
        text = str(failed.value)
        missing = [p for p in named if not re.search(p, text)]
        assert missing == [], (missing, text)
        if error is TimeoutError:
            assert running and raised - running[0] >= timeout_s, (running, raised)
        (key,) = keys_of(events, "announce_secret")
        assert keys_of(events, "forget_announce") == [key]
        if error is RuntimeError:
            assert key in text and f"service-{key}" in text, text
        if queue[0]:
            assert any(e[0] == "act" and "Remove" in e[1] for e in schedd.log[mark:]), schedd.log[mark:]
    finally:
        close_backend(backend, schedd)


# ---- announce.py as the job runs it (POSIX) ------------------------------------------------------------------


@pytest.fixture
def machine_ad(tmp_path: Path) -> Path:
    return write_machine_ad(tmp_path / "machine.ad", "localhost")


@pytest.fixture
def identity(machine_ad: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    monkeypatch.setenv("_CONDOR_MACHINE_AD", str(machine_ad))
    ident: str = services_api().host_identity()
    return ident


def _port_of(hostport: str) -> int:
    return int(hostport.rsplit(":", 1)[1])


@posix
def test_an_attached_service_announces_once_ready_from_a_clean_start(
    tmp_path: Path, machine_ad: Path, identity: str
) -> None:
    report = tmp_path / "child.json"
    with task_server() as server:
        secret = server.announce_secret(["k1"])
        cfg = job_config(
            argv=child_argv("serve", report),
            key="k1",
            url=server.url,
            beat_s=0.5,
            env={"M68B_RECIPE_VAR": "recipe"},
        )
        job = write_job(tmp_path / "job", cfg, secret)
        with AnnounceRun(job, announce_env(machine_ad, M68B_JOB_VAR="job"), (report,)) as run:
            got = wait_announce(server, "k1", 60.0)
            assert got is not None, run.output()
            port = _port_of(got[0])
            assert got == (f"{identity}:{port}", identity)
            assert cfg["ports"][0] <= port <= cfg["ports"][1]
            assert http_status(f"http://127.0.0.1:{port}/") == 200, "announced before the service answered"
            state = run.report(report)
            assert state["secret_beside"] is False, "the child started while ../graphed-secret existed"
            assert not (job / "graphed-secret").exists()
            assert state["cwd"] == "service"
            assert state["sigterm_blocked"] is False, "the child inherited a blocked SIGTERM"
            assert state["env"] == {"M68B_JOB_VAR": "job", "M68B_RECIPE_VAR": "recipe"}
            assert wait_announce(server, "k1", 10.0) == got, "the beats stopped taking 200"


@posix
def test_the_child_s_cwd_holds_exactly_the_inputs(tmp_path: Path, machine_ad: Path) -> None:
    report = tmp_path / "child.json"
    store = tmp_path / "store"
    store.mkdir()
    (store / "m.txt").write_text("model")
    with task_server() as server:
        secret = server.announce_secret(["k1"])
        job = write_job(
            tmp_path / "job", job_config(argv=child_argv("serve", report), key="k1", url=server.url), secret
        )
        (job / "service" / "models").mkdir(parents=True)
        os.symlink(store / "m.txt", job / "service" / "models" / "m.txt")
        (job / "user.cc").write_text("a stand-in ticket cache")
        with AnnounceRun(job, announce_env(machine_ad), (report,)) as run:
            got = wait_announce(server, "k1", 60.0)
            assert got is not None, run.output()
            base = f"http://127.0.0.1:{_port_of(got[0])}"
            assert run.report(report)["entries"] == ["models"]
            assert http_status(base + "/user.cc") == 404
            assert http_status(base + "/service.json") == 404
            assert http_get(base + "/models/m.txt") == "model"


@posix
def test_a_relative_python_is_resolved_in_the_job_dir(tmp_path: Path, machine_ad: Path) -> None:
    report, marker = tmp_path / "child.json", tmp_path / "wrapper.marker"
    with task_server() as server:
        secret = server.announce_secret(["k1"])
        cfg = job_config(
            argv=child_argv("serve", report), key="k1", url=server.url, python="./env/bin/python"
        )
        job = write_job(tmp_path / "job", cfg, secret)
        wrapper = job / "env" / "bin" / "python"
        wrapper.parent.mkdir(parents=True)
        wrapper.write_text(f'#!/bin/sh\necho ran > "{marker}"\nexec "{sys.executable}" "$@"\n')
        wrapper.chmod(0o755)
        with AnnounceRun(job, announce_env(machine_ad), (report,)) as run:
            assert wait_announce(server, "k1", 60.0) is not None, run.output()
            assert marker.is_file(), "the child was not started through ./env/bin/python"


@posix
def test_a_relative_argv0_resolves_among_the_inputs(tmp_path: Path, machine_ad: Path) -> None:
    report = tmp_path / "child.json"
    with task_server() as server:
        secret = server.announce_secret(["k1"])
        cfg = job_config(argv=["./serve.sh", "{port}"], key="k1", url=server.url)
        job = write_job(tmp_path / "job", cfg, secret)
        script = job / "service" / "serve.sh"
        script.parent.mkdir()
        script.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{CHILD_SCRIPT}" serve "$1" "{report}"\n')
        script.chmod(0o755)
        with AnnounceRun(job, announce_env(machine_ad), (report,)) as run:
            assert wait_announce(server, "k1", 60.0) is not None, run.output()
            assert run.report(report)["cwd"] == "service"


@posix
def test_a_missing_argv0_exits_3_naming_it_without_a_traceback(tmp_path: Path, machine_ad: Path) -> None:
    cfg = job_config(argv=["./missing", "{port}"], url=f"http://127.0.0.1:{closed_port()}")
    job = write_job(tmp_path / "job", cfg, os.urandom(32))
    with AnnounceRun(job, announce_env(machine_ad)) as run:
        code = run.wait(60.0)
        out = run.output()
    assert code == 3, out
    assert run.ended is not None and run.ended - run.started <= MARGIN_S + 4.0, out
    assert "./missing" in out and "Traceback" not in out, out


@posix
@defpath_python_distinct
def test_a_bare_python_without_path_is_resolved_on_the_default_path(tmp_path: Path, machine_ad: Path) -> None:
    report = tmp_path / "child.json"
    with task_server() as server:
        secret = server.announce_secret(["k1"])
        cfg = job_config(argv=child_argv("serve", report), key="k1", url=server.url, python="python3")
        job = write_job(tmp_path / "job", cfg, secret)
        env = announce_env(machine_ad)
        env.pop("PATH", None)
        with AnnounceRun(job, env, (report,)) as run:
            assert wait_announce(server, "k1", 60.0) is not None, run.output()
            assert run.report(report)["executable"] == defpath_printed


@posix
def test_sigterm_kills_a_child_that_ignores_it_within_the_bound(tmp_path: Path, machine_ad: Path) -> None:
    report = tmp_path / "child.json"
    with task_server() as server:
        secret = server.announce_secret(["k1"])
        job = write_job(
            tmp_path / "job", job_config(argv=child_argv("ignore", report), key="k1", url=server.url), secret
        )
        with AnnounceRun(job, announce_env(machine_ad), (report,)) as run:
            assert wait_announce(server, "k1", 60.0) is not None, run.output()
            pid = run.report(report)["pid"]
            sent = time.monotonic()
            run.proc.send_signal(signal.SIGTERM)
            code = run.wait(60.0)
            assert run.ended is not None and run.ended - sent <= REAP_S + MARGIN_S, run.output()
            assert code == 143, run.output()
            assert pid_gone(pid), "the SIGTERM-ignoring child outlived announce.py"


@posix
def test_the_orphan_reap_kills_a_child_that_ignores_sigterm(tmp_path: Path, machine_ad: Path) -> None:
    report = tmp_path / "child.json"
    cfg = job_config(argv=child_argv("ignore", report), url=f"http://127.0.0.1:{closed_port()}", lease_s=2.0)
    job = write_job(tmp_path / "job", cfg, os.urandom(32))
    with AnnounceRun(job, announce_env(machine_ad), (report,)) as run:
        pid = run.report(report)["pid"]
        ready = time.monotonic()
        code = run.wait(60.0)
        assert run.ended is not None and run.ended - ready <= 2.0 + REAP_S + MARGIN_S, run.output()
    assert code == 0, run.output()
    assert pid_gone(pid)


@posix
def test_sigterm_during_the_orphan_reap_ends_within_the_bound(tmp_path: Path, machine_ad: Path) -> None:
    report = tmp_path / "child.json"
    cfg = job_config(argv=child_argv("ignore", report), url=f"http://127.0.0.1:{closed_port()}", lease_s=2.0)
    job = write_job(tmp_path / "job", cfg, os.urandom(32))
    with AnnounceRun(job, announce_env(machine_ad), (report,)) as run:
        pid = run.report(report)["pid"]
        assert run.line_time("orphaned", 60.0) is not None, run.output()
        time.sleep(1.0)
        sent = time.monotonic()
        run.proc.send_signal(signal.SIGTERM)
        code = run.wait(60.0)
        assert run.ended is not None and run.ended - sent <= REAP_S + MARGIN_S, run.output()
    assert code == 143, run.output()
    assert pid_gone(pid)


@posix
def test_a_held_first_port_announces_the_next(tmp_path: Path, machine_ad: Path, identity: str) -> None:
    low, high = free_range(3)
    with task_server() as server, wildcard_listener(low):
        secret = server.announce_secret(["k1"])
        cfg = job_config(key="k1", url=server.url, ports=[low, high])
        job = write_job(tmp_path / "job", cfg, secret)
        with AnnounceRun(job, announce_env(machine_ad)) as run:
            got = wait_announce(server, "k1", 60.0)
            assert got == (f"{identity}:{low + 1}", identity), (got, run.output())


@posix
def test_a_tcp_requirement_announces_through_a_connect(tmp_path: Path, machine_ad: Path) -> None:
    report = tmp_path / "child.json"
    with task_server() as server:
        secret = server.announce_secret(["k1"])
        cfg = job_config(argv=child_argv("grpcish", report), key="k1", url=server.url, check="tcp")
        job = write_job(tmp_path / "job", cfg, secret)
        with AnnounceRun(job, announce_env(machine_ad), (report,)) as run:
            assert wait_announce(server, "k1", 60.0) is not None, run.output()


@posix
def test_a_grpc_gateway_never_passes_http_and_the_budget_is_the_whole_start(
    tmp_path: Path, machine_ad: Path
) -> None:
    report = tmp_path / "child.json"
    with task_server() as server:
        secret = server.announce_secret(["k1"])
        cfg = job_config(
            argv=child_argv("grpcish", report),
            key="k1",
            url=server.url,
            ports=list(free_range(4)),
            timeout_s=3.0,
        )
        job = write_job(tmp_path / "job", cfg, secret)
        with AnnounceRun(job, announce_env(machine_ad), (report,)) as run:
            code = run.wait(60.0)
            assert code == 3, run.output()
            assert run.ended is not None and run.ended - run.started <= 3.0 + MARGIN_S, run.output()
            assert wait_announce(server, "k1", 0.3) is None
            assert child_starts(report) == 1, "the child was restarted per port"


@posix
def test_a_child_that_exits_at_once_is_not_restarted_per_port(tmp_path: Path, machine_ad: Path) -> None:
    report = tmp_path / "child.json"
    cfg = job_config(
        argv=child_argv("exit", report, "97"),
        ports=list(free_range(10)),
        url=f"http://127.0.0.1:{closed_port()}",
    )
    job = write_job(tmp_path / "job", cfg, os.urandom(32))
    with AnnounceRun(job, announce_env(machine_ad), (report,)) as run:
        code = run.wait(60.0)
        out = run.output()
    assert code == 3, out
    assert run.ended is not None and run.ended - run.started <= MARGIN_S + 4.0, out
    assert names_code(out, 97), out
    assert child_starts(report) == 1


@posix
def test_a_child_that_fails_its_check_then_exits_is_started_once(tmp_path: Path, machine_ad: Path) -> None:
    for i in range(10):
        report = tmp_path / f"child-{i}.json"
        cfg = job_config(
            argv=child_argv("once", report, "98"),
            ports=list(free_range(5)),
            url=f"http://127.0.0.1:{closed_port()}",
            timeout_s=60.0,
        )
        job = write_job(tmp_path / f"job-{i}", cfg, os.urandom(32))
        with AnnounceRun(job, announce_env(machine_ad), (report,)) as run:
            code = run.wait(60.0)
            out = run.output()
        assert (code, names_code(out, 98), child_starts(report)) == (3, True, 1), (i, out)


@posix
@pytest.mark.parametrize("case", ["no-200-ever", "another-secret", "server-gone"])
def test_an_orphaned_service_is_reaped_and_exits_0(case: str, tmp_path: Path, machine_ad: Path) -> None:
    report = tmp_path / "child.json"
    with task_server() as server:
        registered = server.announce_secret(["k1"])
        url = f"http://127.0.0.1:{closed_port()}" if case == "no-200-ever" else server.url
        lease_s = 60.0 if case == "another-secret" else 2.0
        secret = os.urandom(32) if case != "server-gone" else registered
        cfg = job_config(argv=child_argv("serve", report), key="k1", url=url, lease_s=lease_s, beat_s=0.5)
        job = write_job(tmp_path / "job", cfg, secret)
        with AnnounceRun(job, announce_env(machine_ad), (report,)) as run:
            pid = run.report(report)["pid"]
            since = time.monotonic()
            if case == "server-gone":
                assert wait_announce(server, "k1", 60.0) is not None, run.output()
                server.shutdown()
                since = time.monotonic()
            code = run.wait(60.0)
            bound = MARGIN_S + 2.0 if case == "another-secret" else lease_s + MARGIN_S + 2.0
            assert run.ended is not None and run.ended - since <= bound, run.output()
            if case == "another-secret":
                assert wait_announce(server, "k1", 0.3) is None
        assert code == 0, run.output()
        assert pid_gone(pid), "the orphaned service's child outlived announce.py"


def _replace(path: Path, text: str) -> None:
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text)
    os.replace(tmp, path)


@posix
def test_watch_mode_announces_each_new_url_and_secret(tmp_path: Path, machine_ad: Path) -> None:
    report = tmp_path / "child.json"
    watch = tmp_path / "dag"
    watch.mkdir()
    with task_server() as first, task_server() as second:
        _replace(watch / "graphed-secret", first.announce_secret(["svc0"]).hex())
        _replace(watch / "driver.url", first.url)
        cfg = job_config(argv=child_argv("serve", report), key="svc0", url=None, watch=str(watch))
        job = write_job(tmp_path / "job", cfg, None)
        with AnnounceRun(job, announce_env(machine_ad), (report,)) as run:
            assert wait_announce(first, "svc0", 60.0) is not None, run.output()
            _replace(watch / "graphed-secret", second.announce_secret(["svc0"]).hex())
            _replace(watch / "driver.url", second.url)
            assert wait_announce(second, "svc0", 30.0) is not None, run.output()
            second.forget_announce(["svc0"])
            _replace(watch / "graphed-secret", second.announce_secret(["svc0"]).hex())
            assert wait_announce(second, "svc0", 30.0) is not None, (
                "a new secret on the same url was not announced"
            )
            pid = run.report(report)["pid"]
            sent = time.monotonic()
            run.proc.send_signal(signal.SIGTERM)
            code = run.wait(60.0)
            assert run.ended is not None and run.ended - sent <= REAP_S + MARGIN_S, run.output()
            assert code == 143, run.output()
            assert wait_for(lambda: pid_gone(pid), 5.0), "SIGTERM left the watch-mode child running"
