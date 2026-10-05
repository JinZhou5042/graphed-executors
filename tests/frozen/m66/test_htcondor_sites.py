"""m66 site profiles, schedd choice and launcher refusals, on data only: no htcondor bindings, every OS.

The schedd ads are the recorded LPC collector answer (``data/lpc-schedd-ads.json``). Each per-term set
is two ads derived from those records: the deciding term picks the full-formula winner and the other
two terms together rank the pair the other way, so a weight missing that term picks the other ad.

Discriminates a weight formula missing any term, the proxy-default trap (``use_x509userproxy`` without
an explicit ``x509userproxy`` path), site keys leaking into the generic profile, a missing secret or env
transfer, and launcher refusals that are late (after the bindings are touched) or absent."""

from __future__ import annotations

import getpass
import json
import os
import stat
import sys
import sysconfig
import tarfile
from pathlib import Path
from typing import Any

import pytest
from htcondor_harness import DATA_DIR, htcondor_api, launch_api, sites_api

LPC_QUERY = (
    "FERMIHTC_REMOTE_POOL",
    'FERMIHTC_DRAIN_LPCSCHEDD=?=FALSE && FERMIHTC_SCHEDD_TYPE=?="CMSLPC" && MaxJobsRunning!=0',
)
IMAGE = "/cvmfs/unpacked.cern.ch/registry.hub.docker.com/coffeateam/coffea-almalinux9-noml:2026.9.0-py3.12"
URL = "http://127.0.0.1:10000"
SITE_KEYS = (
    "use_x509userproxy",
    "x509userproxy",
    "+DesiredOS",
    "MY.SingularityImage",
    "MY.SendCredential",
    "+JobFlavour",
)


def recorded_ads() -> dict[str, dict[str, Any]]:
    ads = json.loads((DATA_DIR / "lpc-schedd-ads.json").read_text())["ads"]
    return {ad["Name"]: ad for ad in ads}


def terms(ad: dict[str, Any]) -> tuple[float, float, float]:
    """The wrapper formula's three terms (plan §2 ``schedd_weight``)."""
    return (
        0.7 * ad["RecentDaemonCoreDutyCycle"] * 100,
        0.2 * ad["ShadowsRunning"] / ad["MaxJobsRunning"] * 100,
        0.1 * ad["TotalIdleJobs"],
    )


def per_term_sets() -> dict[str, tuple[dict[str, Any], dict[str, Any]]]:
    """term index name -> (winner, loser). The winner leads on the deciding term only."""
    rec = recorded_ads()
    s4, s5, s6 = rec["lpcschedd4.fnal.gov"], rec["lpcschedd5.fnal.gov"], rec["lpcschedd6.fnal.gov"]
    return {
        # schedd4 with schedd6's duty cycle and 20 more idle jobs than schedd5, against schedd5
        "RecentDaemonCoreDutyCycle": (
            {
                **s4,
                "RecentDaemonCoreDutyCycle": s6["RecentDaemonCoreDutyCycle"],
                "TotalIdleJobs": s5["TotalIdleJobs"] + 20,
            },
            s5,
        ),
        # schedd4 against schedd5 carrying schedd4's duty cycle and one idle job fewer
        "ShadowsRunning/MaxJobsRunning": (
            s4,
            {
                **s5,
                "RecentDaemonCoreDutyCycle": s4["RecentDaemonCoreDutyCycle"],
                "TotalIdleJobs": s4["TotalIdleJobs"] - 1,
            },
        ),
        # the recorded pair: schedd6 is lighter on duty cycle and shadows, heavier on idle jobs
        "TotalIdleJobs": (s4, s6),
    }


def test_schedd_weight_is_the_wrapper_formula_on_the_recorded_ads() -> None:
    sites = sites_api()
    for name, ad in recorded_ads().items():
        assert sites.schedd_weight(ad) == pytest.approx(sum(terms(ad)), rel=1e-12), name


def test_choose_schedd_picks_the_recorded_winner_in_both_orders() -> None:
    sites = sites_api()
    ads = list(recorded_ads().values())
    assert sites.choose_schedd(ads) == "lpcschedd4.fnal.gov"
    assert sites.choose_schedd(list(reversed(ads))) == "lpcschedd4.fnal.gov"


@pytest.mark.parametrize(
    "term", ["RecentDaemonCoreDutyCycle", "ShadowsRunning/MaxJobsRunning", "TotalIdleJobs"]
)
def test_each_weight_term_decides_its_derived_set(term: str) -> None:
    index = ["RecentDaemonCoreDutyCycle", "ShadowsRunning/MaxJobsRunning", "TotalIdleJobs"].index(term)
    winner, loser = per_term_sets()[term]
    tw, tl = terms(winner), terms(loser)
    # the set is well-formed: the term favours the winner, the other two favour the loser
    assert tw[index] < tl[index]
    assert sum(tw) - tw[index] > sum(tl) - tl[index]
    assert sum(tw) < sum(tl)
    sites = sites_api()
    assert sites.choose_schedd([winner, loser]) == winner["Name"]
    assert sites.choose_schedd([loser, winner]) == winner["Name"]


def test_counts_as_alive_counts_the_spooling_hold_only() -> None:
    alive = sites_api().counts_as_alive
    assert alive({"JobStatus": 1}) is True
    assert alive({"JobStatus": 2}) is True
    assert alive({"JobStatus": 5, "HoldReasonCode": 16}) is True
    assert alive({"JobStatus": 5, "HoldReasonCode": 13}) is False
    assert alive({"JobStatus": 4}) is False


def test_site_profiles() -> None:
    api = htcondor_api()
    lpc, lxplus, generic = api.SITES["lpc"], api.SITES["lxplus"], api.SITES["generic"]
    assert isinstance(lpc, api.SiteProfile)
    assert lpc.schedd_query == LPC_QUERY
    assert lpc.sandbox_root == "/uscmst1b_scratch/lpc1/3DayLifetime/{user}"
    assert (lpc.spool, lpc.ship_env) == (True, True)
    assert (lxplus.spool, lxplus.ship_env, lxplus.sandbox_root, lxplus.schedd_query) == (
        True,
        True,
        None,
        None,
    )
    assert (generic.spool, generic.ship_env) == (False, False)
    assert (generic.sandbox_root, generic.schedd_query) == (None, None)
    assert dict(generic.submit) == {}


def describe(site: str, tmp_path: Path, **kwargs: Any) -> dict[str, str]:
    pilots = htcondor_api().CondorPilots(site, log_dir=tmp_path, **kwargs)
    return dict(pilots.submit_description(URL, 3))


def test_base_submit_keys(tmp_path: Path) -> None:
    for site, kwargs in (("generic", {}), ("lpc", {"image": IMAGE}), ("lxplus", {"image": IMAGE})):
        desc = describe(site, tmp_path, **kwargs)
        assert desc["universe"] == "vanilla", site
        assert desc["executable"] == "pilot.sh", site
        assert URL in desc["arguments"] and "graphed-secret" in desc["arguments"], site
        assert desc["output"] == "pilot.$(ProcId).out", site
        assert desc["error"] == "pilot.$(ProcId).err", site
        assert desc["log"] == "pilots.log", site
        assert desc["should_transfer_files"] == "YES", site
        assert desc["when_to_transfer_output"] == "ON_EXIT_OR_EVICT", site
        assert desc["transfer_output_files"] == '""', site
        assert desc["request_cpus"] == "1", site
        assert desc["request_memory"] == "2048", site
        assert desc["JobBatchName"].startswith("graphed-pilots-"), site


def test_lpc_description_sets_an_absolute_proxy_path_and_the_image(tmp_path: Path) -> None:
    desc = describe("lpc", tmp_path, image=IMAGE)
    proxy = desc["x509userproxy"]
    assert desc["use_x509userproxy"] == "true"
    assert os.path.isabs(proxy) and "~" not in proxy and "$" not in proxy, proxy
    if hasattr(os, "getuid"):
        assert proxy == f"{os.path.expanduser('~')}/x509up_u{os.getuid()}"
    assert desc["MY.SingularityImage"] == f'"{IMAGE}"'
    assert desc["+DesiredOS"] == '"EL9"'


def test_lxplus_description_and_extra_submit_override(tmp_path: Path) -> None:
    desc = describe("lxplus", tmp_path, image=IMAGE)
    assert desc["transfer_output_files"] == '""'
    assert desc["MY.SendCredential"] == "True"
    assert desc["MY.SingularityImage"] == f'"{IMAGE}"'
    assert desc["+JobFlavour"] == '"longlunch"'
    over = describe("lxplus", tmp_path, image=IMAGE, extra_submit={"+JobFlavour": '"workday"'})
    assert over["+JobFlavour"] == '"workday"'


def test_generic_description_has_no_site_keys(tmp_path: Path) -> None:
    desc = describe("generic", tmp_path, request_cpus=4, request_memory_mb=8000)
    assert not set(SITE_KEYS) & set(desc), sorted(set(SITE_KEYS) & set(desc))
    assert (desc["request_cpus"], desc["request_memory"]) == ("4", "8000")


def test_transfer_input_files_carry_the_secret_env_and_user_modules(tmp_path: Path) -> None:
    module = tmp_path / "my_analysis.py"
    module.write_text("X = 1\n")
    for site, kwargs, ships in (
        ("generic", {}, False),
        ("lpc", {"image": IMAGE}, True),
        ("lxplus", {"image": IMAGE}, True),
    ):
        desc = describe(site, tmp_path, user_modules=[module], **kwargs)
        entries = [e.strip() for e in desc["transfer_input_files"].split(",")]
        assert "graphed-secret" in entries, (site, entries)
        assert ("env.tgz" in entries) is ships, (site, entries)
        assert any(e.endswith("my_analysis.py") for e in entries), (site, entries)


class BindingsReached(Exception):
    """Raised by the patched ``_htcondor()``: the launcher got past every pre-bindings step."""


def stop_at_bindings() -> Any:
    raise BindingsReached


def lpc_sandbox_dir() -> str:
    return f"/uscmst1b_scratch/lpc1/3DayLifetime/{getpass.getuser()}/graphed-m66"


@pytest.mark.parametrize("site", ["lpc", "lxplus"])
def test_start_refuses_a_site_image_template_without_an_image(
    site: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(launch_api(), "_htcondor", stop_at_bindings)
    log_dir = lpc_sandbox_dir() if site == "lpc" else tmp_path
    pilots = htcondor_api().CondorPilots(site, log_dir=log_dir)
    with pytest.raises(ValueError, match="image"):
        pilots.start(URL, b"s" * 32, 1)


def test_start_refuses_an_lpc_log_dir_outside_the_sandbox(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(launch_api(), "_htcondor", stop_at_bindings)
    pilots = htcondor_api().CondorPilots("lpc", image=IMAGE, log_dir=tmp_path)
    with pytest.raises(ValueError, match="3DayLifetime"):
        pilots.start(URL, b"s" * 32, 1)


def fake_venv(root: Path, *, editable: bool | None) -> Path:
    """A directory shaped like a venv (``editable=None``: no pyvenv.cfg) with one dist-info."""
    root.mkdir()
    if editable is not None:
        (root / "pyvenv.cfg").write_text("home = /usr/bin\ninclude-system-site-packages = false\n")
    purelib = Path(
        sysconfig.get_path("purelib", scheme="venv", vars={"base": str(root), "platbase": str(root)})
    )
    dist = purelib / "probedist-0.1.dist-info"
    dist.mkdir(parents=True)
    dir_info = {"editable": True} if editable else {}
    (dist / "direct_url.json").write_text(json.dumps({"url": "file:///src/probedist", "dir_info": dir_info}))
    return root


def ship_site() -> Any:
    return htcondor_api().SiteProfile(
        name="m66-ship", submit={}, spool=False, ship_env=True, sandbox_root=None, schedd_query=None
    )


@pytest.mark.parametrize(("editable", "names"), [(True, ("editable", "probedist")), (None, ("venv",))])
def test_start_refuses_an_unshippable_env(
    editable: bool | None, names: tuple[str, ...], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(launch_api(), "_htcondor", stop_at_bindings)
    env = fake_venv(tmp_path / "env", editable=editable)
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    pilots = htcondor_api().CondorPilots(ship_site(), log_dir=log_dir, env=env)
    with pytest.raises(ValueError) as excinfo:
        pilots.start(URL, b"s" * 32, 1)
    for name in names:
        assert name in str(excinfo.value), (name, str(excinfo.value))


def test_start_ships_a_non_editable_env_and_a_private_secret(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(launch_api(), "_htcondor", stop_at_bindings)
    env = fake_venv(tmp_path / "env", editable=False)
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    pilots = htcondor_api().CondorPilots(ship_site(), log_dir=log_dir, env=env)
    with pytest.raises(BindingsReached):
        pilots.start(URL, b"s" * 32, 1)
    with tarfile.open(log_dir / "env.tgz") as tar:
        names = tar.getnames()
    assert any(Path(n).name == "pyvenv.cfg" for n in names), names
    secret = log_dir / "graphed-secret"
    assert secret.is_file()
    if sys.platform != "win32":
        assert stat.S_IMODE(secret.stat().st_mode) == 0o600
