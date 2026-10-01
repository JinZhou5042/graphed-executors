"""m66 launcher paths a single personal pool cannot reach: collector failover and the default log_dir."""

from __future__ import annotations

import getpass
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from graphed_executors.htcondor_backend import CondorPilots, SiteProfile, launch

AD = {
    "Name": "s1",
    "RecentDaemonCoreDutyCycle": 0.1,
    "ShadowsRunning": 1,
    "MaxJobsRunning": 10,
    "TotalIdleJobs": 0,
}


class FakeCollector:
    asked: list[str] = []  # noqa: RUF012  (reset per test)

    def __init__(self, node: str) -> None:
        self.node = node
        FakeCollector.asked.append(node)

    def query(self, ad_type: str, constraint: str, projection: list[str]) -> list[dict[str, Any]]:
        if self.node == "down":
            raise OSError("collector down")
        return [AD] if self.node == "up" else []

    def locate(self, daemon: str, name: str) -> dict[str, str]:
        return {"Name": name, "via": self.node}


def fake_htcondor(pool: str) -> Any:
    kinds = SimpleNamespace(Schedd="schedd")
    return SimpleNamespace(
        param={"POOL": pool}, Collector=FakeCollector, AdType=kinds, DaemonType=kinds, Schedd=lambda ad: ad
    )


def queried_site() -> SiteProfile:
    return SiteProfile(
        name="q", submit={}, spool=False, ship_env=False, sandbox_root=None, schedd_query=("POOL", "true")
    )


def test_a_failing_collector_hands_over_to_the_next_node() -> None:
    FakeCollector.asked = []
    name, schedd = CondorPilots(queried_site())._choose(fake_htcondor("down, up"))
    assert (name, schedd["via"]) == ("s1", "up")
    assert FakeCollector.asked == ["down", "up"]


def test_no_answering_collector_is_an_error_naming_each_node() -> None:
    with pytest.raises(RuntimeError, match="down: collector down; empty: no schedd matches true"):
        CondorPilots(queried_site())._choose(fake_htcondor("down empty"))


class BindingsReached(Exception):
    pass


def stop_at_bindings() -> Any:
    raise BindingsReached


def test_the_default_log_dir_is_fresh_under_the_sandbox_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(launch, "_htcondor", stop_at_bindings)
    user_root = tmp_path / getpass.getuser()
    user_root.mkdir()
    site = SiteProfile(
        name="sb",
        submit={},
        spool=False,
        ship_env=False,
        sandbox_root=str(tmp_path / "{user}"),
        schedd_query=None,
    )
    pilots = [CondorPilots(site) for _ in range(2)]
    for p in pilots:
        with pytest.raises(BindingsReached):
            p.start("http://127.0.0.1:1", b"secret", 1)
    dirs = [p.log_dir for p in pilots]
    assert dirs[0] != dirs[1]
    for d in dirs:
        assert d is not None and d.parent == user_root
        assert (d / "graphed-secret").read_text() == b"secret".hex()


class CredBindings:
    """Bindings and schedd in one: the credd answers ``stored``, ``issue_credentials`` raises ``error``
    when given, and each call lands in ``log`` in order."""

    CredType = SimpleNamespace(Kerberos="krb")

    def __init__(self, stored: str | None, error: Exception | None = None) -> None:
        self.stored = stored
        self.error = error
        self.log: list[str] = []

    def Credd(self) -> Any:
        return SimpleNamespace(query_user_cred=self._query)

    def _query(self, kind: str) -> str | None:
        self.log.append(f"query {kind}")
        return self.stored

    def Submit(self, desc: dict[str, str]) -> Any:
        return SimpleNamespace(issue_credentials=self._issue)

    def _issue(self) -> None:
        self.log.append("issue")
        if self.error is not None:
            raise self.error

    def submit(self, desc: Any, count: int = 0, spool: bool = False) -> Any:
        self.log.append("submit")
        return SimpleNamespace(cluster=lambda: 7)

    def spool(self, result: Any) -> None:
        self.log.append("spool")


LXPLUS = {"MY.SendCredential": "True"}


@pytest.mark.parametrize(
    ("stored", "want"),
    [(None, ["query krb", "issue", "submit", "spool"]), ("1790802437", ["query krb", "submit", "spool"])],
)
def test_a_credential_is_stored_before_the_submit_only_when_the_credd_holds_none(
    stored: str | None, want: list[str]
) -> None:
    pilots = CondorPilots("lxplus", image="img")
    desc = pilots.submit_description("http://h:1", 1)
    assert desc["MY.SendCredential"] == "True", "control: the lxplus row sends a credential"
    fake = CredBindings(stored)
    with ExitStack() as stack:
        pilots._submit(fake, fake, desc, 1, stack)
        stack.pop_all()
    assert fake.log == want


def test_a_failed_store_names_the_host_command_and_submits_nothing() -> None:
    fake = CredBindings(None, OSError("Failed to launch /usr/bin/batch_krb5_credential"))
    with pytest.raises(RuntimeError, match=r"batch_krb5_credential.*condor_store_cred add-krb -i -"):
        CondorPilots("lxplus", image="img")._submit(fake, fake, LXPLUS, 1, ExitStack())
    assert fake.log == ["query krb", "issue"]


@pytest.mark.parametrize("case", ["no-credential", "in-job"])
def test_the_credd_is_not_asked_without_a_credential_or_inside_a_job(case: str) -> None:
    pilots = (
        CondorPilots("generic")
        if case == "no-credential"
        else CondorPilots("lxplus", image="img", schedd_locate=("pool", "s1"))
    )
    fake = CredBindings(None)
    with ExitStack() as stack:
        pilots._submit(fake, fake, pilots.submit_description("http://h:1", 1), 1, stack)
        stack.pop_all()
    assert "query krb" not in fake.log and "submit" in fake.log, fake.log
