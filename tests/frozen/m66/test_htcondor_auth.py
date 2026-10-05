"""m66 D2: every task-server route refuses an unsigned or wrongly signed request before ``pickle.loads``.

The body is a pickle whose unpickling creates a marker file, so the marker's absence after a 403 shows
no unpickle happened, and the same body correctly signed (``X-Graphed-Sig`` = the hex HMAC-SHA256 of
the body under the run's secret) creates it, which shows the instrument is live on that route.

Discriminates unpickling before the signature check, a check skipped on any one route, and a pilot
with the wrong secret file that still receives work."""

from __future__ import annotations

import hashlib
import hmac
import os
import pickle
import subprocess
import sys
import threading
import urllib.error
import urllib.request
from pathlib import Path

import pytest
from htcondor_harness import (
    MarkerBomb,
    RecordingLauncher,
    htcondor_api,
    local_backend,
    local_pilots,
    make_runner,
    prov_plan,
    run_bounded,
    wait_for,
)

ROUTES = ("/hello", "/next", "/beat", "/result")


def post(url: str, body: bytes, sig: str | None, timeout: float = 30.0) -> int:
    request = urllib.request.Request(url, data=body, method="POST")
    if sig is not None:
        request.add_header("X-Graphed-Sig", sig)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return int(response.status)
    except urllib.error.HTTPError as exc:
        return int(exc.code)


def sign(secret: bytes, body: bytes) -> str:
    return hmac.new(secret, body, hashlib.sha256).hexdigest()


def post_in_background(url: str, body: bytes, sig: str) -> None:
    """A signed control request; its reply may be a long poll, and only the marker is asserted."""

    def _send() -> None:
        try:
            post(url, body, sig)
        except OSError:
            return

    threading.Thread(target=_send, daemon=True).start()


@pytest.mark.parametrize("route", ROUTES)
def test_route_refuses_bad_signatures_before_unpickling(route: str, tmp_path: Path) -> None:
    recorder = RecordingLauncher()
    backend = htcondor_api().HTCondorBackend(recorder, 1, host="127.0.0.1")
    try:
        (url, secret, _n), *_ = recorder.starts
        target = url.rstrip("/") + route
        for label, key in (("unsigned", None), ("wrong", b"not-the-run-secret")):
            marker = tmp_path / f"{label}.marker"
            body = pickle.dumps(MarkerBomb(str(marker)))
            assert post(target, body, None if key is None else sign(key, body)) == 403, (route, label)
            assert not marker.exists(), f"{route} unpickled a {label} body"
        control = tmp_path / "signed.marker"
        body = pickle.dumps(MarkerBomb(str(control)))
        post_in_background(target, body, sign(secret, body))
        wait_for(control.exists)
        assert control.exists(), f"{route} never unpickled a correctly signed body: the instrument is dead"
    finally:
        backend.close()


def test_a_pilot_with_the_wrong_secret_gets_no_task(tmp_path: Path) -> None:
    recorder = RecordingLauncher(local_pilots())
    backend = local_backend(1, recorder)
    try:
        url = recorder.starts[0][0]
        wrong = tmp_path / "graphed-secret"
        wrong.write_text(os.urandom(32).hex())
        rogue = subprocess.Popen(
            [sys.executable, "-m", "graphed_executors.htcondor_backend.pilot", url, str(wrong)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        runner = make_runner(backend)
        value = run_bounded(lambda: runner.run(prov_plan(4, "m66auth"))).value
        out, _ = rogue.communicate(timeout=60)
    finally:
        backend.close()
    assert rogue.returncode == 2, out
    assert "wrong secret file" in out, out
    pids = {pid for _uri, pid, _worker in value.leaves}
    assert len(value.leaves) == 4
    assert len(pids) == 1 and rogue.pid not in pids, (pids, rogue.pid)
