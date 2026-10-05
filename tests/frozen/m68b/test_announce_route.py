"""m68b B1: the task server's ``/announce`` route (plan-services.md §3.3, B1 ``TaskServer`` bullet).

A ``ServiceJob`` never carries the pilots' secret: the task server mints a per-call announce secret
(``announce_secret(keys)``) valid only on ``/announce`` and only for those keys. The route takes UTF-8 text
``key host:port identity``, verifies its signature against the key's announce secret before anything else
(no key, no secret: 403), then wants exactly three fields with an integer port (400). ``wait_announce``
pops what an announce recorded. The pickled routes verify only the pilots' secret, so an announce secret
signs no pickle. Every OS; a real ``TaskServer`` on loopback; nothing recorded is witnessed by
``wait_announce`` returning ``None``, nothing unpickled by the m66 marker.
"""

from __future__ import annotations

import os
import pickle
import time
from concurrent.futures import Future
from pathlib import Path

import pytest
from m68b_harness import (
    MarkerBomb,
    announce_body,
    get_within,
    in_background,
    post,
    post_announce,
    sign,
    task_server,
    wait_announce,
)

NOTHING_S = 0.3  # how long "nothing recorded" waits


def test_a_signed_announce_is_recorded_once_and_wakes_the_waiter() -> None:
    with task_server() as server:
        secret = server.announce_secret(["k1"])
        assert isinstance(secret, bytes) and len(secret) == 32 and secret != server.secret
        waiter = in_background(lambda: server.wait_announce("k1", 30.0))
        time.sleep(0.3)
        started = time.monotonic()
        assert (
            post_announce(server.url, announce_body("k1", "wn3.example:10005", "wn3.example"), secret) == 200
        )
        assert get_within(waiter, 10.0, "wait_announce") == ("wn3.example:10005", "wn3.example")
        assert time.monotonic() - started < 5.0, "wait_announce was not woken by the announce"
        before = time.monotonic()
        assert wait_announce(server, "k1", 0.5) is None, "wait_announce did not pop the record it returned"
        assert time.monotonic() - before >= 0.45


def test_an_unsigned_or_pilot_signed_announce_is_refused_and_records_nothing() -> None:
    with task_server() as server:
        server.announce_secret(["k1"])
        body = announce_body("k1", "wn3.example:10005", "wn3.example")
        assert post_announce(server.url, body, None) == 403
        assert post_announce(server.url, body, server.secret) == 403
        assert wait_announce(server, "k1", NOTHING_S) is None


def test_another_key_s_announce_secret_does_not_sign_this_key() -> None:
    with task_server() as server:
        server.announce_secret(["k1"])
        other = server.announce_secret(["k2"])
        assert (
            post_announce(server.url, announce_body("k1", "wn3.example:10005", "wn3.example"), other) == 403
        )
        assert wait_announce(server, "k1", NOTHING_S) is None
        assert wait_announce(server, "k2", NOTHING_S) is None


@pytest.mark.parametrize(
    "fields",
    ["k1 wn3.example:10005", "k1 wn3.example:10005 wn3.example extra", "k1 wn3.example:http wn3.example"],
    ids=["two", "four", "non-integer-port"],
)
def test_a_signed_body_that_is_not_three_fields_with_an_integer_port_is_a_bad_request(fields: str) -> None:
    with task_server() as server:
        secret = server.announce_secret(["k1"])
        assert post_announce(server.url, fields.encode(), secret) == 400
        assert wait_announce(server, "k1", NOTHING_S) is None


@pytest.mark.parametrize("body", [b"k1 wn3.example:10005 wn\xff\xfe3", b""], ids=["not-utf8", "empty"])
def test_a_body_without_a_readable_key_is_forbidden(body: bytes) -> None:
    with task_server() as server:
        secret = server.announce_secret(["k1"])
        assert post_announce(server.url, body, secret) == 403
        assert wait_announce(server, "k1", NOTHING_S) is None


def test_a_forgotten_key_is_refused() -> None:
    with task_server() as server:
        secret = server.announce_secret(["k1"])
        body = announce_body("k1", "wn3.example:10005", "wn3.example")
        assert post_announce(server.url, body, secret) == 200
        assert wait_announce(server, "k1", 5.0) == ("wn3.example:10005", "wn3.example")
        server.forget_announce(["k1"])
        assert post_announce(server.url, body, secret) == 403
        assert wait_announce(server, "k1", NOTHING_S) is None


def test_an_announce_secret_signs_no_pickle_and_the_pilot_secret_still_settles(tmp_path: Path) -> None:
    with task_server() as server:
        secret = server.announce_secret(["k1"])
        marker = tmp_path / "unpickled.marker"
        bomb = pickle.dumps(MarkerBomb(str(marker)))
        assert post(server.url + "/result", bomb, sign(secret, bomb)) == 403
        assert post_announce(server.url, bomb, server.secret) == 403
        assert not marker.exists(), "a pickle was loaded on an announce secret or on /announce"
        fut: Future[object] = Future()
        server.add("t1", os.getpid, (), fut)
        leased = server.lease("pilot-1")
        assert leased is not None
        tid = leased[0]
        result = pickle.dumps(("pilot-1", tid, True, pickle.dumps(42)))
        assert post(server.url + "/result", result, sign(secret, result)) == 403
        assert not fut.done()
        assert post(server.url + "/result", result, sign(server.secret, result)) == 200
        assert fut.result(10.0) == 42


def test_a_waiting_announce_takes_no_wake_up_meant_for_a_leasing_pilot() -> None:
    with task_server() as server:
        secret = server.announce_secret(["k1"])
        announced = in_background(lambda: server.wait_announce("k1", 30.0))
        time.sleep(0.3)
        leased = in_background(lambda: server.lease("pilot-1"))
        time.sleep(0.3)
        server.add("t1", os.getpid, (), Future())
        got = get_within(leased, 5.0, "a pilot's lease while an announce waits")
        assert got is not None, "the pilot's lease returned nothing although a task was queued"
        assert (
            post_announce(server.url, announce_body("k1", "wn3.example:10005", "wn3.example"), secret) == 200
        )
        assert get_within(announced, 10.0, "wait_announce") == ("wn3.example:10005", "wn3.example")
