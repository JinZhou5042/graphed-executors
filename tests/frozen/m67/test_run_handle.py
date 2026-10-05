"""m67: ``RunHandle`` tracks and collects a driverless job over recorded bindings (every OS).

``status()`` maps one projected query of the job's ad (``JobStatus``, ``HoldReasonCode``, ``ExitCode``)
to queued/running/held/done/removed/failed, reading the history once the job has left the queue;
``wait`` polls to a terminal state within its bound; ``result()`` refuses before done, retrieves a
spooled sandbox, and re-raises a pickled exception intact; ``save``/``load`` round-trip through JSON.
Every schedd is reached by ``Collector.locate`` on the recorded name, never ``Schedd()``.

The ads are ``data/driver-ads.json``: synthetic, in the shapes of the status lines of the LPC probe
``probes/services-lpc/p1-dag/transcript-p1c-spool-tif.txt``.

Discriminates a spooling-input hold reported as held, a completed job reported done whatever its
exit code, a status that never reads the history, a result read from a stale file before done, a
missing retrieve, a swallowed or stringified exception, an unbounded wait, and ``Schedd()`` (the
submit host's default schedd, not the one that holds the job)."""

from __future__ import annotations

import json
import pickle
from pathlib import Path
from typing import Any

import pytest
from driverless_harness import (
    DATA_DIR,
    FAKE_SCHEDD,
    RecordingSchedd,
    assert_intact_stage_error,
    htcondor_api,
    make_stage_error,
    record_bindings,
    run_bounded,
    schedd_locations,
)
from graphed.core.execution import ExecResult, StopReason

ADS = json.loads((DATA_DIR / "driver-ads.json").read_text())
CLUSTER = int(ADS["cluster"])
QUEUE = {case["case"]: case for case in ADS["queue"]}
HISTORY = {case["case"]: case for case in ADS["history"]}
PROJECTED = {"JobStatus", "HoldReasonCode", "ExitCode"}
SUBMITTED_AT = 1790339439.0  # JobCurrentStartDate of the probe's DAGMan job


def make_handle(log_dir: Path, site: str = "generic") -> Any:
    return htcondor_api().RunHandle(
        site=site, schedd=FAKE_SCHEDD, cluster=CLUSTER, log_dir=log_dir, submitted_at=SUBMITTED_AT
    )


def queue_ad(case: str) -> dict[str, Any]:
    return dict(QUEUE[case]["ad"])


def assert_located_by_name(log: list[tuple[Any, ...]]) -> None:
    locations = schedd_locations(log)
    assert locations, f"no schedd was reached: {log}"
    assert None not in locations, f"Schedd() without a location: {log}"
    assert set(locations) == {FAKE_SCHEDD}, locations
    located = [entry for entry in log if entry[0] == "locate"]
    assert located and all(entry[2:] == ("Schedd", FAKE_SCHEDD) for entry in located), located


@pytest.mark.parametrize("case", sorted(QUEUE))
def test_status_of_a_queued_ad(case: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    schedd = RecordingSchedd(queue=[[queue_ad(case)]])
    record_bindings(monkeypatch, schedd)
    assert make_handle(tmp_path).status() == QUEUE[case]["status"]
    queries = [entry for entry in schedd.log if entry[0] in ("query", "history")]
    assert len(queries) == 1 and queries[0][0] == "query", queries
    _, constraint, projection = queries[0]
    assert str(CLUSTER) in constraint, constraint
    assert set(projection) >= PROJECTED, projection
    assert_located_by_name(schedd.log)


@pytest.mark.parametrize("case", sorted(HISTORY))
def test_status_after_the_job_left_the_queue(
    case: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    schedd = RecordingSchedd(queue=[[]], history=[dict(HISTORY[case]["ad"])])
    record_bindings(monkeypatch, schedd)
    assert make_handle(tmp_path).status() == HISTORY[case]["status"]
    (history,) = [entry for entry in schedd.log if entry[0] == "history"]
    assert str(CLUSTER) in history[1], history
    assert_located_by_name(schedd.log)


@pytest.mark.parametrize("final", ["completed-exit-0", "completed-exit-3"])
def test_wait_polls_to_a_terminal_state(final: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    schedd = RecordingSchedd(queue=[[queue_ad("idle")], [queue_ad("running")], [queue_ad(final)]])
    record_bindings(monkeypatch, schedd)
    handle = make_handle(tmp_path)
    run_bounded(lambda: handle.wait(timeout=60, poll_s=0.01), 90)
    assert len([entry for entry in schedd.log if entry[0] == "query"]) >= 3, schedd.log
    assert handle.status() == QUEUE[final]["status"]


def test_wait_is_bounded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    record_bindings(monkeypatch, RecordingSchedd(queue=[[queue_ad("running")]]))
    handle = make_handle(tmp_path)
    with pytest.raises(TimeoutError):
        run_bounded(lambda: handle.wait(timeout=0.3, poll_s=0.02), 60)


def done_result() -> ExecResult[str]:
    return ExecResult(value="[m67]", n_partitions=1, n_combines=0, stopped=StopReason.EXHAUSTED)


def test_result_before_done_refuses_even_with_a_stale_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "result.pkl").write_bytes(pickle.dumps((True, done_result())))
    record_bindings(monkeypatch, RecordingSchedd(queue=[[queue_ad("running")]]))
    with pytest.raises(Exception) as excinfo:
        make_handle(tmp_path).result()
    assert not isinstance(excinfo.value, NotImplementedError), repr(excinfo.value)
    assert "running" in str(excinfo.value), repr(excinfo.value)


def test_result_of_a_job_that_left_the_queue(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "result.pkl").write_bytes(pickle.dumps((True, done_result())))
    record_bindings(
        monkeypatch, RecordingSchedd(queue=[[]], history=[dict(HISTORY["left-queue-exit-0"]["ad"])])
    )
    assert make_handle(tmp_path).result() == done_result()


def test_result_of_a_spooled_job_retrieves_the_sandbox(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    schedd = RecordingSchedd(
        queue=[[queue_ad("completed-exit-0")]],
        retrieved={"result.pkl": pickle.dumps((True, done_result())), "driver.log": b"m67\n"},
        retrieve_to=tmp_path,
    )
    record_bindings(monkeypatch, schedd)
    assert not (tmp_path / "result.pkl").exists()
    assert make_handle(tmp_path, site="lpc").result() == done_result()
    retrieves = [entry for entry in schedd.log if entry[0] == "retrieve"]
    assert retrieves and str(CLUSTER) in retrieves[0][1], schedd.log
    assert_located_by_name(schedd.log)


def test_result_re_raises_the_pickled_exception_intact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "result.pkl").write_bytes(pickle.dumps((False, make_stage_error())))
    record_bindings(monkeypatch, RecordingSchedd(queue=[[queue_ad("completed-exit-3")]]))
    with pytest.raises(BaseException) as excinfo:
        make_handle(tmp_path).result()
    assert_intact_stage_error(excinfo.value)


def test_remove_acts_on_the_cluster(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    schedd = RecordingSchedd(queue=[[queue_ad("running")]])
    record_bindings(monkeypatch, schedd)
    make_handle(tmp_path).remove()
    acts = [entry for entry in schedd.log if entry[0] == "act"]
    assert len(acts) == 1 and acts[0][1] == "Remove" and str(CLUSTER) in acts[0][2], acts
    assert_located_by_name(schedd.log)


def test_save_and_load_round_trip_through_json(tmp_path: Path) -> None:
    api = htcondor_api()
    handle = make_handle(tmp_path / "logs", site="lpc")
    path = tmp_path / "run-handle.json"
    handle.save(path)
    json.loads(path.read_text())
    loaded = api.RunHandle.load(path)
    assert (loaded.site, loaded.schedd, loaded.cluster) == ("lpc", FAKE_SCHEDD, CLUSTER)
    assert Path(loaded.log_dir) == tmp_path / "logs"
    assert loaded.submitted_at == SUBMITTED_AT
