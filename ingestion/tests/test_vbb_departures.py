"""Tests for the VBB collector: polling, error handling, logbook rows and the Double-Run test."""

from __future__ import annotations

import duckdb
import httpx

from ingestion import vbb_departures
from ingestion.stations import STATIONS
from ingestion.tests.conftest import json_response, make_mock_client


def _no_sleep(_seconds: float) -> None:
    """Skip waiting in tests."""


def test_station_with_no_departures_is_ok_empty(polled_at):
    client = make_mock_client(lambda request: json_response({"departures": []}))
    departures, quarantine, poll_runs = vbb_departures.collect_once(client, STATIONS[:1], polled_at, sleep=_no_sleep)
    assert departures == [] and quarantine == []
    assert poll_runs[0]["status"] == "ok_empty"


def test_server_error_is_logged_not_raised(polled_at):
    calls = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["count"] += 1
        return httpx.Response(503)

    client = make_mock_client(handler)
    _, _, poll_runs = vbb_departures.collect_once(client, STATIONS[:1], polled_at, sleep=_no_sleep)
    assert poll_runs[0]["status"] == "error"
    assert calls["count"] == vbb_departures.MAX_ATTEMPTS  # It retried before giving up


def test_client_error_is_not_retried(polled_at):
    calls = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["count"] += 1
        return httpx.Response(404)

    client = make_mock_client(handler)
    _, _, poll_runs = vbb_departures.collect_once(client, STATIONS[:1], polled_at, sleep=_no_sleep)
    assert poll_runs[0]["status"] == "error" and calls["count"] == 1


def test_mixed_batch_counts(make_raw_departure, polled_at):
    good = make_raw_departure()
    payload = {"departures": [good, good, make_raw_departure(delay=99_999)]}
    client = make_mock_client(lambda request: json_response(payload))
    departures, quarantine, poll_runs = vbb_departures.collect_once(client, STATIONS[:1], polled_at, sleep=_no_sleep)
    log = poll_runs[0]
    assert log["departures_received"] == 3
    assert log["departures_valid"] == 1
    assert log["departures_quarantined"] == 1
    assert log["duplicates_dropped"] == 1
    assert len(departures) == 1 and len(quarantine) == 1


def test_double_run_creates_no_duplicates(make_raw_departure, polled_at, tmp_db, pipelines_dir):
    """Run the same poll twice: the row counts must not change (idempotency)."""
    payload = {"departures": [make_raw_departure(), make_raw_departure(), make_raw_departure(delay=99_999)]}

    def run_once() -> None:
        client = make_mock_client(lambda request: json_response(payload))
        vbb_departures.run("dev", polled_at=polled_at, client=client, local_db_path=tmp_db,
                           pipelines_dir=pipelines_dir, sleep=_no_sleep)

    def counts() -> dict[str, int]:
        with duckdb.connect(str(tmp_db), read_only=True) as con:
            return {
                table: con.execute(f"SELECT COUNT(*) FROM transit.{table}").fetchone()[0]  # nosec B608
                for table in ("vbb_departures", "vbb_poll_runs", "quarantine_events")
            }

    run_once()
    first = counts()
    run_once()
    second = counts()

    stations = len(STATIONS)
    assert first == second
    assert first["vbb_departures"] == 2 * stations  # 2 valid departures at each station
    assert first["vbb_poll_runs"] == stations
    assert first["quarantine_events"] == stations

    with duckdb.connect(str(tmp_db), read_only=True) as con:
        duplicate_keys = con.execute(
            "SELECT COUNT(*) FROM (SELECT departure_key, poll_slot_utc, station_id FROM transit.vbb_departures "
            "GROUP BY ALL HAVING COUNT(*) > 1)"
        ).fetchone()[0]
    assert duplicate_keys == 0
