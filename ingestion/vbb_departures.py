"""Collect live departures from the VBB transit API for the 24 sampled stations.

How it works (one run = one "poll"):
  1. For each station, ask VBB: "what leaves in the next 30 minutes?"
  2. Check every departure against the data contract (ingestion/models.py).
     Good rows -> vbb_departures, broken rows -> quarantine_events.
  3. Write one logbook row per station into vbb_poll_runs
     (ok / ok_empty / error), so missing or empty polls are visible.
  4. Load everything with a MERGE on (departure_key, station_id, poll_slot_utc), so running
     the same poll twice never creates duplicate rows.

The window is 30 minutes while polls run every 15 minutes, so each departure is
usually seen twice. If a scheduled run starts up to 15 minutes late, nothing is missed.

Run it:
    python -m ingestion.vbb_departures --target dev
    python -m ingestion.vbb_departures --target prod
"""

from __future__ import annotations

import sys
import time
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

import dlt
import httpx

from ingestion.common import (
    apply_local_retention,
    build_arg_parser,
    build_pipeline,
    to_poll_slot,
    today_in_berlin,
    utc_now,
)
from ingestion.models import validate_departures
from ingestion.stations import STATIONS, Station

VBB_BASE_URL = "https://v6.vbb.transport.rest"
DEPARTURE_WINDOW_MINUTES = 30
REQUEST_TIMEOUT_SECONDS = 20
MAX_ATTEMPTS = 3
PAUSE_BETWEEN_STATIONS_SECONDS = 0.5  # Be polite to the free public API.
USER_AGENT = "berlin-transit-sla-collector/1.0"

# Ask the API only for the modes in scope (fewer bytes, fewer rows to filter).
PRODUCT_FILTER = {
    "suburban": "true",
    "subway": "true",
    "tram": "true",
    "bus": "true",
    "ferry": "false",
    "express": "false",
    "regional": "false",
}


# -----------------------------------------------------------------------------
# Step 1: Fetch one station, with retries
# -----------------------------------------------------------------------------
def fetch_station_departures(client: httpx.Client, station_id: str,
                             sleep: Callable[[float], None] = time.sleep) -> list[dict[str, Any]]:
    """Ask VBB for the next departures at one station.

    Retries up to 3 times on network errors, rate limits (HTTP 429) and server
    errors (HTTP 5xx), waiting 1 s, then 2 s between attempts. Like redialling
    a busy phone line, but not forever.
    """
    params = {
        "duration": DEPARTURE_WINDOW_MINUTES,
        "remarks": "false",
        "language": "en",
        **PRODUCT_FILTER,
    }
    last_error: Exception | None = None

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = client.get(f"/stops/{station_id}/departures", params=params)
            if response.status_code == 429 or response.status_code >= 500:
                raise httpx.HTTPStatusError(
                    f"HTTP {response.status_code}", request=response.request, response=response
                )
            response.raise_for_status()
            body = response.json()
            departures = body.get("departures") if isinstance(body, dict) else body
            if not isinstance(departures, list):
                raise ValueError("response has no 'departures' list")
            return departures
        except (httpx.TransportError, httpx.HTTPStatusError, ValueError) as error:
            last_error = error
            # A 4xx error other than 429 will not fix itself, so stop retrying.
            if isinstance(error, httpx.HTTPStatusError) and 400 <= error.response.status_code < 500 \
                    and error.response.status_code != 429:
                break
            if attempt < MAX_ATTEMPTS:
                sleep(2 ** (attempt - 1))

    raise RuntimeError(f"station {station_id} failed after retries: {last_error}")


# -----------------------------------------------------------------------------
# Step 2: Poll all stations once
# -----------------------------------------------------------------------------
def collect_once(client: httpx.Client, stations: tuple[Station, ...], polled_at: datetime,
                 sleep: Callable[[float], None] = time.sleep,
                 ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Poll every station once and return (departures, quarantine rows, poll-run log rows)."""
    poll_slot = to_poll_slot(polled_at)
    all_departures: list[dict[str, Any]] = []
    all_quarantine: list[dict[str, Any]] = []
    poll_runs: list[dict[str, Any]] = []

    for index, station in enumerate(stations):
        if index > 0:
            sleep(PAUSE_BETWEEN_STATIONS_SECONDS)

        log_row: dict[str, Any] = {
            "station_id": station.station_id,
            "station_name": station.name,
            "poll_slot_utc": poll_slot,
            "polled_at_utc": polled_at,
            "status": "ok",
            "departures_received": 0,
            "departures_valid": 0,
            "departures_quarantined": 0,
            "duplicates_dropped": 0,
            "error_message": None,
        }

        try:
            raw_departures = fetch_station_departures(client, station.station_id, sleep=sleep)
        except RuntimeError as error:
            # The station failed: log it and carry on with the next one.
            log_row["status"] = "error"
            log_row["error_message"] = str(error)[:500]
            poll_runs.append(log_row)
            continue

        valid, quarantined, duplicates = validate_departures(
            raw_departures, station.station_id, polled_at, poll_slot
        )
        all_departures.extend(valid)
        all_quarantine.extend(quarantined)

        log_row["departures_received"] = len(raw_departures)
        log_row["departures_valid"] = len(valid)
        log_row["departures_quarantined"] = len(quarantined)
        log_row["duplicates_dropped"] = duplicates
        # At 03:00 many stations have nothing leaving: that is normal, not an error.
        if not raw_departures:
            log_row["status"] = "ok_empty"
        poll_runs.append(log_row)

    return all_departures, all_quarantine, poll_runs


# -----------------------------------------------------------------------------
# Step 3: Load into the warehouse
# -----------------------------------------------------------------------------
def load(pipeline: dlt.Pipeline, departures: list[dict[str, Any]], quarantine: list[dict[str, Any]],
         poll_runs: list[dict[str, Any]]) -> None:
    """Load the three tables with MERGE (update-or-insert) so re-runs never duplicate rows."""
    resources = [
        dlt.resource(poll_runs, name="vbb_poll_runs", write_disposition="merge",
                     primary_key=["station_id", "poll_slot_utc"]),
    ]
    if departures:
        resources.append(dlt.resource(departures, name="vbb_departures", write_disposition="merge",
                                      primary_key=["departure_key", "station_id", "poll_slot_utc"]))
    if quarantine:
        resources.append(dlt.resource(quarantine, name="quarantine_events", write_disposition="merge",
                                      primary_key="quarantine_id"))
    pipeline.run(resources)


def run(target: str, polled_at: datetime | None = None, client: httpx.Client | None = None,
        local_db_path: Path | None = None, pipelines_dir: Path | None = None,
        sleep: Callable[[float], None] = time.sleep) -> dict[str, int]:
    """Run one full poll: fetch, validate, load. Returns a small summary."""
    polled_at = polled_at or utc_now()
    own_client = client is None
    if own_client:
        client = httpx.Client(base_url=VBB_BASE_URL, timeout=REQUEST_TIMEOUT_SECONDS,
                              headers={"User-Agent": USER_AGENT})
    try:
        departures, quarantine, poll_runs = collect_once(client, STATIONS, polled_at, sleep=sleep)
    finally:
        if own_client:
            client.close()

    pipeline = build_pipeline(target, "vbb_departures", local_db_path, pipelines_dir)
    load(pipeline, departures, quarantine, poll_runs)

    deleted = 0
    if target == "dev" and departures:
        deleted = apply_local_retention(pipeline, "vbb_departures", "poll_slot_utc")

    return {
        "stations": len(poll_runs),
        "stations_error": sum(1 for row in poll_runs if row["status"] == "error"),
        "stations_empty": sum(1 for row in poll_runs if row["status"] == "ok_empty"),
        "departures_loaded": len(departures),
        "quarantined": len(quarantine),
        "duplicates_dropped": sum(row["duplicates_dropped"] for row in poll_runs),
        "rows_deleted_by_retention": deleted,
    }


def main() -> int:
    parser = build_arg_parser("Poll VBB departures for the 24 sampled stations.")
    args = parser.parse_args()

    # VBB only shows live data, so a past date cannot be backfilled.
    # Use ingestion.synthetic_history for historical practice data instead.
    if args.run_date and args.run_date != today_in_berlin():
        print("VBB has no history API: --run-date can only be today. "
              "Use `python -m ingestion.synthetic_history` for past dates.")
        return 2

    summary = run(args.target)
    print(f"VBB poll finished: {summary}")

    # Fail the run only if every station failed (the API is probably down).
    return 1 if summary["stations_error"] == summary["stations"] else 0


if __name__ == "__main__":
    sys.exit(main())
