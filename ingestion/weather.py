"""Collect hourly Berlin weather from Open-Meteo.

Each run loads the run date and the day before it, so late corrections are picked up.
Future hours are flagged `is_forecast = true`. A later run overwrites them with the
measured values through a MERGE on `hour_utc`: like replacing the weather forecast
in your diary with what actually happened.

Times are requested in UTC to avoid the twice-a-year daylight saving jump.
The Silver layer converts them to Berlin time.

Run it:
    python -m ingestion.weather --target dev
    python -m ingestion.weather --target dev --run-date 2026-11-20   # backfill one day
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import dlt
import httpx
from pydantic import ValidationError

from ingestion.common import apply_local_retention, build_arg_parser, build_pipeline, today_in_berlin, utc_now
from ingestion.models import WeatherHour, build_quarantine_row, short_reason

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
BERLIN_LATITUDE = 52.52
BERLIN_LONGITUDE = 13.405
HOURLY_VARIABLES = [
    "temperature_2m",
    "precipitation",
    "rain",
    "snowfall",
    "wind_speed_10m",
    "wind_gusts_10m",
    "weather_code",
]


def fetch_weather(client: httpx.Client, start: date, end: date) -> dict[str, Any]:
    """Download hourly weather for Berlin between two dates (inclusive)."""
    params = {
        "latitude": BERLIN_LATITUDE,
        "longitude": BERLIN_LONGITUDE,
        "hourly": ",".join(HOURLY_VARIABLES),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "timezone": "UTC",
        "wind_speed_unit": "kmh",
    }
    response = client.get(OPEN_METEO_URL, params=params)
    response.raise_for_status()
    return response.json()


def parse_weather(payload: dict[str, Any], fetched_at: datetime,
                  ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Turn Open-Meteo's column lists into one row per hour, and validate each row.

    Open-Meteo returns parallel lists, e.g. time=[...], rain=[...]. Row i is
    built from position i of every list, like reading one line across a timetable.
    """
    hourly = payload.get("hourly") or {}
    times = hourly.get("time") or []
    slot = fetched_at.replace(minute=0, second=0, microsecond=0)
    valid: list[dict[str, Any]] = []
    quarantined: list[dict[str, Any]] = []

    for i, time_text in enumerate(times):
        def value(name: str) -> Any:
            column = hourly.get(name) or []
            return column[i] if i < len(column) else None

        raw_row = {"time": time_text, **{name: value(name) for name in HOURLY_VARIABLES}}
        try:
            hour_utc = datetime.fromisoformat(time_text).replace(tzinfo=timezone.utc)
            row = WeatherHour(
                hour_utc=hour_utc,
                temperature_c=value("temperature_2m"),
                precipitation_mm=value("precipitation"),
                rain_mm=value("rain"),
                snowfall_cm=value("snowfall"),
                wind_speed_kmh=value("wind_speed_10m"),
                wind_gusts_kmh=value("wind_gusts_10m"),
                weather_code=value("weather_code"),
                is_forecast=hour_utc > fetched_at,
                fetched_at_utc=fetched_at,
            ).model_dump()
            valid.append(row)
        except (ValidationError, ValueError, TypeError) as error:
            reason = short_reason(error) if isinstance(error, ValidationError) else str(error)
            quarantined.append(build_quarantine_row("open_meteo", None, slot, fetched_at, reason, raw_row))

    return valid, quarantined


def load(pipeline: dlt.Pipeline, rows: list[dict[str, Any]], quarantine: list[dict[str, Any]]) -> None:
    """MERGE on hour_utc: one row per hour, always the newest version."""
    resources = [dlt.resource(rows, name="weather_hourly", write_disposition="merge", primary_key="hour_utc")]
    if quarantine:
        resources.append(dlt.resource(quarantine, name="quarantine_events", write_disposition="merge",
                                      primary_key="quarantine_id"))
    pipeline.run(resources)


def run(target: str, run_date: date | None = None, client: httpx.Client | None = None,
        local_db_path: Path | None = None, pipelines_dir: Path | None = None) -> dict[str, int]:
    """Fetch, validate and load weather for run_date and the day before."""
    run_date = run_date or today_in_berlin()
    fetched_at = utc_now()
    own_client = client is None
    if own_client:
        client = httpx.Client(timeout=30, headers={"User-Agent": "berlin-transit-sla-collector/1.0"})
    try:
        payload = fetch_weather(client, run_date - timedelta(days=1), run_date)
    finally:
        if own_client:
            client.close()

    rows, quarantine = parse_weather(payload, fetched_at)
    pipeline = build_pipeline(target, "weather", local_db_path, pipelines_dir)
    load(pipeline, rows, quarantine)

    deleted = apply_local_retention(pipeline, "weather_hourly", "hour_utc") if target == "dev" and rows else 0
    return {"hours_loaded": len(rows), "quarantined": len(quarantine), "rows_deleted_by_retention": deleted}


def main() -> int:
    args = build_arg_parser("Load hourly Berlin weather from Open-Meteo.").parse_args()
    summary = run(args.target, args.run_date)
    print(f"Weather load finished: {summary}")
    return 0 if summary["hours_loaded"] else 1


if __name__ == "__main__":
    sys.exit(main())
