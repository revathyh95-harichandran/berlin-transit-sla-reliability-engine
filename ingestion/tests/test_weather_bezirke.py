"""Tests for the weather and Bezirke collectors."""

from __future__ import annotations

from datetime import date, datetime, timezone

import duckdb
import pytest

from ingestion import bezirke, weather
from ingestion.tests.conftest import json_response, make_mock_client


def _weather_payload(precipitation: list[float]) -> dict:
    hours = [f"2026-11-24T{h:02d}:00" for h in range(len(precipitation))]
    n = len(hours)
    return {"hourly": {
        "time": hours,
        "temperature_2m": [3.5] * n,
        "precipitation": precipitation,
        "rain": precipitation,
        "snowfall": [0.0] * n,
        "wind_speed_10m": [12.0] * n,
        "wind_gusts_10m": [20.0] * n,
        "weather_code": [3] * n,
    }}


def test_weather_rows_are_utc_and_flag_forecasts():
    fetched_at = datetime(2026, 11, 24, 1, 30, tzinfo=timezone.utc)
    rows, quarantine = weather.parse_weather(_weather_payload([0.0, 0.2, 1.5]), fetched_at)
    assert len(rows) == 3 and not quarantine
    assert rows[0]["hour_utc"].tzinfo is not None
    assert [row["is_forecast"] for row in rows] == [False, False, True]


def test_impossible_weather_is_quarantined():
    fetched_at = datetime(2026, 11, 24, 12, 0, tzinfo=timezone.utc)
    rows, quarantine = weather.parse_weather(_weather_payload([0.0, -5.0]), fetched_at)
    assert len(rows) == 1 and len(quarantine) == 1
    assert quarantine[0]["source"] == "open_meteo"


def test_weather_double_run_creates_no_duplicates(tmp_db, pipelines_dir):
    client = make_mock_client(lambda request: json_response(_weather_payload([0.0] * 24)))
    for _ in range(2):
        weather.run("dev", run_date=date(2026, 11, 24), client=client, local_db_path=tmp_db,
                    pipelines_dir=pipelines_dir)
    with duckdb.connect(str(tmp_db), read_only=True) as con:
        assert con.execute("SELECT COUNT(*) FROM transit.weather_hourly").fetchone()[0] == 24


def _bezirk_feature(name: str, code: str) -> dict:
    return {"type": "Feature",
            "properties": {"namgem": name, "gem": code},
            "geometry": {"type": "Polygon", "coordinates": [[[13.3, 52.5], [13.4, 52.5], [13.4, 52.6], [13.3, 52.5]]]}}


def test_bezirke_requires_exactly_twelve_districts():
    fetched_at = datetime(2026, 11, 24, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="expected 12"):
        bezirke.parse_bezirke({"features": [_bezirk_feature("Mitte", "001")]}, fetched_at)


def test_bezirke_keeps_umlauts():
    fetched_at = datetime(2026, 11, 24, tzinfo=timezone.utc)
    features = [_bezirk_feature(f"Bezirk {i}", f"{i:03d}") for i in range(1, 12)]
    features.append(_bezirk_feature("Neukölln", "008"))
    rows = bezirke.parse_bezirke({"features": features}, fetched_at)
    assert len(rows) == 12
    assert "Neukölln" in {row["bezirk_name"] for row in rows}
