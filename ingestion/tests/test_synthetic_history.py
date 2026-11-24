"""Tests for the synthetic practice dataset: labelled, reproducible, realistic enough, and messy on purpose."""

from __future__ import annotations

from collections import Counter
from datetime import date

import pytest

from ingestion import synthetic_history


@pytest.fixture(scope="module")
def dataset():
    # 3 days keep the test fast; the real run uses 14.
    return synthetic_history.build_dataset(date(2026, 11, 23), days=3)


def test_every_row_is_labelled_synthetic(dataset):
    departures, weather, quarantine, _ = dataset
    assert all(row["is_synthetic"] for row in departures)
    assert all(row["is_synthetic"] for row in weather)
    assert all(row["is_synthetic"] for row in quarantine)


def test_same_seed_gives_same_data(dataset):
    again = synthetic_history.build_dataset(date(2026, 11, 23), days=3)
    assert len(again[0]) == len(dataset[0])
    assert again[0][0]["departure_key"] == dataset[0][0]["departure_key"]


def test_messy_rows_are_caught(dataset):
    departures, _, quarantine, duplicates = dataset
    assert quarantine, "some broken rows should be quarantined"
    assert duplicates > 0, "some duplicates should be dropped"
    # Roughly 2 % of rows are damaged on purpose; check the order of magnitude.
    assert 0.002 < len(quarantine) / len(departures) < 0.03


def test_all_modes_and_stations_present(dataset):
    departures = dataset[0]
    assert Counter(row["product"] for row in departures).keys() == {"suburban", "subway", "tram", "bus"}
    assert len({row["station_id"] for row in departures}) == 24


def test_weather_covers_every_hour(dataset):
    weather = dataset[1]
    assert len(weather) == 3 * 24
    assert len({row["hour_utc"] for row in weather}) == len(weather)
