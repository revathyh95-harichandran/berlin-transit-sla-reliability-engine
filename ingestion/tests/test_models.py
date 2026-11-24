"""Tests for the data contract: good rows pass, broken rows go to quarantine with a reason."""

from __future__ import annotations

from ingestion.common import to_poll_slot
from ingestion.models import validate_departures


def _validate(raw_rows, polled_at):
    return validate_departures(raw_rows, "900100003", polled_at, to_poll_slot(polled_at))


def test_valid_departure_passes(make_raw_departure, polled_at):
    valid, quarantined, duplicates = _validate([make_raw_departure()], polled_at)
    assert len(valid) == 1 and not quarantined and duplicates == 0
    row = valid[0]
    assert row["product"] == "tram"
    assert row["planned_departure"].tzinfo is not None
    assert len(row["departure_key"]) == 64  # SHA-256 hex


def test_cancelled_departure_without_delay_is_valid(make_raw_departure, polled_at):
    raw = make_raw_departure(cancelled=True, when=None, delay=None)
    valid, quarantined, _ = _validate([raw], polled_at)
    assert len(valid) == 1 and not quarantined
    assert valid[0]["is_cancelled"] is True
    assert valid[0]["delay_seconds"] is None


def test_missing_planned_time_is_quarantined(make_raw_departure, polled_at):
    valid, quarantined, _ = _validate([make_raw_departure(plannedWhen=None)], polled_at)
    assert not valid
    assert "planned_departure" in quarantined[0]["reason"]


def test_absurd_delay_is_quarantined(make_raw_departure, polled_at):
    valid, quarantined, _ = _validate([make_raw_departure(delay=40_000)], polled_at)
    assert not valid
    assert "delay_seconds" in quarantined[0]["reason"]


def test_out_of_scope_product_is_quarantined(make_raw_departure, polled_at):
    raw = make_raw_departure()
    raw["line"]["product"] = "ferry"
    valid, quarantined, _ = _validate([raw], polled_at)
    assert not valid
    assert "not in scope" in quarantined[0]["reason"]


def test_coordinates_outside_berlin_are_quarantined(make_raw_departure, polled_at):
    raw = make_raw_departure()
    raw["stop"]["location"]["latitude"] = 0.0
    valid, quarantined, _ = _validate([raw], polled_at)
    assert not valid
    assert "stop_latitude" in quarantined[0]["reason"]


def test_non_object_record_is_quarantined(polled_at):
    valid, quarantined, _ = _validate(["not a departure"], polled_at)
    assert not valid and quarantined[0]["reason"] == "record is not a JSON object"


def test_duplicates_in_one_response_are_dropped(make_raw_departure, polled_at):
    raw = make_raw_departure()
    valid, quarantined, duplicates = _validate([raw, raw], polled_at)
    assert len(valid) == 1 and duplicates == 1 and not quarantined


def test_umlauts_survive(make_raw_departure, polled_at):
    raw = make_raw_departure(direction="Hohenschönhausen, Zingster Str.")
    valid, _, _ = _validate([raw], polled_at)
    assert valid[0]["direction"] == "Hohenschönhausen, Zingster Str."


def test_poll_slot_rounds_down_to_quarter_hour(polled_at):
    assert to_poll_slot(polled_at).minute == 0
    assert to_poll_slot(polled_at.replace(minute=29)).minute == 15
