"""Data contracts (Pydantic models) and the quarantine logic.

A data contract is like the checklist at a post office counter: a parcel needs an
address, a stamp and a sensible weight. Parcels that fail the checklist are not
thrown away; they go to the "return to sender" shelf. Here that shelf is the
`quarantine_events` table, also called a dead letter queue (DLQ).
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from ingestion.common import make_key

# Products (modes) in scope: S-Bahn, U-Bahn, Tram and Bus.
ALLOWED_PRODUCTS = {"suburban", "subway", "tram", "bus"}

# Plausible delay range in seconds: up to 10 minutes early, up to 3 hours late.
MIN_DELAY_SECONDS = -600
MAX_DELAY_SECONDS = 3 * 60 * 60

# Loose bounding box around Berlin to catch broken coordinates.
BERLIN_LAT_RANGE = (52.30, 52.70)
BERLIN_LON_RANGE = (13.05, 13.80)


# =============================================================================
# Departure contract
# =============================================================================
class Departure(BaseModel):
    """One planned departure of one trip from one stop, as seen in one poll."""

    model_config = ConfigDict(extra="ignore")

    departure_key: str  # Fingerprint of trip + stop + planned time
    trip_id: str = Field(min_length=1)
    vehicle_trip_number: str | None = None  # "fahrtNr" in VBB data
    station_id: str  # The station that was polled
    stop_id: str  # The exact stop/platform group the vehicle leaves from
    stop_name: str | None = None
    stop_latitude: float | None = None
    stop_longitude: float | None = None
    planned_departure: datetime
    predicted_departure: datetime | None = None
    delay_seconds: int | None = None
    is_cancelled: bool = False
    line_id: str | None = None
    line_name: str = Field(min_length=1)
    product: str
    direction: str | None = None
    operator_name: str | None = None
    prognosis_type: str | None = None
    polled_at_utc: datetime
    poll_slot_utc: datetime

    @field_validator("planned_departure", "predicted_departure", "polled_at_utc", "poll_slot_utc")
    @classmethod
    def must_have_time_zone(cls, value: datetime | None) -> datetime | None:
        """Times without a time zone are ambiguous (is 08:00 Berlin or UTC?), so reject them."""
        if value is not None and value.tzinfo is None:
            raise ValueError("timestamp has no time zone")
        return value

    @field_validator("product")
    @classmethod
    def product_in_scope(cls, value: str) -> str:
        """Only S-Bahn, U-Bahn, Tram and Bus are part of this SLA analysis."""
        if value not in ALLOWED_PRODUCTS:
            raise ValueError(f"product '{value}' is not in scope")
        return value

    @field_validator("delay_seconds")
    @classmethod
    def delay_is_plausible(cls, value: int | None) -> int | None:
        """A delay of 9 hours is almost certainly a data error, not a real delay."""
        if value is not None and not MIN_DELAY_SECONDS <= value <= MAX_DELAY_SECONDS:
            raise ValueError(f"delay {value}s is outside the plausible range")
        return value

    @field_validator("stop_latitude")
    @classmethod
    def latitude_in_berlin(cls, value: float | None) -> float | None:
        if value is not None and not BERLIN_LAT_RANGE[0] <= value <= BERLIN_LAT_RANGE[1]:
            raise ValueError(f"latitude {value} is outside Berlin")
        return value

    @field_validator("stop_longitude")
    @classmethod
    def longitude_in_berlin(cls, value: float | None) -> float | None:
        if value is not None and not BERLIN_LON_RANGE[0] <= value <= BERLIN_LON_RANGE[1]:
            raise ValueError(f"longitude {value} is outside Berlin")
        return value


# =============================================================================
# Weather contract
# =============================================================================
class WeatherHour(BaseModel):
    """Weather for one hour in Berlin (all times in UTC)."""

    model_config = ConfigDict(extra="ignore")

    hour_utc: datetime
    temperature_c: float = Field(ge=-40, le=45)
    precipitation_mm: float = Field(ge=0, le=100)
    rain_mm: float = Field(ge=0, le=100)
    snowfall_cm: float = Field(ge=0, le=50)
    wind_speed_kmh: float = Field(ge=0, le=200)
    wind_gusts_kmh: float | None = Field(default=None, ge=0, le=250)
    weather_code: int | None = None
    is_forecast: bool
    fetched_at_utc: datetime

    @field_validator("hour_utc", "fetched_at_utc")
    @classmethod
    def must_have_time_zone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("timestamp has no time zone")
        return value


# =============================================================================
# Helpers: flatten raw API JSON into flat rows
# =============================================================================
def _get(data: Any, *path: str) -> Any:
    """Safely read a nested value: _get(raw, "line", "name") -> raw["line"]["name"] or None."""
    for key in path:
        if not isinstance(data, dict):
            return None
        data = data.get(key)
    return data


def flatten_departure(raw: dict[str, Any], station_id: str, polled_at: datetime, poll_slot: datetime) -> dict[str, Any]:
    """Turn one nested VBB departure into one flat row (no validation yet)."""
    trip_id = raw.get("tripId")
    stop_id = _get(raw, "stop", "id")
    planned = raw.get("plannedWhen")
    return {
        "departure_key": make_key(trip_id, stop_id, planned),
        "trip_id": trip_id,
        "vehicle_trip_number": _get(raw, "line", "fahrtNr"),
        "station_id": station_id,
        "stop_id": stop_id,
        "stop_name": _get(raw, "stop", "name"),
        "stop_latitude": _get(raw, "stop", "location", "latitude"),
        "stop_longitude": _get(raw, "stop", "location", "longitude"),
        "planned_departure": planned,
        "predicted_departure": raw.get("when"),
        "delay_seconds": raw.get("delay"),
        "is_cancelled": bool(raw.get("cancelled", False)),
        "line_id": _get(raw, "line", "id"),
        "line_name": _get(raw, "line", "name"),
        "product": _get(raw, "line", "product"),
        "direction": raw.get("direction"),
        "operator_name": _get(raw, "line", "operator", "name"),
        "prognosis_type": raw.get("prognosisType"),
        "polled_at_utc": polled_at,
        "poll_slot_utc": poll_slot,
    }


def build_quarantine_row(source: str, station_id: str | None, poll_slot: datetime, polled_at: datetime,
                         reason: str, raw_record: Any) -> dict[str, Any]:
    """Create one row for the quarantine table (the "return to sender" shelf)."""
    payload = json.dumps(raw_record, ensure_ascii=False, default=str, sort_keys=True)
    return {
        "quarantine_id": make_key(source, station_id, poll_slot.isoformat(), payload),
        "source": source,
        "station_id": station_id,
        "poll_slot_utc": poll_slot,
        "polled_at_utc": polled_at,
        "reason": reason[:500],
        "raw_payload": payload,
    }


def short_reason(error: ValidationError) -> str:
    """Make a short, readable reason from a Pydantic error, e.g. 'delay_seconds: delay 40000s is outside ...'."""
    parts = []
    for item in error.errors():
        field = ".".join(str(piece) for piece in item.get("loc", ())) or "record"
        parts.append(f"{field}: {item.get('msg', 'invalid')}")
    return "; ".join(parts)


def validate_departures(raw_departures: list[dict[str, Any]], station_id: str, polled_at: datetime,
                        poll_slot: datetime, source: str = "vbb_departures",
                        ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    """Check every raw departure against the contract.

    Returns three things:
      * valid rows (ready to load)
      * quarantined rows (with the reason they failed)
      * how many exact duplicates were dropped inside this batch
    """
    valid: list[dict[str, Any]] = []
    quarantined: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    duplicates = 0

    for raw in raw_departures:
        if not isinstance(raw, dict):
            quarantined.append(build_quarantine_row(source, station_id, poll_slot, polled_at,
                                                    "record is not a JSON object", raw))
            continue

        flat = flatten_departure(raw, station_id, polled_at, poll_slot)
        try:
            row = Departure(**flat).model_dump()
        except ValidationError as error:
            quarantined.append(build_quarantine_row(source, station_id, poll_slot, polled_at,
                                                    short_reason(error), raw))
            continue

        # The same departure can appear twice in one response; keep it once.
        if row["departure_key"] in seen_keys:
            duplicates += 1
            continue
        seen_keys.add(row["departure_key"])
        valid.append(row)

    return valid, quarantined, duplicates
