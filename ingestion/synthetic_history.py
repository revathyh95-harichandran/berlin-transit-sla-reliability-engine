"""Generate a labelled 14-day synthetic practice dataset (departures + weather).

Why: the VBB API has no history, and a few days of real collection may include no
snow or storms. This generator creates a realistic but clearly fake history, so the
models, tests and dashboards can be built and exercised before real data accumulates.
Think of it as a flight simulator: useful for practice, never mistaken for a real flight.

Rules:
  * Every row has `is_synthetic = true` and lives in its own tables
    (synthetic_departures, synthetic_weather_hourly). It is never mixed with real data.
  * The same random seed always produces the same data, so results are reproducible.
  * The rows are built in the same shape as real VBB responses and go through the
    same validation, so the quarantine (DLQ) logic is exercised too.
    Like real feeds, about 2 % of the rows are messy on purpose.

How the delays are simulated (assumptions, not measurements):
  * Base delay depends on the mode: U-Bahn smallest, then S-Bahn, Tram, Bus.
  * Weekday peak hours (07-09, 16-19 Berlin time) add more delay.
  * Rain, snow and strong wind add delay. Snow hits S-Bahn, Tram and Bus hardest.
  * A small share of departures hit a random incident (10-25 extra minutes).

Run it:
    python -m ingestion.synthetic_history --target dev
    python -m ingestion.synthetic_history --target dev --run-date 2026-11-23   # last day of the 14
"""

from __future__ import annotations

import random
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import dlt
from faker import Faker

from ingestion.common import BERLIN_TZ, build_arg_parser, build_pipeline, to_poll_slot, today_in_berlin
from ingestion.models import validate_departures
from ingestion.stations import STATIONS, Station

SEED = 42
DAYS = 14

# Departures per hour per line, by mode (daytime service).
DEPARTURES_PER_HOUR = {"subway": 10, "suburban": 6, "tram": 6, "bus": 5}

# Typical "normal" delay in seconds, by mode (centre of the distribution).
BASE_DELAY_SECONDS = {"subway": 35, "suburban": 80, "tram": 95, "bus": 130}

# How much each weather condition multiplies the delay, by mode.
WEATHER_EFFECT = {
    "clear": {"subway": 1.0, "suburban": 1.0, "tram": 1.0, "bus": 1.0},
    "rain": {"subway": 1.1, "suburban": 1.4, "tram": 1.5, "bus": 1.7},
    "windy": {"subway": 1.0, "suburban": 1.6, "tram": 1.4, "bus": 1.3},
    "snow": {"subway": 1.3, "suburban": 2.6, "tram": 2.4, "bus": 2.8},
}

WEEKDAY_PEAK_HOURS = {7, 8, 9, 16, 17, 18}
PEAK_EFFECT = 1.7
INCIDENT_PROBABILITY = 0.02
BASE_CANCEL_PROBABILITY = 0.004
NO_REALTIME_PROBABILITY = 0.03
MESSY_ROW_PROBABILITY = 0.02


# =============================================================================
# Step 1: Synthetic weather
# =============================================================================
def generate_weather(start_utc: datetime, hours: int, rng: random.Random) -> list[dict[str, Any]]:
    """Create hourly November-like weather with a few rain spells, one snow spell and one windy day."""
    rows = []
    for h in range(hours):
        hour = start_utc + timedelta(hours=h)
        local_hour = hour.astimezone(BERLIN_TZ).hour
        # Mild daily temperature curve around 4 °C, warmest in the afternoon.
        temperature = 4 + 3 * (1 - abs(local_hour - 14) / 12) + rng.uniform(-1.5, 1.5)
        rows.append({
            "hour_utc": hour,
            "temperature_c": round(temperature, 1),
            "precipitation_mm": 0.0,
            "rain_mm": 0.0,
            "snowfall_cm": 0.0,
            "wind_speed_kmh": round(rng.uniform(5, 20), 1),
            "wind_gusts_kmh": None,
            "weather_code": 1,
            "is_forecast": False,
            "is_synthetic": True,
        })

    def spell(length_range: tuple[int, int]) -> range:
        length = rng.randint(*length_range)
        first = rng.randint(0, hours - length - 1)
        return range(first, first + length)

    # Four rain spells of 3-8 hours.
    for _ in range(4):
        for h in spell((3, 8)):
            amount = round(rng.uniform(0.6, 4.0), 1)
            rows[h].update(precipitation_mm=amount, rain_mm=amount, weather_code=63)

    # One snow spell of 6-10 hours, below freezing.
    for h in spell((6, 10)):
        snow = round(rng.uniform(0.4, 2.0), 1)
        rows[h].update(temperature_c=round(rng.uniform(-3, -0.5), 1), snowfall_cm=snow,
                       precipitation_mm=round(snow * 0.7, 1), rain_mm=0.0, weather_code=73)

    # One windy spell of 8-12 hours.
    for h in spell((8, 12)):
        speed = round(rng.uniform(32, 55), 1)
        rows[h].update(wind_speed_kmh=speed, wind_gusts_kmh=round(speed * 1.6, 1))

    return rows


def weather_condition(row: dict[str, Any]) -> str:
    """Same rule as the data dictionary: snow, then rain, then windy, else clear."""
    if row["snowfall_cm"] > 0:
        return "snow"
    if row["precipitation_mm"] >= 0.5:
        return "rain"
    if row["wind_speed_kmh"] >= 30:
        return "windy"
    return "clear"


# =============================================================================
# Step 2: Synthetic departures (in the same raw shape as the VBB API)
# =============================================================================
def departures_this_hour(product: str, local_time: datetime) -> int:
    """How many departures a line has in this hour: fewer at night and at weekends, more at peak."""
    per_hour = DEPARTURES_PER_HOUR[product]
    hour = local_time.hour
    is_weekend = local_time.weekday() >= 5
    if 1 <= hour <= 4:
        per_hour = 1 if product in ("bus", "tram") else 0  # Night: mostly night buses/trams
    elif hour in (0, 5, 23):
        per_hour = max(1, per_hour // 2)
    elif not is_weekend and hour in WEEKDAY_PEAK_HOURS:
        per_hour = int(per_hour * 1.5)
    if is_weekend:
        per_hour = max(0, int(per_hour * 0.7))
    return per_hour


def simulate_delay(product: str, local_time: datetime, condition: str, rng: random.Random) -> int:
    """Draw one delay in seconds from a skewed distribution (most small, a few large)."""
    centre = BASE_DELAY_SECONDS[product] * WEATHER_EFFECT[condition][product]
    if local_time.weekday() < 5 and local_time.hour in WEEKDAY_PEAK_HOURS:
        centre *= PEAK_EFFECT
    delay = rng.lognormvariate(0, 0.9) * centre - 30  # Allows a few early departures
    if rng.random() < INCIDENT_PROBABILITY:
        delay += rng.uniform(600, 1500)
    return int(max(-120, delay))


def make_raw_departure(station: Station, line: str, product: str, planned_local: datetime,
                       condition: str, rng: random.Random, fake: Faker) -> dict[str, Any]:
    """Build one departure dictionary shaped like a real VBB API record."""
    cancel_probability = BASE_CANCEL_PROBABILITY * (4 if condition == "snow" else 1)
    is_cancelled = rng.random() < cancel_probability
    has_realtime = rng.random() >= NO_REALTIME_PROBABILITY

    delay = None
    predicted = None
    if not is_cancelled and has_realtime:
        delay = simulate_delay(product, planned_local, condition, rng)
        predicted = (planned_local + timedelta(seconds=delay)).isoformat()

    record: dict[str, Any] = {
        "tripId": f"SYN|{line}|{fake.unique.random_int(10_000_000, 99_999_999)}",
        "stop": {
            "id": station.station_id,
            "name": station.name,
            "location": {"latitude": station.latitude, "longitude": station.longitude},
        },
        "plannedWhen": planned_local.isoformat(),
        "when": predicted,
        "delay": delay,
        "direction": fake.street_name(),
        "prognosisType": "prognosed" if has_realtime else None,
        "line": {
            "id": f"syn-{product}-{line.lower()}",
            "name": line,
            "product": product,
            "fahrtNr": str(rng.randint(10000, 99999)),
            "operator": {"name": "S-Bahn Berlin GmbH" if product == "suburban" else "Berliner Verkehrsbetriebe"},
        },
    }
    if is_cancelled:
        record["cancelled"] = True
    return record


def make_messy(record: dict[str, Any], rng: random.Random) -> list[dict[str, Any]]:
    """Damage a record the way real feeds get damaged. Returns 1 or 2 records."""
    problem = rng.choice(["duplicate", "missing_planned", "absurd_delay", "bad_product", "bad_coordinates"])
    broken = {**record, "stop": {**record["stop"], "location": {**record["stop"]["location"]}},
              "line": {**record["line"]}}
    if problem == "duplicate":
        return [record, record]
    if problem == "missing_planned":
        broken["plannedWhen"] = None
    elif problem == "absurd_delay":
        broken["delay"] = rng.randint(20_000, 90_000)
    elif problem == "bad_product":
        broken["line"]["product"] = "unknown"
    elif problem == "bad_coordinates":
        broken["stop"]["location"]["latitude"] = 0.0
    return [broken]


def generate_departures(stations: tuple[Station, ...], start_utc: datetime, hours: int,
                        weather_by_hour: dict[datetime, dict[str, Any]], rng: random.Random,
                        fake: Faker) -> dict[str, list[dict[str, Any]]]:
    """Generate raw departures for every station, line and hour, grouped by station."""
    raw_by_station: dict[str, list[dict[str, Any]]] = {station.station_id: [] for station in stations}

    for h in range(hours):
        hour_utc = start_utc + timedelta(hours=h)
        local_hour = hour_utc.astimezone(BERLIN_TZ)
        condition = weather_condition(weather_by_hour[hour_utc])

        for station in stations:
            for line, product in station.lines:
                count = departures_this_hour(product, local_hour)
                for n in range(count):
                    minute = int(60 * n / count) + rng.randint(0, 2) if count else 0
                    planned = local_hour + timedelta(minutes=min(minute, 59))
                    record = make_raw_departure(station, line, product, planned, condition, rng, fake)
                    if rng.random() < MESSY_ROW_PROBABILITY:
                        raw_by_station[station.station_id].extend(make_messy(record, rng))
                    else:
                        raw_by_station[station.station_id].append(record)

    return raw_by_station


# =============================================================================
# Step 3: Validate exactly like real data, then load
# =============================================================================
def build_dataset(end_date: date, days: int = DAYS, seed: int = SEED,
                  ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], int]:
    """Build the full synthetic dataset. Returns (departures, weather, quarantine, duplicates_dropped)."""
    # Plain random is fine here: it only makes practice data, nothing security-related.
    rng = random.Random(seed)  # nosec B311
    fake = Faker("de_DE")
    fake.seed_instance(seed)

    start_local = datetime.combine(end_date - timedelta(days=days - 1), datetime.min.time(), tzinfo=BERLIN_TZ)
    start_utc = start_local.astimezone(timezone.utc)
    end_utc = datetime.combine(end_date + timedelta(days=1), datetime.min.time(),
                               tzinfo=BERLIN_TZ).astimezone(timezone.utc)
    hours = int((end_utc - start_utc).total_seconds() // 3600)

    weather = generate_weather(start_utc, hours, rng)
    weather_by_hour = {row["hour_utc"]: row for row in weather}
    raw_by_station = generate_departures(STATIONS, start_utc, hours, weather_by_hour, rng, fake)

    departures: list[dict[str, Any]] = []
    quarantine: list[dict[str, Any]] = []
    duplicates = 0
    for station_id, raw_rows in raw_by_station.items():
        # Group rows into the 15-minute poll slot just before their planned time,
        # as if a collector had seen them in a real poll.
        by_slot: dict[datetime, list[dict[str, Any]]] = {}
        for raw in raw_rows:
            planned_text = raw.get("plannedWhen") or raw.get("when")
            reference = datetime.fromisoformat(planned_text) if planned_text else start_utc
            slot = to_poll_slot(reference - timedelta(minutes=15))
            by_slot.setdefault(slot, []).append(raw)

        for slot, slot_rows in by_slot.items():
            valid, bad, dropped = validate_departures(slot_rows, station_id, slot, slot, source="synthetic_vbb")
            departures.extend(valid)
            quarantine.extend(bad)
            duplicates += dropped

    for row in departures:
        row["is_synthetic"] = True
    for row in quarantine:
        row["is_synthetic"] = True
    return departures, weather, quarantine, duplicates


def run(target: str, end_date: date | None = None, local_db_path: Path | None = None,
        pipelines_dir: Path | None = None) -> dict[str, int]:
    """Generate and load the synthetic dataset (MERGE, so re-running is safe)."""
    end_date = end_date or (today_in_berlin() - timedelta(days=1))
    departures, weather, quarantine, duplicates = build_dataset(end_date)

    pipeline = build_pipeline(target, "synthetic_history", local_db_path, pipelines_dir)
    pipeline.run([
        dlt.resource(departures, name="synthetic_departures", write_disposition="merge",
                     primary_key=["departure_key", "station_id", "poll_slot_utc"]),
        dlt.resource(weather, name="synthetic_weather_hourly", write_disposition="merge", primary_key="hour_utc"),
        dlt.resource(quarantine, name="quarantine_events", write_disposition="merge", primary_key="quarantine_id"),
    ])
    return {"departures_loaded": len(departures), "weather_hours": len(weather),
            "quarantined": len(quarantine), "duplicates_dropped": duplicates}


def main() -> int:
    args = build_arg_parser("Generate the labelled 14-day synthetic practice dataset.").parse_args()
    print(f"Synthetic history loaded: {run(args.target, args.run_date)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
