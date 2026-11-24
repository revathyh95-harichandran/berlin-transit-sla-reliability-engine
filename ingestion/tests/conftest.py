"""Shared test fixtures.

All test data is generated with Faker or written by hand. No real API is called:
HTTP requests are answered by a fake transport, like a flight simulator for the network.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
import pytest
from faker import Faker

BERLIN_OFFSET = timezone(timedelta(hours=1))  # CET in November


@pytest.fixture
def fake() -> Faker:
    """A Faker instance with a fixed seed, so every test run sees the same 'random' data."""
    instance = Faker("de_DE")
    instance.seed_instance(1234)
    return instance


@pytest.fixture
def polled_at() -> datetime:
    """A fixed poll time: Tuesday 24 Nov 2026, 07:05 UTC (08:05 in Berlin)."""
    return datetime(2026, 11, 24, 7, 5, tzinfo=timezone.utc)


@pytest.fixture
def make_raw_departure(fake: Faker) -> Callable[..., dict[str, Any]]:
    """Factory that builds one VBB-shaped departure; pass keyword overrides to break it on purpose."""

    def _make(**overrides: Any) -> dict[str, Any]:
        planned = datetime(2026, 11, 24, 8, fake.random_int(10, 40), tzinfo=BERLIN_OFFSET)
        delay = fake.random_int(0, 900)
        record: dict[str, Any] = {
            "tripId": f"1|{fake.random_int(10000, 99999)}|0|86|24112026",
            "stop": {
                "id": "900100003",
                "name": "S+U Alexanderplatz Bhf (Berlin)",
                "location": {"latitude": 52.521508, "longitude": 13.411267},
            },
            "plannedWhen": planned.isoformat(),
            "when": (planned + timedelta(seconds=delay)).isoformat(),
            "delay": delay,
            "direction": fake.street_name(),
            "prognosisType": "prognosed",
            "line": {
                "id": "de-vbb-11000000-tram-m5",
                "name": "M5",
                "product": "tram",
                "fahrtNr": str(fake.random_int(10000, 99999)),
                "operator": {"name": "Berliner Verkehrsbetriebe"},
            },
        }
        record.update(overrides)
        return record

    return _make


@pytest.fixture
def tmp_db(tmp_path: Path) -> Path:
    """A throwaway DuckDB file path for one test."""
    return tmp_path / "test_transit.duckdb"


@pytest.fixture
def pipelines_dir(tmp_path: Path) -> Path:
    """A throwaway folder for the loader's state, so tests never touch the real one."""
    return tmp_path / "pipelines"


def make_mock_client(handler: Callable[[httpx.Request], httpx.Response], base_url: str = "https://test") -> httpx.Client:
    """An httpx client whose requests are answered by `handler` instead of the internet."""
    return httpx.Client(base_url=base_url, transport=httpx.MockTransport(handler))


def json_response(payload: Any, status: int = 200) -> httpx.Response:
    return httpx.Response(status, content=json.dumps(payload).encode("utf-8"),
                          headers={"Content-Type": "application/json"})
