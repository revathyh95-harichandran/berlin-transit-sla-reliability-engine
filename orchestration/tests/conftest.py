"""Shared test helpers for the orchestration tests.

Fake dbt results are built here, like a set of pre-printed practice test papers: no real dbt run is needed.
"""

from __future__ import annotations

from typing import Any

import pytest
from faker import Faker


@pytest.fixture
def fake() -> Faker:
    """A German-locale Faker with a fixed seed, so every run uses the same made-up names."""
    instance = Faker("de_DE")
    instance.seed_instance(2026)
    return instance


def make_test_result(name: str, status: str) -> dict[str, Any]:
    """One fake dbt test result, shaped like an entry in run_results.json."""
    return {"unique_id": f"test.berlin_transit.{name}", "status": status, "failures": 0 if status == "pass" else 1}


def make_model_result(name: str, status: str = "success") -> dict[str, Any]:
    """One fake dbt model result. Model results must never be counted as tests."""
    return {"unique_id": f"model.berlin_transit.{name}", "status": status}


def make_run_results(results: list[dict[str, Any]], generated_at: str = "2026-11-24T21:13:15.000000Z") -> dict[str, Any]:
    """Wrap fake results in the same top-level shape that dbt writes."""
    return {"metadata": {"dbt_schema_version": "fake", "generated_at": generated_at}, "results": results}
