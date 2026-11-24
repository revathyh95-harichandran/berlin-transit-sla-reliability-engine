"""Tests for orchestration/health.py. All run_results dictionaries here are small and fake."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from faker import Faker

from orchestration import health
from orchestration.tests.conftest import make_model_result, make_run_results, make_test_result

FIXED_NOW = datetime(2026, 11, 24, 21, 30, 0, tzinfo=timezone.utc)
ALL_GOOD_STEPS = {"dlt_ingest": "success", "dbt_run": "success", "dbt_test": "success"}


def _green_results(fake: Faker) -> dict:
    return make_run_results(
        [
            make_model_result(fake.word()),
            make_test_result(f"not_null_{fake.word()}", "pass"),
            make_test_result(f"unique_{fake.word()}", "pass"),
        ]
    )


def test_count_ignores_model_results_and_counts_each_status(fake: Faker) -> None:
    results = make_run_results(
        [
            make_model_result("stg_departures"),
            make_test_result("a", "pass"),
            make_test_result("b", "warn"),
            make_test_result("c", "fail"),
            make_test_result("d", "error"),
            make_test_result("e", "skipped"),
            make_test_result(fake.word(), "pass"),
        ]
    )

    counts = health.count_test_results(results)

    assert counts == {"total": 6, "passed": 2, "warned": 1, "failed": 2, "skipped": 1, "other": 0}


def test_count_returns_zeros_when_dbt_never_ran() -> None:
    assert health.count_test_results(None)["total"] == 0


def test_all_green_run_gives_green_overall_and_dry_run_refresh(fake: Faker) -> None:
    report = health.build_health_report(_green_results(fake), ALL_GOOD_STEPS, "dry_run", now_utc=FIXED_NOW)

    assert report["overall_status"] == "GREEN"
    assert report["overall_reasons"] == []
    assert report["stages"]["midstream_dbt"]["status"] == "GREEN"
    assert report["stages"]["downstream_bi_refresh"]["status"] == "GREEN"
    assert report["stages"]["downstream_bi_refresh"]["triggered"] is False


def test_warned_test_turns_midstream_amber_and_overall_amber(fake: Faker) -> None:
    results = make_run_results([make_test_result(fake.word(), "warn"), make_test_result(fake.word(), "pass")])

    report = health.build_health_report(results, ALL_GOOD_STEPS, "triggered", now_utc=FIXED_NOW)

    assert report["stages"]["midstream_dbt"]["status"] == "AMBER"
    assert report["overall_status"] == "AMBER"
    assert any("midstream dbt" in reason for reason in report["overall_reasons"])


def test_failed_test_closes_circuit_breaker_and_blocks_refresh(fake: Faker) -> None:
    results = make_run_results([make_test_result(fake.word(), "fail")])
    steps = {"dlt_ingest": "success", "dbt_run": "success", "dbt_test": "failed"}

    report = health.build_health_report(results, steps, "blocked", now_utc=FIXED_NOW)

    assert report["overall_status"] == "RED"
    assert report["stages"]["midstream_dbt"]["status"] == "RED"
    assert "circuit breaker is closed" in report["stages"]["midstream_dbt"]["reason"]
    assert report["stages"]["downstream_bi_refresh"]["blocked"] is True
    assert report["stages"]["downstream_bi_refresh"]["triggered"] is False


def test_failed_ingestion_is_red_and_downstream_steps_show_upstream_failed() -> None:
    steps = {"dlt_ingest": "failed", "dbt_run": "upstream_failed", "dbt_test": "upstream_failed"}

    report = health.build_health_report(None, steps, "blocked", now_utc=FIXED_NOW)

    assert report["stages"]["upstream_ingestion"]["status"] == "RED"
    assert report["stages"]["midstream_dbt"]["status"] == "RED"
    assert report["overall_status"] == "RED"


def test_skipped_ingestion_is_amber_not_red(fake: Faker) -> None:
    steps = {"dlt_ingest": "skipped", "dbt_run": "success", "dbt_test": "success"}

    report = health.build_health_report(_green_results(fake), steps, "dry_run", now_utc=FIXED_NOW)

    assert report["stages"]["upstream_ingestion"]["status"] == "AMBER"
    assert report["overall_status"] == "AMBER"


def test_not_checked_stages_do_not_change_overall_status(fake: Faker) -> None:
    steps = {"dlt_ingest": "not_checked", "dbt_run": "success", "dbt_test": "success"}

    report = health.build_health_report(_green_results(fake), steps, "not_run", now_utc=FIXED_NOW)

    assert report["stages"]["upstream_ingestion"]["status"] == "NOT_CHECKED"
    assert report["stages"]["downstream_bi_refresh"]["status"] == "NOT_CHECKED"
    assert report["overall_status"] == "GREEN"


def test_unknown_refresh_outcome_is_recorded_as_pending() -> None:
    stage = health.downstream_refresh_stage("something-odd", {})

    assert stage["refresh_outcome"] == "pending"
    assert stage["status"] == "AMBER"


def test_timestamps_show_utc_and_berlin_wall_clock_time() -> None:
    report = health.build_health_report(None, {}, "not_run", now_utc=FIXED_NOW)

    assert report["generated_at_utc"] == "2026-11-24T21:30:00+00:00"
    # November is winter time in Berlin: UTC+1.
    assert report["generated_at_berlin"] == "2026-11-24T22:30:00+01:00"


def test_normalise_outcome_maps_github_and_airflow_words() -> None:
    assert health.normalise_outcome("success") == "success"
    assert health.normalise_outcome("failure") == "failed"
    assert health.normalise_outcome("cancelled") == "failed"
    assert health.normalise_outcome("upstream_failed") == "upstream_failed"
    assert health.normalise_outcome("") == "not_run"


def test_report_is_written_as_utf8_with_umlauts_kept(tmp_path, fake: Faker) -> None:
    results = make_run_results([make_test_result("bezirk_name", "pass")])
    report = health.build_health_report(results, ALL_GOOD_STEPS, "dry_run", now_utc=FIXED_NOW)
    report["note"] = "Tempelhof-Schöneberg"
    target = tmp_path / "logs" / "pipeline_health.json"

    health.write_health_report(report, target)

    raw = target.read_text(encoding="utf-8")
    assert "Tempelhof-Schöneberg" in raw
    assert json.loads(raw)["overall_status"] == "GREEN"


def test_main_reads_run_results_and_writes_json(tmp_path, fake: Faker) -> None:
    run_results_path = tmp_path / "run_results.json"
    run_results_path.write_text(json.dumps(_green_results(fake)), encoding="utf-8")
    output = tmp_path / "pipeline_health.json"

    exit_code = health.main(
        [
            "--run-results", str(run_results_path),
            "--output", str(output),
            "--step", "dlt_ingest=not_checked",
            "--step", "dbt_run=success",
            "--step", "dbt_test=success",
            "--refresh-outcome", "not_run",
        ]
    )

    assert exit_code == 0
    written = json.loads(output.read_text(encoding="utf-8"))
    assert written["overall_status"] == "GREEN"
    assert written["tests"]["passed"] == 2


def test_stale_run_results_are_ignored_when_tests_did_not_run(tmp_path, fake: Faker) -> None:
    # An old file that says "all passed" must not make a run look green if dbt test never ran.
    run_results_path = tmp_path / "run_results.json"
    run_results_path.write_text(json.dumps(_green_results(fake)), encoding="utf-8")
    output = tmp_path / "pipeline_health.json"

    health.main(
        [
            "--run-results", str(run_results_path),
            "--output", str(output),
            "--step", "dbt_run=failure",
            "--step", "dbt_test=upstream_failed",
        ]
    )

    written = json.loads(output.read_text(encoding="utf-8"))
    assert written["tests"]["total"] == 0
    assert written["overall_status"] == "RED"


def test_failed_dbt_build_without_test_results_says_run_failed() -> None:
    # Like the transform workflow: one "dbt build" outcome is reported as both run and test.
    steps = {"dlt_ingest": "not_checked", "dbt_run": "failed", "dbt_test": "upstream_failed"}

    report = health.build_health_report(None, steps, "not_run", now_utc=FIXED_NOW)

    assert report["stages"]["midstream_dbt"]["status"] == "RED"
    assert "dbt run failed" in report["stages"]["midstream_dbt"]["reason"]
    assert report["stages"]["downstream_bi_refresh"]["status"] == "NOT_CHECKED"
    assert report["overall_status"] == "RED"
