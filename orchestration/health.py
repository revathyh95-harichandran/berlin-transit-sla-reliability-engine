"""Pipeline health report: a traffic-light summary of one pipeline run.

Think of the traffic-light panel in a railway control room:
  * GREEN = every signal worked, trains run on time.
  * AMBER = the line runs, but something needs a look (a warning, or a step was skipped).
  * RED   = a signal failed, so the train is held at the platform (the circuit breaker is closed).

The report combines two sources:
  1. dbt_project/target/run_results.json, the test and model results dbt writes after every run.
  2. The outcome of each orchestration step (for example "dbt_test = failed"), which only the
     orchestrator (Airflow, the local runner or GitHub Actions) knows.

The result is written to logs/pipeline_health.json. That file is committed on purpose, so the
history of runs stays visible in git.

This module does NOT import Airflow, so it runs and is tested on any laptop.

Run it by hand, for example:
    python orchestration/health.py --step dbt_run=success --step dbt_test=success --refresh-outcome dry_run
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

# Repository root: this file lives in orchestration/, so the root is one folder up.
REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_RUN_RESULTS = REPO_ROOT / "dbt_project" / "target" / "run_results.json"
DEFAULT_OUTPUT = REPO_ROOT / "logs" / "pipeline_health.json"

BERLIN_TZ = ZoneInfo("Europe/Berlin")
SCHEMA_VERSION = 1

# Traffic-light colours. NOT_CHECKED means "this run did not look at that stage" (for example the
# collector runs in its own workflow). It never makes the overall status worse.
GREEN = "GREEN"
AMBER = "AMBER"
RED = "RED"
NOT_CHECKED = "NOT_CHECKED"
SEVERITY = {GREEN: 0, AMBER: 1, RED: 2}

# Step names, the same in Airflow, the local runner and the GitHub workflows.
STEP_INGEST = "dlt_ingest"
STEP_DBT_RUN = "dbt_run"
STEP_DBT_TEST = "dbt_test"
STEP_REFRESH = "trigger_powerbi_import_refresh"

# Refresh outcomes: what happened at the Power BI step.
REFRESH_OUTCOMES = ("triggered", "dry_run", "blocked", "failed", "skipped", "pending", "not_run")


def normalise_outcome(raw: str) -> str:
    """Map the different words used by Airflow and GitHub Actions onto one small set.

    Like translating "Fertig", "done" and "OK" into one tick mark on a checklist.
    """
    value = str(raw).strip().lower()
    if value in ("success", "succeeded", "ok", "pass", "passed"):
        return "success"
    if value in ("failure", "failed", "fail", "error", "cancelled", "canceled"):
        return "failed"
    if value in ("skipped", "skip"):
        return "skipped"
    if value in ("upstream_failed",):
        return "upstream_failed"
    if value in ("not_checked",):
        return "not_checked"
    return "not_run"


def count_test_results(run_results: Mapping[str, Any] | None) -> dict[str, int]:
    """Count dbt test results by outcome.

    dbt lists every node in run_results["results"]. Tests have ids that start with "test.",
    so models and seeds are skipped. Think of it as counting the ticks and crosses on a
    school test paper, ignoring the questions that were not marked.
    """
    counts = {"total": 0, "passed": 0, "warned": 0, "failed": 0, "skipped": 0, "other": 0}
    if not run_results:
        return counts

    for result in run_results.get("results", []):
        if not str(result.get("unique_id", "")).startswith("test."):
            continue
        counts["total"] += 1
        status = str(result.get("status", "")).strip().lower()
        if status == "pass":
            counts["passed"] += 1
        elif status == "warn":
            counts["warned"] += 1
        elif status in ("fail", "error", "runtime error"):
            counts["failed"] += 1
        elif status in ("skipped", "skip"):
            counts["skipped"] += 1
        else:
            counts["other"] += 1
    return counts


def _stage(status: str, reason: str, steps: Mapping[str, str], **extra: Any) -> dict[str, Any]:
    """Build one stage block for the JSON file (status, a plain-English reason, and its steps)."""
    block: dict[str, Any] = {"status": status, "reason": reason, "steps": dict(steps)}
    block.update(extra)
    return block


def upstream_ingestion_stage(steps: Mapping[str, str]) -> dict[str, Any]:
    """Stage 1: the collectors that load RAW (VBB departures and weather)."""
    outcome = normalise_outcome(steps.get(STEP_INGEST, "not_checked"))
    stage_steps = {STEP_INGEST: outcome}

    if outcome == "success":
        return _stage(GREEN, "Ingestion finished without errors.", stage_steps)
    if outcome == "failed":
        return _stage(RED, "Ingestion failed, so the data is not fresh.", stage_steps)
    if outcome == "skipped":
        return _stage(AMBER, "Ingestion was skipped in this run, so RAW was not refreshed.", stage_steps)
    if outcome == "not_checked":
        return _stage(NOT_CHECKED, "Ingestion runs in its own workflow and was not checked here.", stage_steps)
    return _stage(AMBER, "Ingestion outcome is unknown.", stage_steps)


def midstream_dbt_stage(steps: Mapping[str, str], tests: Mapping[str, int]) -> dict[str, Any]:
    """Stage 2: dbt run and dbt test. This is where the circuit breaker lives."""
    run_outcome = normalise_outcome(steps.get(STEP_DBT_RUN, "not_run"))
    test_outcome = normalise_outcome(steps.get(STEP_DBT_TEST, "not_run"))
    stage_steps = {STEP_DBT_RUN: run_outcome, STEP_DBT_TEST: test_outcome}

    # Check the failed tests first: they explain a failed build better than a generic message.
    if tests["failed"] > 0:
        return _stage(
            RED,
            f"{tests['failed']} dbt test(s) failed. The circuit breaker is closed, so promotion is blocked.",
            stage_steps,
            tests=dict(tests),
        )
    if test_outcome == "failed":
        return _stage(
            RED,
            "dbt tests stopped before reporting results. The circuit breaker is closed, so promotion is blocked.",
            stage_steps,
            tests=dict(tests),
        )
    if run_outcome == "failed":
        return _stage(RED, "dbt run failed, so the models may not be rebuilt.", stage_steps, tests=dict(tests))
    if run_outcome in ("upstream_failed", "not_run"):
        return _stage(RED, "dbt run did not start because an earlier step failed.", stage_steps, tests=dict(tests))
    if test_outcome in ("upstream_failed", "not_run", "skipped"):
        return _stage(RED, "dbt tests did not run, so the data was not verified.", stage_steps, tests=dict(tests))
    if tests["warned"] > 0:
        return _stage(AMBER, f"{tests['warned']} dbt test(s) warned. Data passed, but look at them.", stage_steps,
                      tests=dict(tests))
    return _stage(GREEN, "All dbt tests passed.", stage_steps, tests=dict(tests))


REFRESH_REASONS = {
    "triggered": (GREEN, "Power BI import refresh was triggered after the tests passed."),
    "dry_run": (GREEN, "Power BI refresh was only logged (DRY_RUN is on). Nothing was sent to Power BI."),
    "blocked": (RED, "Power BI refresh is blocked. The dashboard keeps the last verified numbers."),
    "failed": (RED, "Power BI refresh call failed."),
    "skipped": (AMBER, "Power BI refresh was skipped in this run."),
    "pending": (AMBER, "Refresh decision was not recorded yet."),
    "not_run": (NOT_CHECKED, "This run does not include the Power BI refresh step."),
}


def downstream_refresh_stage(refresh_outcome: str, steps: Mapping[str, str]) -> dict[str, Any]:
    """Stage 3: the Power BI import refresh (only runs after the tests passed)."""
    outcome = str(refresh_outcome).strip().lower()
    if outcome not in REFRESH_REASONS:
        outcome = "pending"
    status, reason = REFRESH_REASONS[outcome]
    return _stage(
        status,
        reason,
        {STEP_REFRESH: outcome},
        refresh_outcome=outcome,
        triggered=outcome == "triggered",
        blocked=outcome == "blocked",
    )


def overall_status(stages: Mapping[str, Mapping[str, Any]]) -> str:
    """The worst colour across the stages wins, like the strictest rule on a checklist.

    NOT_CHECKED stages are ignored, because they were not part of this run.
    """
    worst = GREEN
    for stage in stages.values():
        status = stage["status"]
        if status == NOT_CHECKED:
            continue
        if SEVERITY[status] > SEVERITY[worst]:
            worst = status
    return worst


def _format_timestamps(now_utc: datetime) -> dict[str, str]:
    """Return the same moment as UTC and as Berlin wall-clock time (with its offset)."""
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    return {
        "generated_at_utc": now_utc.astimezone(timezone.utc).isoformat(timespec="seconds"),
        "generated_at_berlin": now_utc.astimezone(BERLIN_TZ).isoformat(timespec="seconds"),
    }


def build_health_report(
    run_results: Mapping[str, Any] | None,
    steps: Mapping[str, str],
    refresh_outcome: str = "not_run",
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    """Combine dbt results and step outcomes into one report dictionary.

    Args:
        run_results: the parsed run_results.json, or None when dbt did not run.
        steps: outcome per step name, for example {"dbt_run": "success", "dbt_test": "failed"}.
        refresh_outcome: one of REFRESH_OUTCOMES.
        now_utc: the time to stamp on the report (defaults to the current time; tests pass a fixed time).
    """
    now = now_utc if now_utc is not None else datetime.now(timezone.utc)
    tests = count_test_results(run_results)

    stages = {
        "upstream_ingestion": upstream_ingestion_stage(steps),
        "midstream_dbt": midstream_dbt_stage(steps, tests),
        "downstream_bi_refresh": downstream_refresh_stage(refresh_outcome, steps),
    }
    status = overall_status(stages)

    reasons = [
        f"{name.replace('_', ' ')}: {stage['reason']}"
        for name, stage in stages.items()
        if stage["status"] in (AMBER, RED)
    ]

    generated_at = None
    if run_results and isinstance(run_results.get("metadata"), Mapping):
        generated_at = run_results["metadata"].get("generated_at")

    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "overall_status": status,
        "overall_reasons": reasons,
    }
    report.update(_format_timestamps(now))
    report["dbt_results_generated_at"] = generated_at
    report["stages"] = stages
    report["tests"] = tests
    return report


def load_run_results(path: Path) -> dict[str, Any] | None:
    """Read dbt's run_results.json. Returns None when the file is missing (dbt never ran)."""
    if not path.exists():
        return None
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def write_health_report(report: Mapping[str, Any], path: Path) -> Path:
    """Write the report as readable UTF-8 JSON (German letters such as 'Ö' stay as they are)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    return path


def _parse_steps(pairs: Sequence[str]) -> dict[str, str]:
    """Turn ["dbt_test=failed", ...] into {"dbt_test": "failed", ...}."""
    steps: dict[str, str] = {}
    for pair in pairs:
        if "=" not in pair:
            raise argparse.ArgumentTypeError(f"--step must look like NAME=OUTCOME, got '{pair}'")
        name, outcome = pair.split("=", 1)
        steps[name.strip()] = normalise_outcome(outcome)
    return steps


def main(argv: Sequence[str] | None = None) -> int:
    """Command-line entry point, used by the GitHub Actions transform workflow."""
    parser = argparse.ArgumentParser(description="Write logs/pipeline_health.json for one run.")
    parser.add_argument("--run-results", type=Path, default=DEFAULT_RUN_RESULTS)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--step", action="append", default=[], help="step outcome, e.g. dbt_test=success")
    parser.add_argument("--refresh-outcome", default="not_run", choices=REFRESH_OUTCOMES)
    args = parser.parse_args(argv)

    steps = _parse_steps(args.step)
    # Only read run_results when the test step really ran in this run, so a stale file from an
    # earlier run cannot make today's report look green.
    test_ran = steps.get(STEP_DBT_TEST) in ("success", "failed")
    run_results = load_run_results(args.run_results) if test_ran else None

    report = build_health_report(run_results, steps, args.refresh_outcome)
    write_health_report(report, args.output)
    print(f"Health: {report['overall_status']} -> {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
