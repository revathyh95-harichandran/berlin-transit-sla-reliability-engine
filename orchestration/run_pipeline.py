"""Local pipeline runner: the same steps as the Airflow DAG, run as plain Python on a laptop.

Airflow does not install on Windows, so this file does the same job in the same order:

    dlt_ingest -> dbt_run -> dbt_test -> write_health_telemetry -> trigger_powerbi_import_refresh

It keeps the same circuit breaker as the DAG. Think of a school timetable: if the chemistry
experiment (dbt_test) fails, the results are not copied onto the notice board (the Power BI refresh).
The health file is always written at the end, even when something failed earlier.

Examples (run from the repository root):
    python orchestration/run_pipeline.py --skip-ingest          # dbt + tests + health, no API calls
    python orchestration/run_pipeline.py --target dev           # full run, Power BI still in DRY_RUN
    DRY_RUN=False python orchestration/run_pipeline.py          # only if the Power BI secrets are set

Exit codes: 0 = pipeline finished and the refresh was not blocked or failed, 1 = something blocked or failed.
"""

from __future__ import annotations

import argparse
import os
import subprocess  # nosec B404 - only fixed command lists are run, never text typed by a user
import sys
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
# Make "import orchestration.health" work even when this file is started directly with python.
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from orchestration import health, powerbi_refresh  # noqa: E402  (needs the sys.path line above)

DBT_PROJECT_DIR = REPO_ROOT / "dbt_project"
RUN_RESULTS_FILE = DBT_PROJECT_DIR / "target" / "run_results.json"
HEALTH_FILE = REPO_ROOT / "logs" / "pipeline_health.json"

# The same step names as the DAG, so the health report looks identical in both places.
STEP_INGEST = health.STEP_INGEST
STEP_DBT_RUN = health.STEP_DBT_RUN
STEP_DBT_TEST = health.STEP_DBT_TEST


def find_tool(name: str) -> str:
    """Find a tool inside the project's virtual environment (.venv), if it exists.

    Like finding the right key on a key ring: prefer the project's own key, otherwise use the
    one that is already on the PATH.
    """
    folder = "Scripts" if os.name == "nt" else "bin"
    suffix = ".exe" if os.name == "nt" else ""
    candidate = REPO_ROOT / ".venv" / folder / f"{name}{suffix}"
    if candidate.exists():
        return str(candidate)
    if name == "python":
        return sys.executable
    return name


def log(message: str) -> None:
    """Print a message with a clock time, so the console reads like a station departure log."""
    stamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{stamp}] {message}", flush=True)


def child_environment(target: str) -> dict[str, str]:
    """Environment for the child programs: UTF-8 output and no telemetry calls from dlt."""
    env = dict(os.environ)
    env["PYTHONUTF8"] = "1"
    env["RUNTIME__DLTHUB_TELEMETRY"] = "false"
    env["PIPELINE_TARGET"] = target
    return env


def run_command(label: str, command: list[str], cwd: Path, env: dict[str, str]) -> bool:
    """Run one command and return True when it exits with code 0."""
    log(f"START {label}: {' '.join(command)}")
    # The command is a list (no shell), so nothing is interpreted by a shell. Bandit B603 is a false alarm here.
    completed = subprocess.run(command, cwd=cwd, env=env, check=False)  # nosec B603
    ok = completed.returncode == 0
    log(f"{'OK   ' if ok else 'FAIL '} {label} (exit code {completed.returncode})")
    return ok


def run_pipeline(target: str, skip_ingest: bool) -> int:
    """Run the steps in order with the circuit breaker, then write the health file."""
    env = child_environment(target)
    python = find_tool("python")
    dbt = find_tool("dbt")
    steps: dict[str, str] = {}

    # Step 1: ingestion. Weather still runs when VBB fails, like the GitHub collector does.
    if skip_ingest:
        steps[STEP_INGEST] = "skipped"
        log("SKIP dlt_ingest (--skip-ingest): RAW is not refreshed in this run.")
    else:
        vbb_ok = run_command(
            "dlt_ingest: VBB departures",
            [python, "-m", "ingestion.vbb_departures", "--target", target],
            REPO_ROOT, env,
        )
        weather_ok = run_command(
            "dlt_ingest: weather",
            [python, "-m", "ingestion.weather", "--target", target],
            REPO_ROOT, env,
        )
        steps[STEP_INGEST] = "success" if (vbb_ok and weather_ok) else "failed"

    # Step 2 and 3: dbt run, then dbt test. Like the DAG: if ingestion failed, these do not start.
    if steps[STEP_INGEST] == "failed":
        steps[STEP_DBT_RUN] = "upstream_failed"
        steps[STEP_DBT_TEST] = "upstream_failed"
        log("dbt_run and dbt_test skipped because ingestion failed (upstream_failed).")
    else:
        run_ok = run_command(
            "dbt_run",
            [dbt, "run", "--target", target, "--profiles-dir", "."],
            DBT_PROJECT_DIR, env,
        )
        steps[STEP_DBT_RUN] = "success" if run_ok else "failed"
        if run_ok:
            test_ok = run_command(
                "dbt_test (circuit breaker)",
                [dbt, "test", "--target", target, "--profiles-dir", "."],
                DBT_PROJECT_DIR, env,
            )
            steps[STEP_DBT_TEST] = "success" if test_ok else "failed"
        else:
            steps[STEP_DBT_TEST] = "upstream_failed"

    # Step 4: the circuit breaker. Only a fully passing dbt test allows the refresh.
    tests_passed = steps[STEP_DBT_TEST] == "success"

    # Step 5: Power BI import refresh. Dry run by default, so nothing is sent to Power BI.
    try:
        refresh_outcome = powerbi_refresh.trigger_refresh(env, tests_passed=tests_passed, log=log)["outcome"]
    except powerbi_refresh.PowerBIConfigError as error:
        log(f"Power BI refresh failed: {error}")
        refresh_outcome = "failed"

    # Step 6: health telemetry. Written even when earlier steps failed (like trigger_rule all_done).
    # Only read dbt's results when dbt test really ran in this run, so an old file is never used.
    run_results = None
    if steps[STEP_DBT_TEST] in ("success", "failed"):
        run_results = health.load_run_results(RUN_RESULTS_FILE)
    report = health.build_health_report(run_results, steps, refresh_outcome)
    health.write_health_report(report, HEALTH_FILE)
    log(f"Health report written: {HEALTH_FILE} -> overall {report['overall_status']}")
    for reason in report["overall_reasons"]:
        log(f"  - {reason}")

    failed = steps[STEP_INGEST] == "failed" or steps[STEP_DBT_RUN] == "failed" or steps[STEP_DBT_TEST] == "failed"
    return 1 if failed or refresh_outcome == "failed" else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the Berlin transit ELT steps locally, with the circuit breaker.")
    parser.add_argument("--target", default="dev", choices=["dev", "prod"], help="dbt and ingestion target (default dev)")
    parser.add_argument("--skip-ingest", action="store_true", help="do not call the VBB and weather APIs")
    args = parser.parse_args(argv)
    return run_pipeline(args.target, args.skip_ingest)


if __name__ == "__main__":
    raise SystemExit(main())
