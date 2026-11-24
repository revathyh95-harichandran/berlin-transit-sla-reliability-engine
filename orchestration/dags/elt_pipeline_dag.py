"""Airflow DAG: the hourly ELT pipeline for the Berlin transit SLA reliability engine (see ADR-001).

The task order, in plain words:

    dlt_ingest  ->  dbt_run  ->  dbt_test  ->  write_health_telemetry  ->  trigger_powerbi_import_refresh
    (load RAW)     (build the    (CIRCUIT     (write the traffic-light   (ask Power BI to reload,
                    models)       BREAKER)     health file)                only when the tests passed)

The circuit breaker works like the fuse box in a flat. If a test fails, the fuse trips:
  * dbt_test is marked failed,
  * the refresh task is "upstream_failed" (Airflow never starts it), so Power BI keeps yesterday's
    verified numbers,
  * write_health_telemetry still runs, because its trigger rule is all_done. The failure is recorded.

Two rules that keep the design safe:
  * dbt_test has retries=0. Re-running a failing test just fails again and would hide the problem.
  * DRY_RUN is ON unless the environment (or the Airflow Variable DRY_RUN) says False.
    In dry-run mode the refresh is only logged, never sent.

This file needs Apache Airflow 2.x to run. The business logic lives in orchestration/*.py, which
has no Airflow import, so it is tested with plain pytest.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# The repository root. Airflow usually loads this file from orchestration/dags/, so the root is
# two folders up. PIPELINE_REPO_DIR overrides it when the DAG folder is somewhere else.
REPO_ROOT = Path(os.environ.get("PIPELINE_REPO_DIR", Path(__file__).resolve().parents[2]))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from orchestration import health, powerbi_refresh  # noqa: E402  (needs the sys.path line above)

log = logging.getLogger("airflow.task")

# Settings come from environment variables, so the same file works on a laptop and on a server.
TARGET = os.environ.get("PIPELINE_TARGET", "dev")  # dev = DuckDB, prod = Snowflake
PYTHON_BIN = os.environ.get("PIPELINE_PYTHON", sys.executable)  # the Python that has the project packages
DBT_BIN = os.environ.get("PIPELINE_DBT", "dbt")  # the dbt command (dbt-core and dbt-duckdb or dbt-snowflake)
DBT_DIR = REPO_ROOT / "dbt_project"
RUN_RESULTS_FILE = DBT_DIR / "target" / "run_results.json"
HEALTH_FILE = REPO_ROOT / "logs" / "pipeline_health.json"

# Task ids. The same names are used in the local runner and in the health report.
TASK_INGEST = health.STEP_INGEST
TASK_DBT_RUN = health.STEP_DBT_RUN
TASK_DBT_TEST = health.STEP_DBT_TEST
TASK_HEALTH = "write_health_telemetry"
TASK_REFRESH = health.STEP_REFRESH

INGEST_COMMAND = (
    # Run VBB first, then weather. A VBB failure must not stop the weather load (like the GitHub collector),
    # so the exit codes are saved and combined at the end: any failure makes the task fail.
    f'cd "{REPO_ROOT}" && export PYTHONUTF8=1 RUNTIME__DLTHUB_TELEMETRY=false && '
    f'"{PYTHON_BIN}" -m ingestion.vbb_departures --target {TARGET}; rc_vbb=$?; '
    f'"{PYTHON_BIN}" -m ingestion.weather --target {TARGET}; rc_weather=$?; '
    f'if [ "$rc_vbb" -ne 0 ] || [ "$rc_weather" -ne 0 ]; then exit 1; fi'
)

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,  # waits 5, then 10 minutes, and so on
    "max_retry_delay": timedelta(minutes=30),
}


def _task_outcome(context: dict, task_id: str) -> str:
    """Read the state of an earlier task in this run (for example "success" or "failed")."""
    task_instance = context["dag_run"].get_task_instance(task_id)
    if task_instance is None or task_instance.state is None:
        return "not_run"
    return str(task_instance.state)


def _collect_outcomes(context: dict) -> dict[str, str]:
    """The state of the three steps that come before the health task."""
    return {
        TASK_INGEST: _task_outcome(context, TASK_INGEST),
        TASK_DBT_RUN: _task_outcome(context, TASK_DBT_RUN),
        TASK_DBT_TEST: _task_outcome(context, TASK_DBT_TEST),
    }


def _write_health(steps: dict[str, str], refresh_outcome: str) -> dict:
    """Build the health report from the step states and write logs/pipeline_health.json."""
    run_results = None
    if steps[TASK_DBT_TEST] in ("success", "failed"):
        run_results = health.load_run_results(RUN_RESULTS_FILE)
    report = health.build_health_report(run_results, steps, refresh_outcome)
    health.write_health_report(report, HEALTH_FILE)
    log.info("Health report written to %s: overall %s", HEALTH_FILE, report["overall_status"])
    return report


def write_health_telemetry(**context: object) -> None:
    """Task 4: record the run in logs/pipeline_health.json. Runs even when earlier tasks failed.

    At this point Power BI has not been called yet. If the tests passed, the refresh is still to come,
    so the report says "pending". The next task overwrites it with the real outcome.
    """
    steps = _collect_outcomes(context)
    refresh_outcome = "pending" if steps[TASK_DBT_TEST] == "success" else "blocked"
    _write_health(steps, refresh_outcome)


def _powerbi_environment() -> dict[str, str]:
    """Environment for the Power BI call. DRY_RUN falls back to an Airflow Variable, then to True."""
    env = dict(os.environ)
    if "DRY_RUN" not in env:
        try:
            from airflow.models import Variable

            env["DRY_RUN"] = Variable.get("DRY_RUN", default_var="True")
        except Exception:  # noqa: BLE001  (no metadata database available: stay in the safe dry-run mode)
            env["DRY_RUN"] = "True"
    # In production the group id, dataset id and access token should come from an Airflow Connection or a
    # secrets backend. They are read from environment variables here and are never written into this file.
    return env


def trigger_powerbi_refresh(**context: object) -> None:
    """Task 5: ask Power BI for an import refresh. Only runs when dbt_test succeeded.

    The trigger rule already blocks the task when dbt_test failed. This check is a second lock on the door,
    in case the DAG is ever edited by mistake.
    """
    steps = _collect_outcomes(context)
    if steps[TASK_DBT_TEST] != "success":
        _write_health(steps, "blocked")
        raise AirflowSkipException("Circuit breaker closed: dbt tests did not pass, Power BI refresh blocked.")

    try:
        result = powerbi_refresh.trigger_refresh(_powerbi_environment(), tests_passed=True, log=log.info)
    except powerbi_refresh.PowerBIConfigError as error:
        _write_health(steps, "failed")
        raise AirflowException(str(error)) from error

    _write_health(steps, result["outcome"])


with DAG(
    dag_id="berlin_transit_elt_pipeline",
    description="Hourly: ingest VBB and weather, build dbt models, test, then refresh Power BI only if tests passed.",
    # Berlin time zone, so "hourly" and the start date match the clocks people see on the street.
    start_date=pendulum.datetime(2026, 11, 24, 0, 0, tz="Europe/Berlin"),
    schedule="@hourly",
    # No back-filling of missed hours: each run reads the latest data anyway.
    catchup=False,
    # Never run two pipelines at the same time, like one train per track section.
    max_active_runs=1,
    default_args=default_args,
    tags=["berlin-transit", "elt", "circuit-breaker"],
    doc_md=__doc__,
) as dag:

    # Task 1: load the RAW tables (VBB departures and Open-Meteo weather).
    dlt_ingest = BashOperator(
        task_id=TASK_INGEST,
        bash_command=INGEST_COMMAND,
    )

    # Task 2: build the Silver and Gold models. dbt uses the profiles.yml next to the project.
    dbt_run = BashOperator(
        task_id=TASK_DBT_RUN,
        bash_command=f'cd "{DBT_DIR}" && "{DBT_BIN}" run --target {TARGET} --profiles-dir .',
    )

    # Task 3: the circuit breaker. A failing test fails this task, which blocks everything after it.
    # retries=0: a failed test should stop the line, not be retried until it passes by chance.
    dbt_test = BashOperator(
        task_id=TASK_DBT_TEST,
        bash_command=f'cd "{DBT_DIR}" && "{DBT_BIN}" test --target {TARGET} --profiles-dir .',
        retries=0,
    )

    # Task 4: always runs (all_done), so a failed run still leaves a health record.
    write_health = PythonOperator(
        task_id=TASK_HEALTH,
        python_callable=write_health_telemetry,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Task 5: runs only when every task before it succeeded, including dbt_test (listed twice on purpose).
    trigger_refresh = PythonOperator(
        task_id=TASK_REFRESH,
        python_callable=trigger_powerbi_refresh,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        retries=1,
    )

    dlt_ingest >> dbt_run >> dbt_test >> write_health >> trigger_refresh
    dbt_test >> trigger_refresh
