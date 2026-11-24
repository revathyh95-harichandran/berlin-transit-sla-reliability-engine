"""Shared helpers used by every collector.

What lives here:
  * where the data is loaded (local DuckDB for development, Snowflake for production)
  * date and time helpers (Berlin time, 15-minute poll slots)
  * a small hashing helper for building stable row keys
  * the 14-day rolling retention for the local database
"""

from __future__ import annotations

import argparse
import hashlib
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import dlt

# Berlin local time zone (switches between CET and CEST automatically).
BERLIN_TZ = ZoneInfo("Europe/Berlin")

# All RAW tables go into one dataset (a schema in the database).
DATASET_NAME = "transit"

# Local development database file (ignored by git, never committed).
LOCAL_DB_PATH = Path("data") / "berlin_transit.duckdb"

# Keep only the last 14 days locally so the file stays small (< 150 MB).
RETENTION_DAYS = 14

# Fixed Snowflake objects created by infra/rbac_masking.sql.
SNOWFLAKE_DATABASE = "RAW"
SNOWFLAKE_WAREHOUSE = "TRANSIT_WH"
SNOWFLAKE_ROLE = "INGEST_ROLE"
SNOWFLAKE_DEFAULT_USER = "SVC_INGEST"


# -----------------------------------------------------------------------------
# Command-line arguments shared by every collector
# -----------------------------------------------------------------------------
def build_arg_parser(description: str) -> argparse.ArgumentParser:
    """Create the standard command-line options: --target and --run-date."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--target",
        choices=["dev", "prod"],
        default="dev",
        help="dev = local DuckDB file, prod = Snowflake (needs SNOWFLAKE_* environment variables)",
    )
    parser.add_argument(
        "--run-date",
        type=parse_run_date,
        default=None,
        help="Logical date to load, format YYYY-MM-DD (default: today in Berlin)",
    )
    return parser


def parse_run_date(value: str) -> date:
    """Turn '2026-11-24' into a date object, with a clear error message if the format is wrong."""
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"--run-date must look like YYYY-MM-DD, got '{value}'") from error


def today_in_berlin() -> date:
    """The current calendar date in Berlin (not UTC)."""
    return datetime.now(BERLIN_TZ).date()


# -----------------------------------------------------------------------------
# Time helpers
# -----------------------------------------------------------------------------
def utc_now() -> datetime:
    """Current time in UTC, always time-zone aware."""
    return datetime.now(timezone.utc)


def to_poll_slot(moment: datetime) -> datetime:
    """Round a time DOWN to its 15-minute slot, in UTC.

    Example: 08:07 and 08:14 both belong to the 08:00 slot.
    Two runs in the same slot get the same slot value, so re-running a poll
    overwrites the earlier rows instead of creating duplicates (idempotency:
    pressing the lift button twice still calls the lift once).
    """
    moment_utc = moment.astimezone(timezone.utc)
    return moment_utc.replace(minute=moment_utc.minute - moment_utc.minute % 15, second=0, microsecond=0)


# -----------------------------------------------------------------------------
# Keys
# -----------------------------------------------------------------------------
def make_key(*parts: object) -> str:
    """Build a stable SHA-256 key from several values.

    The same inputs always give the same key, so it works like a fingerprint
    for a row. This is a technical key, not privacy hashing (that happens in
    the Silver layer with a secret salt).
    """
    joined = "|".join("" if part is None else str(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


# -----------------------------------------------------------------------------
# Destinations
# -----------------------------------------------------------------------------
def snowflake_credentials_from_env() -> dict[str, str]:
    """Read the Snowflake login for the service user from environment variables.

    In GitHub Actions these come from repository secrets. The private key is
    never written to disk or printed.
    """
    account = os.environ.get("SNOWFLAKE_ACCOUNT", "").strip()
    private_key = os.environ.get("SNOWFLAKE_PRIVATE_KEY", "").strip()
    if not account or not private_key:
        raise RuntimeError(
            "Missing Snowflake settings: set SNOWFLAKE_ACCOUNT and SNOWFLAKE_PRIVATE_KEY "
            "(GitHub repository secrets) to use --target prod."
        )
    return {
        "host": account,
        "username": os.environ.get("SNOWFLAKE_USER", SNOWFLAKE_DEFAULT_USER),
        "database": SNOWFLAKE_DATABASE,
        "warehouse": SNOWFLAKE_WAREHOUSE,
        "role": SNOWFLAKE_ROLE,
        "private_key": private_key,
    }


def build_pipeline(
    target: str,
    pipeline_name: str,
    local_db_path: Path | None = None,
    pipelines_dir: Path | None = None,
) -> dlt.Pipeline:
    """Create a dlt pipeline that loads into DuckDB (dev) or Snowflake (prod).

    A pipeline is like a delivery route: it knows where the parcels (rows)
    should go and keeps a small notebook (state) of what it already delivered.
    """
    if target == "dev":
        db_path = local_db_path or LOCAL_DB_PATH
        db_path.parent.mkdir(parents=True, exist_ok=True)
        destination = dlt.destinations.duckdb(credentials=str(db_path))
    elif target == "prod":
        destination = dlt.destinations.snowflake(
            credentials=snowflake_credentials_from_env(),
            # Delete the temporary upload files after each load to save storage.
            keep_staged_files=False,
        )
    else:
        raise ValueError(f"Unknown target '{target}', expected 'dev' or 'prod'")

    return dlt.pipeline(
        pipeline_name=f"{pipeline_name}_{target}",
        destination=destination,
        dataset_name=DATASET_NAME,
        pipelines_dir=str(pipelines_dir) if pipelines_dir else None,
    )


def apply_local_retention(pipeline: dlt.Pipeline, table_name: str, timestamp_column: str) -> int:
    """Delete rows older than 14 days from a local DuckDB table.

    Only used for the dev target. Production keeps the full history.
    Returns the number of deleted rows.
    """
    cutoff = utc_now() - timedelta(days=RETENTION_DAYS)
    # Table and column names come from code constants (never user input) and the
    # cutoff date is passed as a query parameter, so this SQL is safe from injection.
    with pipeline.sql_client() as client:
        qualified_table = client.make_qualified_table_name(table_name)
        rows = client.execute_sql(
            f"SELECT COUNT(*) FROM {qualified_table} WHERE {timestamp_column} < %s", cutoff  # nosec B608
        )
        to_delete = rows[0][0] if rows else 0
        if to_delete:
            client.execute_sql(f"DELETE FROM {qualified_table} WHERE {timestamp_column} < %s", cutoff)  # nosec B608
    return int(to_delete)
