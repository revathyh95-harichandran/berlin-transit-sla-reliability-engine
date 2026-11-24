"""
Before vs after speed test: the same business question answered two ways.

  1. AD-HOC: a hand-written SQL that reads the raw RAW tables and does all the
     cleaning, de-duplication and map matching again on every request.
  2. MART: a short SQL that reads the pre-built Gold table (mart_sla_breach_summary).

Everyday picture: asking a friend to re-count every bus ticket from the shoebox each
time you want a total, versus reading the total from the summary sheet they already
filled in. Both give the same answer; the summary sheet is much faster.

What this script does:
  * opens the local DuckDB file in READ-ONLY mode (it can never change the data),
  * runs one warm-up pass per query (not counted), then 5 timed runs,
  * counts the rows each query scans (from DuckDB's own profiler),
  * checks that both queries return the same numbers,
  * prints a table and writes finops/benchmark_results.json (UTF-8).

How to run (from the repository root):
    .venv\\Scripts\\python.exe -X utf8 finops\\benchmark_adhoc_vs_mart.py
"""

import json
import statistics
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

# ---------------------------------------------------------------------------
# Paths: the script lives in finops/, so the repository root is one level up.
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = REPO_ROOT / "data" / "berlin_transit.duckdb"
SQL_DIR = REPO_ROOT / "finops" / "benchmarks"
RESULTS_PATH = REPO_ROOT / "finops" / "benchmark_results.json"

ADHOC_SQL_FILE = SQL_DIR / "adhoc_sla_weekday_peaks_from_raw.sql"
MART_SQL_FILE = SQL_DIR / "mart_sla_weekday_peaks.sql"

TIMED_RUNS = 5  # the number of measured runs per query
WARM_UP_RUNS = 1  # one run first so that file caches are warm; not counted

# The first six columns are the labels and counts; the seventh is the breach rate.
LABEL_COLUMN_COUNT = 6


def load_sql(path):
    # Read the query text as UTF-8, so German district names survive.
    with open(path, encoding="utf-8") as handle:
        return handle.read()


def time_query(connection, sql):
    # Run the query several times and record how long each run took, in milliseconds.
    timings_ms = []
    for _ in range(WARM_UP_RUNS):
        connection.execute(sql).fetchall()
    for _ in range(TIMED_RUNS):
        start = time.perf_counter()
        connection.execute(sql).fetchall()
        timings_ms.append((time.perf_counter() - start) * 1000)
    return timings_ms


def walk_profile(node, found):
    # The profiler gives a tree of steps. This visits every step and keeps the scan steps.
    name = str(node.get("operator_name", "")).strip().upper()
    if "SCAN" in name:
        found.append(int(node.get("operator_cardinality", 0)))
    for child in node.get("children", []):
        walk_profile(child, found)


def count_rows_scanned(connection, sql):
    # Ask DuckDB's profiler how many rows its scan steps read, like counting
    # the pages a reader actually opened. Profiling is done in a separate run,
    # so it does not slow down the timed runs.
    with tempfile.TemporaryDirectory() as temp_dir:
        profile_file = Path(temp_dir) / "profile.json"
        connection.execute("PRAGMA enable_profiling='json'")
        connection.execute(f"PRAGMA profiling_output='{profile_file.as_posix()}'")
        connection.execute(sql).fetchall()
        connection.execute("PRAGMA disable_profiling")
        with open(profile_file, encoding="utf-8") as handle:
            profile = json.load(handle)
    scanned_counts = []
    walk_profile(profile, scanned_counts)
    return sum(scanned_counts)


def results_match(rows_a, rows_b):
    # Both answers must have the same rows and the same numbers (rate compared with a tiny tolerance).
    if len(rows_a) != len(rows_b):
        return False
    for row_a, row_b in zip(rows_a, rows_b):
        if tuple(row_a[:LABEL_COLUMN_COUNT]) != tuple(row_b[:LABEL_COLUMN_COUNT]):
            return False
        rate_a, rate_b = row_a[LABEL_COLUMN_COUNT], row_b[LABEL_COLUMN_COUNT]
        if rate_a is None or rate_b is None:
            if rate_a is not rate_b:
                return False
        elif abs(rate_a - rate_b) > 1e-9:
            return False
    return True


def benchmark_one(connection, label, sql):
    # Measure one query: timings, rows scanned and the number of result rows.
    print(f"Running {label} ...")
    timings_ms = time_query(connection, sql)
    rows_scanned = count_rows_scanned(connection, sql)
    result_rows = connection.execute(sql).fetchall()
    return {
        "median_ms": round(statistics.median(timings_ms), 1),
        "min_ms": round(min(timings_ms), 1),
        "max_ms": round(max(timings_ms), 1),
        "timed_runs_ms": [round(value, 1) for value in timings_ms],
        "rows_scanned": rows_scanned,
        "result_rows": len(result_rows),
    }, result_rows


def print_table(adhoc, mart, speedup):
    # A small plain-text table that is easy to paste into a report.
    print()
    print(f"{'Query':<34}{'Median ms':>12}{'Min ms':>10}{'Max ms':>10}{'Rows scanned':>16}{'Result rows':>14}")
    print("-" * 96)
    print(
        f"{'Ad-hoc from RAW':<34}{adhoc['median_ms']:>12}{adhoc['min_ms']:>10}"
        f"{adhoc['max_ms']:>10}{adhoc['rows_scanned']:>16,}{adhoc['result_rows']:>14}"
    )
    print(
        f"{'Gold mart (mart_sla_breach_summary)':<34}{mart['median_ms']:>12}{mart['min_ms']:>10}"
        f"{mart['max_ms']:>10}{mart['rows_scanned']:>16,}{mart['result_rows']:>14}"
    )
    print("-" * 96)
    print(f"Speed-up (median ad-hoc / median mart): {speedup:.1f} times faster with the mart")


def main():
    # Print a short header so the run is easy to recognise in the console.
    print("Berlin transit: ad-hoc SQL on RAW versus the Gold mart")
    print(f"Database file (opened read-only): {DB_PATH}")

    connection = duckdb.connect(str(DB_PATH), read_only=True)
    connection.execute("LOAD spatial")

    adhoc_sql = load_sql(ADHOC_SQL_FILE)
    mart_sql = load_sql(MART_SQL_FILE)

    adhoc_stats, adhoc_rows = benchmark_one(connection, "ad-hoc SQL on RAW", adhoc_sql)
    mart_stats, mart_rows = benchmark_one(connection, "Gold mart SQL", mart_sql)
    connection.close()

    matches = results_match(adhoc_rows, mart_rows)
    speedup = adhoc_stats["median_ms"] / mart_stats["median_ms"] if mart_stats["median_ms"] else float("nan")

    print_table(adhoc_stats, mart_stats, speedup)
    print()
    print("Both answers match row for row:", "YES" if matches else "NO - check the queries")

    # Save the evidence, with a timestamp, so the numbers can be traced later.
    output = {
        "run_timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "database_file": "data/berlin_transit.duckdb",
        "duckdb_version": duckdb.__version__,
        "question": "SLA breach rate by Bezirk, weather and hour, weekday peak hours",
        "timed_runs_per_query": TIMED_RUNS,
        "warm_up_runs_per_query": WARM_UP_RUNS,
        "adhoc_from_raw": {"sql_file": "finops/benchmarks/adhoc_sla_weekday_peaks_from_raw.sql", **adhoc_stats},
        "gold_mart": {"sql_file": "finops/benchmarks/mart_sla_weekday_peaks.sql", **mart_stats},
        "speedup_median": round(speedup, 2),
        "results_match": matches,
        "note": "Local DuckDB numbers. Snowflake timings and credits differ and are estimated in docs/cost_performance_scorecard.md.",
    }
    with open(RESULTS_PATH, "w", encoding="utf-8") as handle:
        json.dump(output, handle, indent=2, ensure_ascii=False)
    print(f"Results saved to: {RESULTS_PATH}")


if __name__ == "__main__":
    main()
