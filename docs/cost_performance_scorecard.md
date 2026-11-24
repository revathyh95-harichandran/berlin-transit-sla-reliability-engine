# Cost and Performance Scorecard

- **Date:** 2026-11-24 (second review, after the staging and mart fixes in commit fd2edd5)
- **Author:** Revathy Harichandran
- **Scope:** Snowflake warehouse cost, dbt query plans, incremental loads, local DuckDB and Git storage.

## 1. Why this matters, in plain English

Snowflake charges for the time the warehouse is switched on, like a taxi meter that runs while the car
waits at the curb. Every 15 minutes the collector wakes the warehouse, and every 3 hours dbt does too. The
warehouse then stays on for 60 seconds after the last query, even when nothing is happening (AUTO_SUSPEND = 60).
So a short job can cost a full minute of meter time. This scorecard measures where the meter runs.

Query plans are the route map a query takes through the data. A full table scan is reading every page of a
book to find one sentence. An incremental load is reading only the new pages.

## 2. Scorecard

| # | Check | Result | Evidence |
|---|-------|--------|----------|
| 1 | Warehouse is XSMALL | PASS | `infra/rbac_masking.sql`, Step 1 (checked in code, not yet in Snowsight) |
| 2 | AUTO_SUSPEND = 60 seconds | PASS | Same file; AUTO_RESUME and INITIALLY_SUSPENDED are set |
| 3 | Daily resource monitor (4 credits) | WARN | Estimated use is 2.4 to 3.9 credits a day. The high case is 98 % of the cap, so there is almost no headroom (section 4) |
| 4 | Fact table `fct_departures` is incremental (merge) | PASS | Incremental run 0.17 s against 0.77 s for a full build (median of 3); row count unchanged at 190,934 |
| 5 | Incremental run does not re-read all raw data | PASS | `stg_departures` is now incremental with the poll-time filter before the de-duplication window. Incremental run 0.37 s against 1.03 s (median of 3) |
| 6 | Other heavy models are incremental | PASS | `mart_sla_breach_summary` is incremental (delete and insert by date). `dim_hour_profile` is still a full rebuild but reads only the small staging table |
| 7 | No `SELECT *` in models | PASS | Search of `dbt_project/models` finds no `select *` |
| 8 | No fan-out or Cartesian joins in the fact | PASS | Row count stays flat through every dimension join; dimensions are unique-key tested |
| 9 | Spatial join (point-in-polygon) is cheap | PASS | 48 stops against 12 districts in about 10 ms |
| 10 | Local DuckDB below 150 MB | PASS | 104.5 MB after rebuilding the file from scratch (it had grown to 172.5 MB because repeated full refreshes leave free space that a checkpoint does not hand back) |
| 11 | Git repository below 5 MB | PASS | 180 KiB of loose objects; `.git` folder 618 KB |
| 12 | CI cost is 0 EUR | PASS | The repository is public (confirmed through the GitHub API), so Actions minutes are free. If it ever goes private, the 3,000 to 4,000 minutes a month for collect.yml would exceed the 2,000-minute free quota |
| 13 | Snowflake storage cost | PASS | About 8 to 16 MB of new RAW data per day (compressed estimate); well under 1 EUR per year |

Summary: **0 FAIL, 1 WARN, 12 PASS** (13 checks). The only cloud risk left is the thin margin under the
4-credit cap.

## 3. Benchmark: ad-hoc question against RAW versus the Gold mart

Question: *SLA breach rate by Bezirk, weather and hour, for weekday peak hours.*
Both queries return the same 145 rows and the same totals. Five timed runs each, after one warm-up run.
Local DuckDB, read-only. Source: `finops/benchmark_results.json` (run 2026-11-24 21:47 UTC).

| Approach | Median ms | Min ms | Max ms | Rows read by scans |
|----------|----------:|-------:|-------:|-------------------:|
| Ad-hoc SQL from RAW (dedup, map match, peaks all rebuilt) | 149.5 | 143.9 | 243.3 | 955,131 |
| Gold mart `mart_sla_breach_summary` | 21.0 | 19.1 | 32.4 | 6,074 |
| **Speed-up** | **7.1 times** | | | **157 times fewer rows** |

Everyday picture: the ad-hoc query is cooking from raw ingredients for every guest. The mart is the same
dish, cooked once and kept warm. On Snowflake, where each second costs credits, the gap in cost is larger
than the gap in time shown here.

### Incremental versus full refresh (dbt model time, 3 runs each)

| Model | Full refresh median | Incremental median | Rows in the table after either run |
|-------|--------------------:|-------------------:|-----------------------------------:|
| `stg_departures` | 1.03 s (1.03, 1.03, 1.06) | 0.37 s (0.36, 0.41, 0.37) | 190,934 |
| `fct_departures` | 0.77 s (0.73, 0.77, 0.77) | 0.17 s (0.17, 0.18, 0.16) | 190,934 |
| Both together | 1.80 s | 0.54 s | |

Both models are about 3 times faster on an incremental run. Times are dbt model execution times on the
local DuckDB file; wall-clock time also includes dbt start-up (about 5 seconds).

## 4. Snowflake credit estimate (ESTIMATE, not measured)

Assumptions, to verify in Snowsight under **Admin > Cost Management**:
- The trial gives $400 of credit.
- Enterprise edition on AWS eu-central-1 (Frankfurt) costs about $3 to $4 per credit.
- XSMALL uses 1 credit per hour. Billing is per second with a 60-second minimum per resume.
- Each wake is billed for its active time plus the 60-second auto-suspend tail.
- The staging fix is not credited. Real savings may be lower than the estimate's upper bound.

| Job | Wakes per day | Billed seconds per wake (low to high) | Credits per day (low to high) |
|-----|--------------:|--------------------------------------:|------------------------------:|
| collect.yml (VBB + weather, MERGE into RAW) | 96 | 75 to 120 | 2.0 to 3.2 |
| transform.yml (`dbt build --target prod`, `20 */3 * * *`) | 8 | 180 to 300 | 0.4 to 0.7 |
| Power BI Import refresh (once a day, not configured yet) | 1 | 60 to 180 | 0.02 to 0.05 |
| **Total** | | | **2.4 to 3.9** |

- Against the 4-credit daily cap: the low case is at 60 %, and the high case is at 98 %. The monitor's
  notify trigger (80 %) would fire on heavy days. Nothing is suspended under these estimates.
- Trial length: $400 at $3 to $4 per credit is 100 to 133 credits. At 2.4 to 3.9 credits a day that lasts
  26 to 55 days. The standard trial is 30 days, so that limit is likely the one that binds.
- Before the change (transform hourly, 24 runs a day) the estimate was 3.2 to 5.3 credits a day, which
  broke the cap at the high end.
- About 1.6 of the collect credits are pure idle tail (96 x 60 seconds). Collecting every 30 minutes would
  halve that, but it needs a 30-minute departure window (ADR-002 change).

## 5. Storage guardrails

- **Local DuckDB:** 172.5 MB measured in this review, which is 115 % of the 150 MB guard. It was 81 MB at
  the start of the day and 107.8 MB after the first round of timings. Each full refresh drops and rebuilds
  a table, and DuckDB keeps the freed space in the file until a checkpoint runs. The data itself is smaller.
  Fix: one `CHECKPOINT` on the dev file (owner: `ingestion-eng`), then re-measure. The duplicate
  `transit_staging` schema (about 190,000 synthetic rows) can also be dropped once the load succeeds.
- **Snowflake RAW growth:** about 87,700 VBB rows a day (38 rows per station poll, 24 stations, 96 polls), so
  roughly 2.6 million rows a month. About 754 bytes per row as JSON, which is roughly 66 MB a day before
  compression. Compressed, the estimate is 8 to 16 MB a day, or 0.25 to 0.5 GB a month.

## 6. Recommendations (status after the fixes)

1. **Stop re-reading all of RAW on every run.** Done in fd2edd5: `stg_departures` is incremental with the
   poll-time filter applied before de-duplication (owner: `analytics-eng`).
2. **Cut transform frequency to every 3 hours.** Done in fd2edd5: `transform.yml` runs `20 */3 * * *`
   (owner: `qa-devops`).
3. **Make `mart_sla_breach_summary` incremental.** Done in fd2edd5 (owner: `analytics-eng`).
4. **Shrink the local DuckDB file (done).** A `CHECKPOINT` did not reduce the file, so I deleted it and
   rebuilt it from the collectors and dbt: 104.5 MB. If it grows past 150 MB again, rebuilding is the fix,
   because the local file only holds reproducible data.
5. **Confirm repo visibility and the Actions quota (done).** The repository is public, so Actions minutes
   are free. Re-check this if the repository is ever made private.
6. **Keep the Power BI refresh to once a day (open, for when it is configured).** Import only the Gold mart
   tables, not the 190,000-row fact (owner: `bi-semantic`).
7. **Keep 3 hours or raise the cap margin (open).** At 98 % of the cap in the high case, one extra retry
   could trip the monitor. Either keep the 3-hour cadence with a check on real usage, or ask for a cap of 5
   credits in `infra/rbac_masking.sql` (owner: `architect`).

## 7. How to measure real usage

Run the queries in `finops/snowflake_usage_queries.sql` in Snowsight after a week of runs. Replace the
estimates in section 4 with the real numbers, and update this scorecard.

## 8. How to reproduce the local numbers

- Benchmark: `.venv\Scripts\python.exe -X utf8 finops\benchmark_adhoc_vs_mart.py`
- Model timings: `dbt run -s stg_departures fct_departures --full-refresh` and then without the flag, from `dbt_project/`.
- Query plans: `dbt compile --target dev --profiles-dir .`, then profile the compiled SQL with DuckDB (read-only).
