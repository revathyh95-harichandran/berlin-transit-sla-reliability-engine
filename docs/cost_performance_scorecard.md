# Cost and Performance Scorecard

- **Date:** 2026-11-24
- **Author:** Revathy Harichandran
- **Scope:** Snowflake warehouse cost, dbt query plans, incremental loads, local DuckDB and Git storage.

## 1. Why this matters, in plain English

Snowflake charges for the time the warehouse is switched on, like a taxi meter that runs while the car
waits at the curb. Every 15 minutes the collector wakes the warehouse, and every hour dbt does too. The
warehouse then stays on for 60 seconds after the last query, even when nothing is happening (AUTO_SUSPEND = 60).
So a short job can cost a full minute of meter time. This scorecard measures where the meter runs.

Query plans are the route map a query takes through the data. A full table scan is reading every page of a
book to find one sentence. An incremental load is reading only the new pages.

## 2. Scorecard

| # | Check | Result | Evidence |
|---|-------|--------|----------|
| 1 | Warehouse is XSMALL | PASS | `infra/rbac_masking.sql`, Step 1 (config checked in code, not yet in Snowsight) |
| 2 | AUTO_SUSPEND = 60 seconds | PASS | Same file; AUTO_RESUME and INITIALLY_SUSPENDED are set |
| 3 | Daily resource monitor (4 credits) | WARN | Monitor exists. Estimated use is 3.2 to 5.3 credits per day, so the high case hits the cap (section 4) |
| 4 | Fact table `fct_departures` is incremental (merge) | PASS | Plan shows 2,834 rows merged on an incremental run, against 190,934 on a full build (1.5 %) |
| 5 | Incremental run does not re-read all raw data | WARN | The staging view still deduplicates all 190,934 raw rows on every run; the lookback filter is applied after that step |
| 6 | Other heavy models are incremental | WARN | `mart_sla_breach_summary` rebuilds 29,555 rows from scratch every hour |
| 7 | No `SELECT *` in Gold models | WARN | 2 in marts (`fct_departures`, `mart_sla_breach_summary`), 3 in `stg_departures`, 3 in `stg_weather_hourly` (mostly pass-through, low impact) |
| 8 | No fan-out or Cartesian joins in the fact | PASS | Row count stays at 2,834 through every dimension join; dimensions are unique-key tested |
| 9 | Spatial join (point-in-polygon) is cheap | PASS | 48 stops against 12 districts in about 10 ms |
| 10 | Local DuckDB below 150 MB | WARN | 107.8 MB now (72 %). Full 14-day local collection is projected at 150 to 250 MB |
| 11 | Git repository below 5 MB | PASS | 154 KiB of loose objects; `.git` folder 545 KB |
| 12 | CI cost is 0 EUR | WARN | Free for a public repo. About 3,000 to 4,000 Actions minutes a month for collect.yml would exceed the 2,000-minute free quota if the repo is private |
| 13 | Snowflake storage cost | PASS | About 8 to 16 MB of new RAW data per day (compressed estimate); well under 1 EUR per year |

Summary: **0 FAIL, 6 WARN, 7 PASS.** The warehouse settings are right. The problems are how often the
warehouse wakes up and how much work each wake does.

## 3. Benchmark: ad-hoc question against RAW versus the Gold mart

Question: *SLA breach rate by Bezirk, weather and hour, for weekday peak hours.*
Both queries return the same 145 rows and the same totals (12,287 breaches out of 62,388 rated departures).
Five timed runs each, after one warm-up run. Local DuckDB, read-only.

| Approach | Median ms | Min ms | Max ms | Rows read by scans |
|----------|----------:|-------:|-------:|-------------------:|
| Ad-hoc SQL from RAW (dedup, map match, peaks all rebuilt) | 113.1 | 93.0 | 122.5 | 955,131 |
| Gold mart `mart_sla_breach_summary` | 17.2 | 16.5 | 17.4 | 6,074 |
| **Speed-up** | **6.6 times** | | | **157 times fewer rows** |

Everyday picture: the ad-hoc query is cooking from raw ingredients for every guest. The mart is the same
dish, cooked once and kept warm. On Snowflake, where each second costs credits, the gap in cost is larger
than the gap in time shown here. Raw numbers are in `finops/benchmark_results.json`.

## 4. Snowflake credit estimate (ESTIMATE, not measured)

Assumptions, to verify in Snowsight under **Admin > Cost Management**:
- The trial gives $400 of credit.
- Enterprise edition on AWS eu-central-1 (Frankfurt) costs about $3 to $4 per credit.
- XSMALL uses 1 credit per hour. Billing is per second with a 60-second minimum per resume.
- Each wake is billed for its active time plus the 60-second auto-suspend tail.

| Job | Wakes per day | Billed seconds per wake (low to high) | Credits per day (low to high) |
|-----|--------------:|--------------------------------------:|------------------------------:|
| collect.yml (VBB + weather, MERGE into RAW) | 96 | 75 to 120 | 2.0 to 3.2 |
| transform.yml (`dbt build --target prod`) | 24 | 180 to 300 | 1.2 to 2.0 |
| Power BI Import refresh (once a day, not configured yet) | 1 | 60 to 180 | 0.02 to 0.05 |
| **Total** | | | **3.2 to 5.3** |

- Against the 4-credit daily cap: the low case is at 80 % (the notify trigger). The high case hits 100 %
  and the monitor suspends the warehouse, so collection and transform stop until the next day.
- Trial length: $400 at $3 to $4 per credit is 100 to 133 credits. At 3.2 credits a day that lasts 31 to 42 days.
  With the cap binding at 4 credits a day it lasts 25 to 33 days. The standard trial is 30 days, so that
  limit is likely the one that binds.
- About 1.6 of the collect credits are pure idle tail (96 x 60 seconds). That is the cheapest thing to change.

### Cheapest safe schedules (estimate)

| Option | Credits per day (low to high) | Trade-off |
|--------|------------------------------:|-----------|
| A. Today (collect 15 min, transform hourly) | 3.2 to 5.3 | Over the cap at the high end |
| B. Transform every 3 hours (`20 */3 * * *`) | 2.4 to 4.0 | Dashboard data up to 3 hours old. Tight at the top end |
| C. B plus the staging fix (section 6, item 1) | 2.0 to 3.2 | Same freshness; transform active time roughly halved |
| D. Collect every 30 minutes, transform every 3 hours | 1.4 to 2.3 | Needs a 30-minute departure window (change ADR-002) |

**Recommendation:** option C now. It keeps 15-minute collection (ADR-002) and cuts the largest cost.

## 5. Storage guardrails

- **Local DuckDB:** 107.8 MB today. Most of it is the 14-day synthetic practice set plus a duplicate copy in the
  `transit_staging` schema (about 190,000 rows). Local retention runs in `ingestion/common.py`, but deleted
  rows do not shrink the file until a checkpoint runs.
- **Snowflake RAW growth:** about 87,700 VBB rows a day (38 rows per station poll, 24 stations, 96 polls), so
  roughly 2.6 million rows a month. About 754 bytes per row as JSON, which is roughly 66 MB a day before
  compression. Compressed, the estimate is 8 to 16 MB a day, or 0.25 to 0.5 GB a month.

## 6. Recommendations (highest value first)

1. **Stop re-reading all of RAW on every run** (owner: `analytics-eng`). Materialize `stg_departures` and
   `stg_weather_hourly` as incremental tables, or push the `polled_at_utc >= watermark - lookback` filter
   into the raw read before the de-duplication window. It is safe because the newest reading for any
   departure is always inside the window. The staging view is referenced six times across five models,
   so each hourly run scans RAW about six times today.
2. **Cut transform frequency to every 3 hours** (owner: `qa-devops`, file `transform.yml`). Change the
   cron to `20 */3 * * *`. This saves about 0.8 to 1.3 credits a day.
3. **Make `mart_sla_breach_summary` incremental** (owner: `analytics-eng`). Use `delete+insert` by
   `date_key`, so only recent days are rebuilt. Replace the two `SELECT *` lines with explicit columns
   (owner: `analytics-eng`).
4. **Shrink the local file after each load** (owner: `ingestion-eng`). Run `CHECKPOINT` after retention
   deletes, and drop the duplicate `transit_staging` schema once the load succeeds.
5. **Confirm repo visibility and Actions quota** (owner: `architect`). Free unlimited minutes apply only
   to public repos. Check this on GitHub before relying on the 0 EUR CI claim.
6. **Keep the Power BI refresh to once a day** (owner: `bi-semantic`). Import only the Gold mart tables, not
   the 190,000-row fact.

## 7. How to measure real usage

Run the queries in `finops/snowflake_usage_queries.sql` in Snowsight after a week of runs. Replace the
estimates in section 4 with the real numbers, and update this scorecard.

## 8. How to reproduce the local numbers

- Benchmark: `.venv\Scripts\python.exe -X utf8 finops\benchmark_adhoc_vs_mart.py`
- Query plans: `dbt compile --target dev --profiles-dir .`, then profile the compiled SQL with DuckDB (read-only).
