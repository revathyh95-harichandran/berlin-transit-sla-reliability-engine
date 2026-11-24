# ADR-001: Power BI Import Mode, refreshed only after dbt tests pass

- **Status:** Accepted
- **Date:** 2026-11-24
- **Author:** Revathy Harichandran

## 1. Context — what problem am I solving?

My dashboards read from the Gold (PROD) layer in Snowflake. Power BI can connect to it in two ways:

- **DirectQuery** — every click on a visual sends a live query to Snowflake.
- **Import Mode** — Power BI copies the data into its own compressed in-memory model on a schedule, and visuals
  read from that copy.

I needed to decide which one fits a portfolio-scale, low-budget, GDPR-aware pipeline where data quality is
checked by dbt tests before anyone sees it.

## 2. Everyday analogy

Think of a café menu board.

- **DirectQuery** is like the waiter running to the kitchen every time a guest asks "what is the soup today?".
  Always current, but slow, and the kitchen gets interrupted all day.
- **Import Mode** is like writing the menu on the board each morning, *after* the chef has tasted every dish.
  Guests read the board instantly, and nothing goes on the board that the chef has not approved.

In my pipeline, the "chef tasting the dishes" is `dbt test`.

## 3. Options I considered

| Option | How it works | Pros | Cons |
|--------|--------------|------|------|
| A. DirectQuery | Live SQL to Snowflake per visual | Always the latest data | Wakes the warehouse on every click (credits); slower visuals; users may see half-loaded or untested data; DAX limitations |
| B. Import Mode, fixed schedule | Power BI refreshes at a fixed time | Fast visuals, cheap | Refresh can run while the pipeline is mid-load or after tests failed |
| **C. Import Mode, triggered after dbt tests pass** | Airflow calls the Power BI REST API only when `dbt test` succeeds | Fast visuals, cheap, and only tested data reaches the report | Data is as fresh as the last successful run, not real-time |

## 4. Decision

I chose **Option C**. My Airflow DAG runs `dlt_ingest → dbt_run → dbt_test → write_health_telemetry →
trigger_powerbi_import_refresh`. The refresh task only runs if `dbt_test` succeeds. If a test fails, the DAG stops
there (a circuit breaker, like a home fuse box), and the dashboard keeps showing yesterday's verified numbers
instead of today's broken ones.

## 5. Trade-offs

- **Gain:** instant visuals, full DAX support, predictable cost, and a guarantee that every number on the report
  passed quality checks.
- **Give up:** real-time freshness. For daily transit and weather KPIs, a refresh after each pipeline run is
  more than enough. I prefer "slightly stale but correct" over "fresh but possibly wrong".

## 6. Cost & GDPR impact

- **Cost:** Snowflake is only queried once per refresh instead of on every click. With an XSMALL warehouse and
  `AUTO_SUSPEND = 60`, the warehouse wakes up briefly, serves the refresh, and goes back to sleep. Locally I test
  the trigger with `DRY_RUN=True`, so development costs €0.
- **GDPR / DSGVO:** the import only reads from the PROD layer, where personal identifiers are already SHA-256
  hashed in Silver and sensitive columns are masked for `BI_READER_ROLE`. Data stays in the EU
  (Snowflake AWS eu-central-1, Frankfurt). Row-Level Security in the Power BI model further limits who sees which
  district.

## 7. Consequences & follow-ups

- The Airflow DAG owns the refresh trigger; no fixed schedule is configured in the Power BI Service.
- `logs/pipeline_health.json` records whether the last refresh was triggered or blocked.
- If a future project truly needs near-real-time data, I will revisit this with a new ADR (e.g. a small
  DirectQuery or composite model only for that one table).
