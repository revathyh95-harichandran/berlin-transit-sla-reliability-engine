# ADR-002: 24/7 transit collection with GitHub Actions into Snowflake, rush hour defined from data

- **Status:** Accepted
- **Date:** 2026-11-24
- **Author:** Revathy Harichandran

## 1. Context: what problem am I solving?

Berlin transit operations lose commuter trust when weather and rush hours cause cascading delays across S-Bahn,
U-Bahn, Tram and Bus. Today nobody can answer three simple questions: **where** (which Bezirk and line),
**when** (which hours), and **why** (weather or everyday operations).

The data exists but is scattered:

| Source | What it gives | Limitation |
|--------|---------------|------------|
| VBB REST API (`v6.vbb.transport.rest`) | Live departures with planned time, predicted time, delay, line, mode, cancellations | Only the next minutes; **no history** |
| Open-Meteo | Hourly rain, snowfall, wind, temperature for Berlin | Hourly, city-wide |
| Berlin Geoportal WFS (`alkis_bezirke`) | Official boundaries of the 12 Bezirke | Static |

Because VBB has no history, I must **collect it myself** on a schedule.

## 2. Everyday analogy

It is like counting traffic for a city study. I can't rewind last month, so I put a volunteer with a clipboard at
24 street corners and ask them to write down every bus every 15 minutes, day and night. Afterwards I look at the
clipboards to see when the queues are longest, instead of guessing "it's probably 8 am".

## 3. Options I considered

| Option | How it works | Pros | Cons |
|--------|--------------|------|------|
| A. Scheduled task on my laptop → local DuckDB | Windows Task Scheduler runs the collector every 15 min | No cloud setup | Laptop must stay on and awake; gaps whenever it sleeps |
| B. Rush-hour-only polling in the cloud | Poll only at assumed peak times | Fewer runs | Assumes the answer; no off-peak baseline to compare against |
| **C. GitHub Actions every 15 min, 24/7 → Snowflake RAW** | A scheduled workflow polls VBB and loads straight into Snowflake | Runs while my laptop sleeps; full-day baseline; free compute on a public repo | Needs Snowflake from day one; GitHub may start scheduled runs late or skip some |
| D. Apache Airflow on a server | Airflow schedules the collector | Production-grade orchestration | Needs an always-on server, which is overkill for one small collector |

## 4. Decision

I chose **Option C** for collection and keep **Airflow (D)** as the production design for the full pipeline.
The Airflow DAG is written and validated in CI. GitHub Actions is the lightweight collector: the right tool for
the size of the job.

- **Schedule:** one cron, `*/15 * * * *` (every 15 minutes, every day, UTC).
- **Stations:** 24 stations, 2 per Bezirk, covering S-Bahn, U-Bahn, Tram and Bus.
- **Each poll** asks for departures in the next 15 minutes, so consecutive polls cover the timeline without gaps.
- **Login:** a Snowflake service user `SVC_INGEST` with key-pair authentication (no password, no MFA prompt).
  The private key and account identifier live only in GitHub Secrets.
- **Local development** and CI use DuckDB; Snowflake is the production target.

## 5. Definitions

| Term | Definition |
|------|-----------|
| **Departure event** | One planned departure of one trip from one stop (`tripId` + stop + planned time). |
| **Latest prediction wins** | A departure is seen in several polls; I keep the reading from the **latest** poll before departure, because it is closest to what really happened. |
| **SLA breach** | Departure delay **> 300 seconds (more than 5 minutes)**. |
| **Cancelled** | VBB marks the departure `cancelled = true`. Counted separately, not as a breach. |
| **No realtime** | `delay` is empty (no live data). Counted, but excluded from the breach rate. |
| **Breach rate** | Breaches ÷ departures that have realtime data and are not cancelled. |
| **Weather condition** (per hour, first match wins) | Snow = snowfall > 0 cm · Rain = precipitation ≥ 0.5 mm · Windy = wind speed ≥ 30 km/h · Clear = everything else. |
| **Peak (rush) hour** | Computed from data, separately for **weekdays** and **weekends**: an hour of the day is a peak hour when its average number of scheduled departures is in the **top 25 %** of hours for that day type. |
| **Day type** | `is_weekend` (Saturday/Sunday) and `is_public_holiday` (Berlin public holidays from a fixed reference list). |
| **Empty poll** | A poll that succeeds but returns 0 departures (normal at night). Logged as `ok_empty`, not as an error. |
| **Coverage** | Successful polls ÷ expected polls (96 per station per day). It shows late or skipped scheduled runs. |
| **Synthetic data** | A generated 14-day practice dataset, flagged `is_synthetic = true` and always reported separately from real data. |

## 6. Trade-offs

- **Gain:** a continuous, unbiased record where rush hour is measured rather than assumed, collected without my
  laptop.
- **Give up:**
  - **A sample, not a census:** 24 stations, not every stop.
  - **Predictions, not certified actual times.**
  - **Short history:** the sprint ends Thursday, so the first real weekend data arrives on 28–29 November.
    Until then, weekend comparisons use the labelled synthetic set.

## 7. Cost & GDPR impact

- **Cost:**
  - 96 runs a day wake the XSMALL warehouse for about a minute each, roughly **1.5–2 credits per day** of trial credit.
  - A daily resource monitor (`TRANSIT_DAILY_CAP`) suspends the warehouse if usage goes above the daily budget.
  - GitHub Actions is free for public repositories. Out-of-pocket cost: **€0**.
- **GDPR / DSGVO:**
  - The data has **no personal data**: timetables, delays and weather, with no passengers or staff.
  - Trip and vehicle IDs are still SHA-256 hashed with a salt in Silver, and masked for `BI_READER_ROLE`.
  - All storage stays in the EU: Snowflake AWS eu-central-1 (Frankfurt), and DuckDB locally.

## 8. Consequences & follow-ups

- **`raw.poll_runs`:** every poll writes one row per station (`polled_at_utc`, status, number of departures),
  so gaps are visible.
- **Default branch:** GitHub only runs scheduled workflows from the default branch, so the workflow is enabled
  after merging to `main`.
- **Monday 30 November:** re-run the weekend comparison on real weekend data.
- **After the trial:** if collection continues past the Snowflake trial, revisit the 15-minute frequency, or
  batch several polls per warehouse wake-up.
