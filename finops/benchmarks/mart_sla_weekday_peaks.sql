-- =============================================================================
-- Same question as adhoc_sla_weekday_peaks_from_raw.sql, answered from the Gold mart.
--   "What is the SLA breach rate by Bezirk, weather and hour, on weekday peak hours?"
--
-- The mart already holds the cooked dish: one row per date x hour x Bezirk x line x
-- weather x source, with the counts ready. This query only adds up the rows.
-- Dialect: DuckDB. Source: berlin_transit.dev_marts.mart_sla_breach_summary (local dev build).
-- =============================================================================

select
    bezirk_name,
    weather_condition,
    hour_of_day,
    is_synthetic,
    sum(breach_departures) as breach_departures,
    sum(rated_departures) as rated_departures,
    sum(breach_departures) * 1.0 / nullif(sum(rated_departures), 0) as breach_rate
from berlin_transit.dev_marts.mart_sla_breach_summary
where
    day_type = 'weekday'
    and is_peak_hour
group by
    bezirk_name,
    weather_condition,
    hour_of_day,
    is_synthetic
order by
    bezirk_name,
    weather_condition,
    hour_of_day,
    is_synthetic
