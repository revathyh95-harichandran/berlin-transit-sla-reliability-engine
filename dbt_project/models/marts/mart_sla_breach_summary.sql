{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='date_key',
        on_schema_change='append_new_columns'
    )
}}

/*
    mart_sla_breach_summary (Gold mart)

    Grain: one row per Berlin date x hour of day x Bezirk x line x weather condition x is_synthetic.

    This is the table the dashboard reads. It answers "where, when and why": where (Bezirk, line),
    when (date, hour, peak or not, weekday or weekend) and why (weather condition).
    Think of it as a tally sheet: each row is one box on the sheet with its counts.

    Counting rules (ADR-002), every departure lands in exactly ONE of these three buckets:
      * cancelled_departures   : VBB marked it cancelled. Not a breach.
      * no_realtime_departures : not cancelled but no delay value. Excluded from the breach rate.
      * rated_departures       : not cancelled and has a delay value. This is the breach-rate denominator.
    breach_rate = breach_departures / rated_departures (empty when nothing was rated).
    Real and synthetic data are never mixed: is_synthetic is part of the grain.

    How it loads (incremental, delete+insert by date_key): a normal run finds the Berlin dates that have
    departures polled in the last `lookback_hours`, deletes those whole days from this table and rebuilds
    them from the fact. Older days are not touched, like re-adding only this week's till receipts instead of
    the whole year. --vars '{run_date: ...}' rebuilds that one day; --full-refresh rebuilds everything.
    Known trade-off: peak-hour flags of old days are only refreshed on a full refresh.
*/

with newest_poll as (  -- noqa: ST03

    -- Newest poll stored per data source; only used by incremental runs.
    select
        is_synthetic,
        max(last_polled_at_utc) as newest_polled_at_utc
    from {{ ref('fct_departures') }}
    group by is_synthetic

),

affected_dates as (  -- noqa: ST03

    -- Berlin dates touched by recent polls (or the single backfill day); only used by incremental runs.
    {% if var('run_date', none) is not none %}
        select cast(replace('{{ var("run_date") }}', '-', '') as integer) as date_key
    {% else %}
        select distinct recent.date_key
        from {{ ref('fct_departures') }} as recent
        inner join newest_poll
            on recent.is_synthetic = newest_poll.is_synthetic
        where recent.last_polled_at_utc >= {{ add_hours('newest_poll.newest_polled_at_utc', '-' ~ var('lookback_hours')) }}
    {% endif %}

),

departures as (

    select
        fact.date_key,
        fact.hour_of_day,
        fact.bezirk_key,
        fact.line_key,
        fact.weather_hour_key,
        fact.hour_profile_key,
        fact.is_cancelled,
        fact.has_realtime,
        fact.is_sla_breach,
        fact.delay_minutes,
        fact.is_synthetic
    from {{ ref('fct_departures') }} as fact
    {% if is_incremental() %}
        inner join affected_dates
            on fact.date_key = affected_dates.date_key
    {% endif %}

)

select
    {{ surrogate_key([
        'calendar.date_key', 'departures.hour_of_day', 'departures.bezirk_key',
        'departures.line_key', "coalesce(weather.weather_condition, 'unknown')", 'departures.is_synthetic'
    ]) }} as summary_key,
    calendar.date,
    calendar.date_key,
    departures.hour_of_day,
    bezirke.bezirk_name,
    lines.line_name,
    lines.mode_label,
    coalesce(weather.weather_condition, 'unknown') as weather_condition,
    departures.is_synthetic,
    calendar.day_type,
    coalesce(profile.is_peak_hour, false) as is_peak_hour,

    count(*) as scheduled_departures,
    sum(case when departures.is_cancelled then 1 else 0 end) as cancelled_departures,
    sum(case when not departures.is_cancelled and not departures.has_realtime then 1 else 0 end)
        as no_realtime_departures,
    sum(case when not departures.is_cancelled and departures.has_realtime then 1 else 0 end)
        as rated_departures,
    sum(case when departures.is_sla_breach then 1 else 0 end) as breach_departures,
    sum(case when departures.is_sla_breach then 1 else 0 end) * 1.0
    / nullif(sum(case when not departures.is_cancelled and departures.has_realtime then 1 else 0 end), 0)
        as breach_rate,
    avg(case when not departures.is_cancelled then departures.delay_minutes end) as avg_delay_minutes

from departures
left join {{ ref('dim_date') }} as calendar
    on departures.date_key = calendar.date_key
left join {{ ref('dim_line') }} as lines
    on departures.line_key = lines.line_key
left join {{ ref('dim_bezirk') }} as bezirke
    on departures.bezirk_key = bezirke.bezirk_key
left join {{ ref('dim_weather_hour') }} as weather
    on departures.weather_hour_key = weather.weather_hour_key
left join {{ ref('dim_hour_profile') }} as profile
    on departures.hour_profile_key = profile.hour_profile_key
group by
    calendar.date,
    calendar.date_key,
    departures.hour_of_day,
    departures.bezirk_key,
    bezirke.bezirk_name,
    departures.line_key,
    lines.line_name,
    lines.mode_label,
    coalesce(weather.weather_condition, 'unknown'),
    departures.is_synthetic,
    calendar.day_type,
    coalesce(profile.is_peak_hour, false)
