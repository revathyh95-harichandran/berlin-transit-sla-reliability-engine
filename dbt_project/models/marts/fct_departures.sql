{{
    config(
        materialized='incremental',
        unique_key='departure_sk',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

/*
    fct_departures (Gold fact table)

    Grain: ONE row per departure event: one planned departure of one trip from one stop,
    seen from one polled station (departure_key + station_id), per data source (is_synthetic).

    Think of it as the café receipt roll: every departure is one receipt line, and the
    measures (delay, breach, cancelled) are the numbers printed on it. The dimension tables
    (date, line, stop, Bezirk, weather, hour profile) are the menu that explains the codes.

    How it loads (incremental MERGE on departure_sk):
      * First run or --full-refresh: every departure is built.
      * Normal run: only readings polled in the last `lookback_hours` (default 3) before the newest
        poll already stored are re-processed. A departure is predicted again in each new poll,
        and the MERGE overwrites the old row, so the latest prediction always wins.
      * Backfill: with --vars '{run_date: 2026-11-20}' the whole Berlin calendar day is rebuilt.
        Re-running any of this never creates duplicates, because departure_sk is the merge key.

    Join safety: each dimension is joined on its unique key (tested), so joins cannot
    multiply rows. A singular test checks that this table holds exactly as many rows as stg_departures.
*/

with departures as (

    select * from {{ ref('stg_departures') }}

),

{% if is_incremental() %}
    newest_poll_per_source as (  -- noqa: ST03

        -- The newest poll already stored, separately for real and synthetic data.
        select
            is_synthetic,
            max(last_polled_at_utc) as newest_polled_at_utc
        from {{ this }}
        group by is_synthetic

    ),
{% endif %}

rows_to_process as (

    select departures.*
    from departures
    {% if is_incremental() %}
        {% if var('run_date', none) is not none %}
            -- Backfill: rebuild exactly one Berlin calendar day.
            where departures.planned_date_berlin = cast('{{ var("run_date") }}' as date)
        {% else %}
            left join newest_poll_per_source
                on departures.is_synthetic = newest_poll_per_source.is_synthetic
            -- Normal run: re-read the last few hours of polls (or everything for a source not stored yet).
            where
                newest_poll_per_source.newest_polled_at_utc is null
                or departures.polled_at_utc >= {{ add_hours('newest_poll_per_source.newest_polled_at_utc', '-' ~ var('lookback_hours')) }}
        {% endif %}
    {% endif %}

)

select
    rows_to_process.departure_sk,
    rows_to_process.departure_key,
    rows_to_process.station_id,

    -- Foreign keys to the dimensions
    calendar.date_key,
    {{ surrogate_key(['rows_to_process.line_name', 'rows_to_process.product']) }} as line_key,
    {{ surrogate_key(['rows_to_process.station_id', 'rows_to_process.stop_id']) }} as stop_key,
    coalesce(stops.bezirk_key, '000') as bezirk_key,
    coalesce(weather.weather_hour_key, '-1') as weather_hour_key,
    {{ surrogate_key(['rows_to_process.is_synthetic', 'calendar.day_type', 'rows_to_process.planned_hour_berlin']) }}
        as hour_profile_key,

    -- Hashed trip identifiers (the raw IDs stop in Silver)
    rows_to_process.trip_id_hash,
    rows_to_process.vehicle_trip_number_hash,

    -- Times
    rows_to_process.planned_departure_utc,
    rows_to_process.planned_departure_berlin,
    rows_to_process.predicted_departure_berlin,
    rows_to_process.planned_hour_berlin as hour_of_day,
    rows_to_process.polled_at_utc as last_polled_at_utc,

    -- Measures
    rows_to_process.delay_seconds,
    round(rows_to_process.delay_seconds / 60.0, 2) as delay_minutes,
    rows_to_process.is_cancelled,
    rows_to_process.has_realtime,
    rows_to_process.is_sla_breach,
    rows_to_process.is_synthetic

from rows_to_process
left join {{ ref('dim_date') }} as calendar
    on rows_to_process.planned_date_berlin = calendar.date
left join {{ ref('dim_stop') }} as stops
    on {{ surrogate_key(['rows_to_process.station_id', 'rows_to_process.stop_id']) }} = stops.stop_key
left join {{ ref('stg_weather_hourly') }} as weather
    on
        date_trunc('hour', rows_to_process.planned_departure_utc) = weather.hour_utc
        and rows_to_process.is_synthetic = weather.is_synthetic
