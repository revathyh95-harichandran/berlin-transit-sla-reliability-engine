/*
    mart_data_quality (Gold mart)

    Grain: one row per Berlin date x data source (is_synthetic).

    This is the "data reliability" scoreboard behind the Before vs After story: did the collector
    show up on time, how many rows were good, and how many were rejected and why.
    Like a school attendance sheet: expected 96 check-ins per station per day, how many arrived?

    Real polls: expected = 96 polls per station per day (every 15 minutes), times the number of
    stations seen in the logbook. coverage_pct = (ok + ok_empty) / expected, so a skipped or late
    scheduled run shows up as a gap. An empty poll (ok_empty) is a normal quiet night, not an error.
    Synthetic data has no poll logbook, so its poll columns stay empty; it still reports
    quarantine counts and the number of departures that reached Silver.
    is_partial_day marks today (Berlin), whose coverage is still growing.
*/

with station_count as (

    select count(distinct station_id) as stations
    from {{ ref('stg_poll_runs') }}

),

poll_summary as (

    select
        poll_date_berlin as date_day,
        is_synthetic,
        count(*) as polls_logged,
        sum(case when status = 'ok' then 1 else 0 end) as polls_ok,
        sum(case when status = 'ok_empty' then 1 else 0 end) as polls_ok_empty,
        sum(case when status = 'error' then 1 else 0 end) as polls_error,
        sum(departures_received) as rows_received,
        sum(departures_valid) as rows_valid,
        sum(duplicates_dropped) as duplicates_dropped
    from {{ ref('stg_poll_runs') }}
    group by poll_date_berlin, is_synthetic

),

quarantine_summary as (

    select
        poll_date_berlin as date_day,
        is_synthetic,
        count(*) as rows_quarantined,
        sum(case when reason_category = 'missing_planned_time' then 1 else 0 end) as quarantined_missing_planned_time,
        sum(case when reason_category = 'implausible_delay' then 1 else 0 end) as quarantined_implausible_delay,
        sum(case when reason_category = 'out_of_scope_product' then 1 else 0 end) as quarantined_out_of_scope_product,
        sum(case when reason_category = 'bad_coordinates' then 1 else 0 end) as quarantined_bad_coordinates,
        sum(case when reason_category = 'not_an_object' then 1 else 0 end) as quarantined_not_an_object,
        sum(case when reason_category = 'weather_reading' then 1 else 0 end) as quarantined_weather_reading,
        sum(case when reason_category = 'other' then 1 else 0 end) as quarantined_other
    from {{ ref('stg_quarantine_events') }}
    group by poll_date_berlin, is_synthetic

),

silver_summary as (

    -- Departures that survived cleaning and de-duplication, by the Berlin day they were first polled.
    select
        polled_date_berlin as date_day,
        is_synthetic,
        count(*) as departures_in_silver
    from {{ ref('stg_departures') }}
    group by polled_date_berlin, is_synthetic

),

all_days as (

    select
        date_day,
        is_synthetic
    from poll_summary
    union
    select
        date_day,
        is_synthetic
    from quarantine_summary
    union
    select
        date_day,
        is_synthetic
    from silver_summary

)

select
    {{ surrogate_key(['all_days.date_day', 'all_days.is_synthetic']) }} as quality_key,
    all_days.date_day as date,
    all_days.is_synthetic,
    all_days.date_day >= cast({{ to_berlin_time('current_timestamp') }} as date) as is_partial_day,

    -- Polls: expected versus logged versus outcome (real data only)
    case when not all_days.is_synthetic then station_count.stations * 96 end as polls_expected,
    polls.polls_logged,
    polls.polls_ok,
    polls.polls_ok_empty,
    polls.polls_error,
    case
        when not all_days.is_synthetic and station_count.stations > 0
            then round(
                100.0 * (coalesce(polls.polls_ok, 0) + coalesce(polls.polls_ok_empty, 0))
                / (station_count.stations * 96), 1
            )
    end as coverage_pct,

    -- Rows: received, valid, rejected, de-duplicated
    polls.rows_received,
    polls.rows_valid,
    coalesce(quarantine.rows_quarantined, 0) as rows_quarantined,
    polls.duplicates_dropped,
    coalesce(silver.departures_in_silver, 0) as departures_in_silver,

    -- Rejected rows by simplified reason
    coalesce(quarantine.quarantined_missing_planned_time, 0) as quarantined_missing_planned_time,
    coalesce(quarantine.quarantined_implausible_delay, 0) as quarantined_implausible_delay,
    coalesce(quarantine.quarantined_out_of_scope_product, 0) as quarantined_out_of_scope_product,
    coalesce(quarantine.quarantined_bad_coordinates, 0) as quarantined_bad_coordinates,
    coalesce(quarantine.quarantined_not_an_object, 0) as quarantined_not_an_object,
    coalesce(quarantine.quarantined_weather_reading, 0) as quarantined_weather_reading,
    coalesce(quarantine.quarantined_other, 0) as quarantined_other

from all_days
cross join station_count
left join poll_summary as polls
    on
        all_days.date_day = polls.date_day
        and all_days.is_synthetic = polls.is_synthetic
left join quarantine_summary as quarantine
    on
        all_days.date_day = quarantine.date_day
        and all_days.is_synthetic = quarantine.is_synthetic
left join silver_summary as silver
    on
        all_days.date_day = silver.date_day
        and all_days.is_synthetic = silver.is_synthetic
