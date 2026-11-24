/*
    dim_hour_profile (Gold dimension)

    Grain: one row per data source (is_synthetic) x day type x hour of day (0-23).

    Rush hour is measured, not assumed (ADR-002). Like counting cars past a window every hour:
      1. For each day type, count scheduled departures per hour on every day I have data for.
         Hours with no departures count as 0, so quiet night hours are not skipped.
      2. Average those counts per hour (avg_scheduled_departures).
      3. Rank the 24 hours with PERCENT_RANK (0 = quietest hour, 1 = busiest hour).
      4. An hour is a peak hour when its percent rank is in the top share of hours
         (var peak_hour_top_share = 0.25, so percent rank >= 0.75, which gives 6 of 24 hours).
         Hours that tie share the same rank, so a tie at the border can add an extra peak hour.

    Built from stg_departures (not from the fact table) so the fact can point at this dimension
    without a circular dependency.
*/

with hours as (

    select 0 as hour_of_day
    union all
    select 1
    union all
    select 2
    union all
    select 3
    union all
    select 4
    union all
    select 5
    union all
    select 6
    union all
    select 7
    union all
    select 8
    union all
    select 9
    union all
    select 10
    union all
    select 11
    union all
    select 12
    union all
    select 13
    union all
    select 14
    union all
    select 15
    union all
    select 16
    union all
    select 17
    union all
    select 18
    union all
    select 19
    union all
    select 20
    union all
    select 21
    union all
    select 22
    union all
    select 23

),

days_with_data as (

    -- Every Berlin day on which a data source has any departures, with its day type.
    select distinct
        departures.is_synthetic,
        departures.planned_date_berlin,
        calendar.day_type
    from {{ ref('stg_departures') }} as departures
    inner join {{ ref('dim_date') }} as calendar
        on departures.planned_date_berlin = calendar.date
    where departures.planned_date_berlin is not null

),

departures_per_day_hour as (

    select
        is_synthetic,
        planned_date_berlin,
        planned_hour_berlin as hour_of_day,
        count(*) as scheduled_departures
    from {{ ref('stg_departures') }}
    where planned_date_berlin is not null
    group by is_synthetic, planned_date_berlin, planned_hour_berlin

),

every_day_every_hour as (

    -- All 24 hours for every day, filling missing hours with 0 departures.
    select
        days_with_data.is_synthetic,
        days_with_data.day_type,
        hours.hour_of_day,
        coalesce(counts.scheduled_departures, 0) as scheduled_departures
    from days_with_data
    cross join hours
    left join departures_per_day_hour as counts
        on
            days_with_data.is_synthetic = counts.is_synthetic
            and days_with_data.planned_date_berlin = counts.planned_date_berlin
            and hours.hour_of_day = counts.hour_of_day

),

hourly_average as (

    select
        is_synthetic,
        day_type,
        hour_of_day,
        avg(scheduled_departures) as avg_scheduled_departures
    from every_day_every_hour
    group by is_synthetic, day_type, hour_of_day

),

ranked as (

    select
        is_synthetic,
        day_type,
        hour_of_day,
        avg_scheduled_departures,
        percent_rank() over (
            partition by is_synthetic, day_type
            order by avg_scheduled_departures
        ) as busyness_percent_rank
    from hourly_average

)

select
    {{ surrogate_key(['is_synthetic', 'day_type', 'hour_of_day']) }} as hour_profile_key,
    is_synthetic,
    day_type,
    hour_of_day,
    avg_scheduled_departures,
    busyness_percent_rank,
    busyness_percent_rank >= (1 - {{ var('peak_hour_top_share') }}) as is_peak_hour
from ranked
