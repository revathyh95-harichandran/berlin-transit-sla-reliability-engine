-- Every full Berlin day should have 24 weather hours per source (real and synthetic).
-- Skipped on purpose:
--   * the first day of each source (it may start in the middle of the day, like the first day of a diary)
--   * today and later days (the day is still happening, like a shop that is not yet closed for the evening)
--   * the two DST change days (late March has 23 hours, late October has 25 hours)
-- Warn only: a missing hour usually means the Open-Meteo call was late, which is live data, not broken code.
{{ config(severity='warn') }}

with hours_per_day as (

    select
        is_synthetic,
        date_berlin,
        count(*) as hour_count
    from {{ ref('stg_weather_hourly') }}
    group by is_synthetic, date_berlin

),

first_day_per_source as (

    select
        is_synthetic,
        min(date_berlin) as first_day
    from {{ ref('stg_weather_hourly') }}
    group by is_synthetic

),

today as (

    select cast({{ to_berlin_time('current_timestamp') }} as date) as today_berlin

)

select
    hours_per_day.is_synthetic,
    hours_per_day.date_berlin,
    hours_per_day.hour_count
from hours_per_day
inner join first_day_per_source
    on hours_per_day.is_synthetic = first_day_per_source.is_synthetic
cross join today
where hours_per_day.date_berlin > first_day_per_source.first_day
    and hours_per_day.date_berlin < today.today_berlin
    and hours_per_day.hour_count != 24
    -- last Sunday of March or October: the clocks change, so the day has 23 or 25 hours
    and not (
        extract(month from hours_per_day.date_berlin) in (3, 10)
        and extract(day from hours_per_day.date_berlin) >= 25
        and {{ iso_day_of_week('hours_per_day.date_berlin') }} = 7
    )
