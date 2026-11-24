/*
    dim_date (Gold dimension)

    Grain: one row per calendar day (Europe/Berlin), 2025-01-01 to 2027-12-31.

    A wall calendar with extra notes: weekday name, weekend flag, Berlin public holiday flag
    and the day_type used for rush-hour logic (ADR-002). A public holiday behaves like a Sunday
    on the roads, so it counts as 'weekend_or_holiday'.
*/

with digits as (

    select 0 as digit
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

),

numbers as (

    -- 0 to 9999, built by combining four digit columns (like an odometer).
    select ones.digit + 10 * tens.digit + 100 * hundreds.digit + 1000 * thousands.digit as day_offset
    from digits as ones
    cross join digits as tens
    cross join digits as hundreds
    cross join digits as thousands

),

calendar as (

    -- 1095 days = 2025, 2026 and 2027, matching the holiday seed file.
    select {{ add_days("cast('2025-01-01' as date)", 'day_offset') }} as date_day
    from numbers
    where day_offset < 1095

),

with_weekday as (

    select
        date_day,
        {{ iso_day_of_week('date_day') }} as iso_day_number
    from calendar

)

select
    extract(year from calendar_days.date_day) * 10000
    + extract(month from calendar_days.date_day) * 100
    + extract(day from calendar_days.date_day) as date_key,
    calendar_days.date_day as date,
    extract(year from calendar_days.date_day) as year_number,
    extract(month from calendar_days.date_day) as month_number,
    case calendar_days.iso_day_number
        when 1 then 'Monday'
        when 2 then 'Tuesday'
        when 3 then 'Wednesday'
        when 4 then 'Thursday'
        when 5 then 'Friday'
        when 6 then 'Saturday'
        else 'Sunday'
    end as day_of_week_name,
    calendar_days.iso_day_number,
    calendar_days.iso_day_number >= 6 as is_weekend,
    holidays.holiday_date is not null as is_public_holiday,
    holidays.holiday_name_en as public_holiday_name,
    case
        when calendar_days.iso_day_number >= 6 or holidays.holiday_date is not null then 'weekend_or_holiday'
        else 'weekday'
    end as day_type
from with_weekday as calendar_days
left join {{ ref('berlin_public_holidays') }} as holidays
    on calendar_days.date_day = holidays.holiday_date
