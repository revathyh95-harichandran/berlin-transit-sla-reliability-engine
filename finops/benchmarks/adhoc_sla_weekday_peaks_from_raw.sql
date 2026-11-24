-- =============================================================================
-- Ad-hoc question, answered straight from the RAW tables (no dbt models used):
--   "What is the SLA breach rate by Bezirk, weather and hour, on weekday peak hours?"
--
-- Think of it as cooking a meal from raw ingredients every time someone asks for
-- it: I wash, peel and chop everything again on each request. The Gold mart is the
-- same dish, prepared once and kept warm in the fridge.
--
-- Steps (all written by hand, every time):
--   1. Stack real and practice departures and keep only the newest reading per departure.
--   2. Convert times to Europe/Berlin.
--   3. Find the Bezirk of every stop with a point-in-polygon test (is the GPS pin inside the district outline?).
--   4. Pick the weather bucket for each hour (first matching rule wins).
--   5. Work out weekday / weekend-or-holiday and the peak hours (top 25 % busiest hours).
--   6. Count breaches and rated departures, then divide.
-- Dialect: DuckDB. Source: berlin_transit.transit.* (RAW copy in the local file).
-- =============================================================================

with readings as (

    -- Step 1a: real departures (VBB) and practice departures, one stack.
    select
        departure_key,
        station_id,
        stop_id,
        stop_latitude,
        stop_longitude,
        is_cancelled,
        delay_seconds,
        planned_departure,
        polled_at_utc,
        poll_slot_utc,
        false as is_synthetic
    from berlin_transit.transit.vbb_departures

    union all

    select
        departure_key,
        station_id,
        stop_id,
        stop_latitude,
        stop_longitude,
        is_cancelled,
        delay_seconds,
        planned_departure,
        polled_at_utc,
        poll_slot_utc,
        coalesce(is_synthetic, true) as is_synthetic
    from berlin_transit.transit.synthetic_departures

),

latest_reading as (

    -- Step 1b: "latest prediction wins": keep the reading from the newest poll per departure.
    select *
    from readings
    qualify row_number() over (
        partition by departure_key, station_id, is_synthetic
        order by polled_at_utc desc, poll_slot_utc desc
    ) = 1

),

departures as (

    -- Steps 2 and 6 inputs: Berlin date and hour, cancelled flag, UTC hour for the weather join.
    select
        is_synthetic,
        station_id,
        stop_id,
        stop_latitude,
        stop_longitude,
        coalesce(is_cancelled, false) as is_cancelled,
        delay_seconds,
        cast(timezone('Europe/Berlin', planned_departure) as date) as planned_date_berlin,
        extract(hour from timezone('Europe/Berlin', planned_departure)) as planned_hour_berlin,
        date_trunc('hour', timezone('UTC', planned_departure)) as planned_hour_utc
    from latest_reading

),

stop_points as (

    -- Step 3a: one GPS point per stop (average of its readings), as dim_stop does.
    select
        station_id,
        stop_id,
        avg(stop_latitude) as latitude,
        avg(stop_longitude) as longitude
    from departures
    group by station_id, stop_id

),

stop_bezirk as (

    -- Step 3b: point-in-polygon against the 12 district outlines. Lower code wins on a border.
    select
        stop_points.station_id,
        stop_points.stop_id,
        min(bezirke.bezirk_code) as bezirk_code
    from stop_points
    inner join berlin_transit.transit.bezirke as bezirke
        on st_contains(
            st_geomfromgeojson(bezirke.geometry_geojson),
            st_point(stop_points.longitude, stop_points.latitude)
        )
    group by stop_points.station_id, stop_points.stop_id

),

weather_readings as (

    -- Step 4a: real and practice hourly weather, stacked.
    select
        timezone('UTC', hour_utc) as hour_utc,
        false as is_synthetic,
        precipitation_mm,
        snowfall_cm,
        wind_speed_kmh,
        fetched_at_utc
    from berlin_transit.transit.weather_hourly

    union all

    select
        timezone('UTC', hour_utc) as hour_utc,
        coalesce(is_synthetic, true) as is_synthetic,
        precipitation_mm,
        snowfall_cm,
        wind_speed_kmh,
        cast(null as timestamp) as fetched_at_utc
    from berlin_transit.transit.synthetic_weather_hourly

),

weather_hour as (

    -- Step 4b: keep the newest fetch per hour, then give each hour one weather bucket.
    select
        hour_utc,
        is_synthetic,
        case
            when snowfall_cm > 0 then 'snow'
            when precipitation_mm >= 0.5 then 'rain'
            when wind_speed_kmh >= 30 then 'windy'
            else 'clear'
        end as weather_condition
    from weather_readings
    qualify row_number() over (
        partition by hour_utc, is_synthetic
        order by fetched_at_utc desc nulls last
    ) = 1

),

day_types as (

    -- Step 5a: every Berlin day that has departures, labelled weekday or weekend_or_holiday.
    select distinct
        departures.is_synthetic,
        departures.planned_date_berlin,
        case
            when isodow(departures.planned_date_berlin) >= 6 then 'weekend_or_holiday'
            when holidays.holiday_date is not null then 'weekend_or_holiday'
            else 'weekday'
        end as day_type
    from departures
    left join berlin_transit.dev_staging.berlin_public_holidays as holidays
        on departures.planned_date_berlin = holidays.holiday_date

),

hourly_counts as (

    -- Step 5b: departures per source, day and hour.
    select
        is_synthetic,
        planned_date_berlin,
        planned_hour_berlin as hour_of_day,
        count(*) as scheduled_departures
    from departures
    group by is_synthetic, planned_date_berlin, planned_hour_berlin

),

hour_profile as (

    -- Step 5c: average departures per hour and day type, with empty hours counted as 0.
    select
        day_types.is_synthetic,
        day_types.day_type,
        hours.hour_of_day,
        avg(coalesce(hourly_counts.scheduled_departures, 0)) as avg_scheduled_departures
    from day_types
    cross join generate_series(0, 23) as hours (hour_of_day)
    left join hourly_counts
        on
            day_types.is_synthetic = hourly_counts.is_synthetic
            and day_types.planned_date_berlin = hourly_counts.planned_date_berlin
            and hours.hour_of_day = hourly_counts.hour_of_day
    group by day_types.is_synthetic, day_types.day_type, hours.hour_of_day

),

peak_hours as (

    -- Step 5d: the busiest 25 % of hours (percent rank >= 0.75) are peak hours.
    select
        is_synthetic,
        day_type,
        hour_of_day,
        percent_rank() over (
            partition by is_synthetic, day_type
            order by avg_scheduled_departures
        ) >= 0.75 as is_peak_hour
    from hour_profile

)

-- Step 6: breach counts per Bezirk, weather, hour and source, for weekday peak hours only.
select
    coalesce(bezirke.bezirk_name, 'Unknown') as bezirk_name,
    coalesce(weather_hour.weather_condition, 'unknown') as weather_condition,
    departures.planned_hour_berlin as hour_of_day,
    departures.is_synthetic,
    sum(case when not departures.is_cancelled and coalesce(departures.delay_seconds, 0) > 300 then 1 else 0 end)
        as breach_departures,
    sum(case when not departures.is_cancelled and departures.delay_seconds is not null then 1 else 0 end)
        as rated_departures,
    sum(case when not departures.is_cancelled and coalesce(departures.delay_seconds, 0) > 300 then 1 else 0 end) * 1.0
    / nullif(sum(case when not departures.is_cancelled and departures.delay_seconds is not null then 1 else 0 end), 0)
        as breach_rate
from departures
inner join day_types
    on
        departures.is_synthetic = day_types.is_synthetic
        and departures.planned_date_berlin = day_types.planned_date_berlin
inner join peak_hours
    on
        day_types.is_synthetic = peak_hours.is_synthetic
        and day_types.day_type = peak_hours.day_type
        and departures.planned_hour_berlin = peak_hours.hour_of_day
left join stop_bezirk
    on
        departures.station_id = stop_bezirk.station_id
        and departures.stop_id = stop_bezirk.stop_id
left join berlin_transit.transit.bezirke as bezirke
    on stop_bezirk.bezirk_code = bezirke.bezirk_code
left join weather_hour
    on
        departures.planned_hour_utc = weather_hour.hour_utc
        and departures.is_synthetic = weather_hour.is_synthetic
where
    day_types.day_type = 'weekday'
    and peak_hours.is_peak_hour
group by
    coalesce(bezirke.bezirk_name, 'Unknown'),
    coalesce(weather_hour.weather_condition, 'unknown'),
    departures.planned_hour_berlin,
    departures.is_synthetic
order by
    bezirk_name,
    weather_condition,
    hour_of_day,
    departures.is_synthetic
