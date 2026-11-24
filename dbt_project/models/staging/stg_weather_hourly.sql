/*
    stg_weather_hourly (Silver)

    Grain: one row per hour of weather per data source, i.e. one row per (hour_utc, is_synthetic).

    Real and synthetic weather are stacked, flagged with is_synthetic, and each hour is given a
    weather_condition bucket. Per ADR-002 the FIRST matching rule wins, like a bouncer checking
    a list top to bottom:
        snow  = snowfall > 0 cm
        rain  = precipitation >= 0.5 mm
        windy = wind speed >= 30 km/h
        clear = everything else
*/

with real_weather as (

    select
        {{ as_utc_timestamp('hour_utc') }} as hour_utc,
        temperature_c,
        precipitation_mm,
        rain_mm,
        snowfall_cm,
        wind_speed_kmh,
        wind_gusts_kmh,
        weather_code,
        is_forecast,
        {{ as_utc_timestamp('fetched_at_utc') }} as fetched_at_utc,
        false as is_synthetic
    from {{ source('transit', 'weather_hourly') }}

),

synthetic_weather as (

    select
        {{ as_utc_timestamp('hour_utc') }} as hour_utc,
        temperature_c,
        precipitation_mm,
        rain_mm,
        snowfall_cm,
        wind_speed_kmh,
        wind_gusts_kmh,
        weather_code,
        is_forecast,
        cast(null as timestamp) as fetched_at_utc,
        coalesce(is_synthetic, true) as is_synthetic
    from {{ source('transit', 'synthetic_weather_hourly') }}

),

all_weather as (

    select
        hour_utc,
        temperature_c,
        precipitation_mm,
        rain_mm,
        snowfall_cm,
        wind_speed_kmh,
        wind_gusts_kmh,
        weather_code,
        is_forecast,
        fetched_at_utc,
        is_synthetic
    from real_weather
    union all
    select
        hour_utc,
        temperature_c,
        precipitation_mm,
        rain_mm,
        snowfall_cm,
        wind_speed_kmh,
        wind_gusts_kmh,
        weather_code,
        is_forecast,
        fetched_at_utc,
        is_synthetic
    from synthetic_weather

),

latest_per_hour as (

    -- If an hour was loaded twice (forecast, then measured), the newest fetch wins.
    select
        hour_utc,
        temperature_c,
        precipitation_mm,
        rain_mm,
        snowfall_cm,
        wind_speed_kmh,
        wind_gusts_kmh,
        weather_code,
        is_forecast,
        fetched_at_utc,
        is_synthetic
    from all_weather
    qualify row_number() over (
        partition by hour_utc, is_synthetic
        order by fetched_at_utc desc nulls last
    ) = 1

),

converted as (

    select
        hour_utc,
        {{ to_berlin_time('hour_utc') }} as hour_berlin,
        temperature_c,
        precipitation_mm,
        rain_mm,
        snowfall_cm,
        wind_speed_kmh,
        wind_gusts_kmh,
        weather_code,
        is_forecast,
        is_synthetic
    from latest_per_hour

)

select
    {{ surrogate_key(['is_synthetic', 'hour_utc']) }} as weather_hour_key,
    hour_utc,
    hour_berlin,
    cast(hour_berlin as date) as date_berlin,
    extract(hour from hour_berlin) as hour_of_day_berlin,
    temperature_c,
    precipitation_mm,
    rain_mm,
    snowfall_cm,
    wind_speed_kmh,
    wind_gusts_kmh,
    weather_code,
    is_forecast,
    case
        when snowfall_cm > 0 then 'snow'
        when precipitation_mm >= 0.5 then 'rain'
        when wind_speed_kmh >= 30 then 'windy'
        else 'clear'
    end as weather_condition,
    is_synthetic
from converted
