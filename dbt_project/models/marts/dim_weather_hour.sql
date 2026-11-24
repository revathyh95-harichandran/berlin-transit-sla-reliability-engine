/*
    dim_weather_hour (Gold dimension)

    Grain: one row per hour of weather per data source (hour_utc + is_synthetic),
    plus one 'unknown' row for departures in an hour with no weather reading.

    The weather_condition bucket (snow, rain, windy, clear) follows ADR-002. The unknown row keeps
    every departure joinable instead of silently dropping it, like a "no label" folder.
*/

select
    weather_hour_key,
    hour_utc,
    hour_berlin,
    date_berlin,
    hour_of_day_berlin,
    weather_condition,
    temperature_c,
    precipitation_mm,
    rain_mm,
    snowfall_cm,
    wind_speed_kmh,
    wind_gusts_kmh,
    is_forecast,
    is_synthetic
from {{ ref('stg_weather_hourly') }}

union all

select
    '-1' as weather_hour_key,
    cast(null as timestamp) as hour_utc,
    cast(null as timestamp) as hour_berlin,
    cast(null as date) as date_berlin,
    cast(null as integer) as hour_of_day_berlin,
    'unknown' as weather_condition,
    cast(null as double) as temperature_c,
    cast(null as double) as precipitation_mm,
    cast(null as double) as rain_mm,
    cast(null as double) as snowfall_cm,
    cast(null as double) as wind_speed_kmh,
    cast(null as double) as wind_gusts_kmh,
    cast(null as boolean) as is_forecast,
    cast(null as boolean) as is_synthetic
