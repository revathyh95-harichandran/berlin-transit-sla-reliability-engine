/*
    stg_departures (Silver)

    Grain: one row per departure event per polled station per data source,
    i.e. one row per (departure_key, station_id, is_synthetic).

    What it does, step by step:
      1. Stack the real VBB departures and the synthetic practice departures on top of each other
         and flag each row with is_synthetic (like two stacks of receipts, one stamped "PRACTICE").
      2. "Latest prediction wins": the same departure shows up in several polls. I keep only the
         reading from the newest poll, because it is closest to what really happened.
      3. Convert every timestamp from UTC to Europe/Berlin (CET/CEST, daylight saving included).
      4. Replace trip_id and vehicle_trip_number with salted SHA-256 hashes, so the raw IDs
         never travel into Gold. Like swapping a passenger name for a locker number.
      5. Derive has_realtime and is_sla_breach exactly as ADR-002 defines them.
*/

{% set departure_columns = [
    'departure_key', 'trip_id', 'vehicle_trip_number', 'station_id', 'stop_id', 'stop_name',
    'stop_latitude', 'stop_longitude', 'planned_departure', 'predicted_departure', 'delay_seconds',
    'is_cancelled', 'line_id', 'line_name', 'product', 'direction', 'polled_at_utc', 'poll_slot_utc'
] %}

with real_departures as (

    -- Real departures collected from the VBB API.
    select
        {% for column in departure_columns -%}
            {{ column }},
        {% endfor -%}
        false as is_synthetic
    from {{ source('transit', 'vbb_departures') }}

),

synthetic_departures as (

    -- Generated practice departures, kept apart by the is_synthetic flag.
    select
        {% for column in departure_columns -%}
            {{ column }},
        {% endfor -%}
        coalesce(is_synthetic, true) as is_synthetic
    from {{ source('transit', 'synthetic_departures') }}

),

all_readings as (

    select * from real_departures
    union all
    select * from synthetic_departures

),

latest_reading as (

    -- One row per departure: the reading from the newest poll wins.
    select *
    from all_readings
    qualify row_number() over (
        partition by departure_key, station_id, is_synthetic
        order by polled_at_utc desc, poll_slot_utc desc
    ) = 1

),

converted as (

    select
        {{ surrogate_key(['departure_key', 'station_id', 'is_synthetic']) }} as departure_sk,
        departure_key,
        station_id,
        stop_id,
        stop_name,
        stop_latitude,
        stop_longitude,

        -- Sensitive IDs: only the salted hash leaves this model.
        {{ hash_pii('trip_id') }} as trip_id_hash,
        {{ hash_pii('vehicle_trip_number') }} as vehicle_trip_number_hash,

        line_id,
        line_name,
        product,
        direction,

        -- Timestamps: UTC (as collected) and Berlin wall-clock time.
        {{ to_utc_time('planned_departure') }} as planned_departure_utc,
        {{ to_berlin_time('planned_departure') }} as planned_departure_berlin,
        {{ to_berlin_time('predicted_departure') }} as predicted_departure_berlin,
        {{ to_utc_time('polled_at_utc') }} as polled_at_utc,
        {{ to_berlin_time('polled_at_utc') }} as polled_at_berlin,

        delay_seconds,
        coalesce(is_cancelled, false) as is_cancelled,
        is_synthetic

    from latest_reading

)

select
    departure_sk,
    departure_key,
    station_id,
    stop_id,
    stop_name,
    stop_latitude,
    stop_longitude,
    trip_id_hash,
    vehicle_trip_number_hash,
    line_id,
    line_name,
    product,
    direction,
    planned_departure_utc,
    planned_departure_berlin,
    predicted_departure_berlin,
    polled_at_utc,
    cast(planned_departure_berlin as date) as planned_date_berlin,
    extract(hour from planned_departure_berlin) as planned_hour_berlin,
    cast(polled_at_berlin as date) as polled_date_berlin,
    delay_seconds,
    is_cancelled,

    -- No realtime = the delay field is empty (ADR-002). Cancelled trips are counted separately.
    (delay_seconds is not null) as has_realtime,

    -- SLA breach = more than 300 seconds late. Cancelled departures are never breaches.
    (not is_cancelled and coalesce(delay_seconds, 0) > {{ var('sla_breach_seconds') }}) as is_sla_breach,

    is_synthetic
from converted
