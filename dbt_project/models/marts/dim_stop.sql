/*
    dim_stop (Gold dimension)

    Grain: one row per polled station and stop (station_id + stop_id).

    Each stop gets the Bezirk whose outline contains its GPS point (point-in-polygon, like
    dropping a pin on a paper map and reading the colour it lands in). A stop that lands in
    no outline gets bezirk_key '000' (Unknown). If a stop sits exactly on a border and matches
    two districts, the lower district code wins so every stop has exactly one district.
*/

with stops as (

    select
        station_id,
        stop_id,
        max(stop_name) as stop_name,
        avg(stop_latitude) as latitude,
        avg(stop_longitude) as longitude
    from {{ ref('stg_departures') }}
    group by station_id, stop_id

),

matched as (

    select
        stops.station_id,
        stops.stop_id,
        bezirke.bezirk_code
    from stops
    inner join {{ ref('stg_bezirke') }} as bezirke
        on {{ point_in_polygon('bezirke.geometry_geojson', 'stops.longitude', 'stops.latitude') }}
    qualify row_number() over (
        partition by stops.station_id, stops.stop_id
        order by bezirke.bezirk_code
    ) = 1

)

select
    {{ surrogate_key(['stops.station_id', 'stops.stop_id']) }} as stop_key,
    stops.stop_id,
    stops.stop_name,
    stops.latitude,
    stops.longitude,
    stops.station_id,
    coalesce(matched.bezirk_code, '000') as bezirk_key,
    coalesce(bezirke.bezirk_name, 'Unknown') as bezirk_name
from stops
left join matched
    on
        stops.station_id = matched.station_id
        and stops.stop_id = matched.stop_id
left join {{ ref('stg_bezirke') }} as bezirke
    on matched.bezirk_code = bezirke.bezirk_code
