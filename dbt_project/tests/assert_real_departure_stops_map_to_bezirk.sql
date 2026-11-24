-- Every real stop should sit inside one of the 12 Berlin district outlines.
-- A stop mapped to '000' (Unknown) is not wrong data, but it is a gap in the map, so we warn and list the stops.
-- Like a postal sorting office: a letter with no matching postcode goes in the "unknown" tray and someone should look at it.
{{ config(severity='warn') }}

select
    fct.stop_key,
    stops.stop_id,
    stops.stop_name,
    stops.bezirk_key,
    count(*) as departures
from {{ ref('fct_departures') }} as fct
inner join {{ ref('dim_stop') }} as stops
    on fct.stop_key = stops.stop_key
where not fct.is_synthetic
    and stops.bezirk_key = '000'
group by fct.stop_key, stops.stop_id, stops.stop_name, stops.bezirk_key
