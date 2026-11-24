/*
    stg_bezirke (Silver)

    Grain: one row per Berlin district (Bezirk). 12 rows expected.

    The district outlines are kept as GeoJSON text. dim_stop later checks which outline
    each stop GPS point falls inside, like putting a pin on a paper map.
*/

select
    bezirk_code,
    bezirk_name,
    geometry_geojson
from {{ source('transit', 'bezirke') }}
