/*
    dim_bezirk (Gold dimension)

    Grain: one row per Berlin district (Bezirk): the 12 official districts plus one
    'Unknown' row for stops that fall outside every district outline.

    The Unknown row means a fact row never has an empty district key, like a "miscellaneous"
    drawer in a filing cabinet.
*/

select
    bezirk_code as bezirk_key,
    bezirk_code,
    bezirk_name
from {{ ref('stg_bezirke') }}

union all

select
    '000' as bezirk_key,
    '000' as bezirk_code,
    'Unknown' as bezirk_name
