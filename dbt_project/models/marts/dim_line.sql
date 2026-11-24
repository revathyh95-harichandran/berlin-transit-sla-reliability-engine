/*
    dim_line (Gold dimension)

    Grain: one row per transit line and mode (line_name + product), e.g. "S41" + suburban.

    Real and synthetic data share the same line rows, so a line can be compared across both.
    mode_label turns VBB product codes into the names riders use.
*/

select
    {{ surrogate_key(['line_name', 'product']) }} as line_key,
    line_name,
    product,
    case product
        when 'suburban' then 'S-Bahn'
        when 'subway' then 'U-Bahn'
        when 'tram' then 'Tram'
        when 'bus' then 'Bus'
        else 'Other'
    end as mode_label
from {{ ref('stg_departures') }}
group by line_name, product
