/*
    stg_quarantine_events (Silver)

    Grain: one row per rejected record (quarantine_id).

    These are the parcels that failed the data-contract checklist and were put on the
    "return to sender" shelf. I keep the reason and a simplified reason_category so the
    data-quality mart can count them. The raw payload is deliberately NOT carried forward,
    because it contains the unhashed trip and vehicle IDs.
*/

select
    quarantine_id,
    source,
    station_id,
    {{ as_utc_timestamp('poll_slot_utc') }} as poll_slot_utc,
    cast({{ to_berlin_time(as_utc_timestamp('poll_slot_utc')) }} as date) as poll_date_berlin,
    {{ as_utc_timestamp('polled_at_utc') }} as polled_at_utc,
    reason,
    case
        when source = 'open_meteo' then 'weather_reading'
        when lower(reason) like 'planned_departure%' then 'missing_planned_time'
        when lower(reason) like 'delay_seconds%' then 'implausible_delay'
        when lower(reason) like 'product%' then 'out_of_scope_product'
        when lower(reason) like 'stop_latitude%' or lower(reason) like 'stop_longitude%' then 'bad_coordinates'
        when lower(reason) like 'record is not a json object%' then 'not_an_object'
        else 'other'
    end as reason_category,
    coalesce(is_synthetic, false) as is_synthetic
from {{ source('transit', 'quarantine_events') }}
qualify row_number() over (
    partition by quarantine_id
    order by polled_at_utc desc
) = 1
