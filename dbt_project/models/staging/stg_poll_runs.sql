/*
    stg_poll_runs (Silver)

    Grain: one row per station per 15-minute poll slot (station_id, poll_slot_utc).

    This is the collector logbook, like a security guard patrol sheet: for every round
    it says whether the guard checked in (ok), found nothing to report (ok_empty), or
    could not complete the round (error). Only real polls exist; synthetic data has no logbook.
*/

select
    station_id,
    station_name,
    {{ as_utc_timestamp('poll_slot_utc') }} as poll_slot_utc,
    {{ to_berlin_time(as_utc_timestamp('poll_slot_utc')) }} as poll_slot_berlin,
    cast({{ to_berlin_time(as_utc_timestamp('poll_slot_utc')) }} as date) as poll_date_berlin,
    {{ as_utc_timestamp('polled_at_utc') }} as polled_at_utc,
    status,
    departures_received,
    departures_valid,
    departures_quarantined,
    duplicates_dropped,
    false as is_synthetic
from {{ source('transit', 'vbb_poll_runs') }}
qualify row_number() over (
    partition by station_id, poll_slot_utc
    order by polled_at_utc desc
) = 1
