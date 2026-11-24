-- A departure board shows today and tomorrow, not next month.
-- If a planned departure is more than 24 hours after "now" (UTC), the timestamp or the time zone is probably broken.
select
    departure_sk,
    planned_departure_utc
from {{ ref('fct_departures') }}
where planned_departure_utc > {{ add_hours(to_utc_time('current_timestamp'), '24') }}
