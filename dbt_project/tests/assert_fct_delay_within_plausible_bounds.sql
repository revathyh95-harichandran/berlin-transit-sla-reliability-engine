-- Delay sanity check: a departure cannot be more than 10 minutes early or 3 hours late.
-- Think of a bus timetable board: a bus 40 minutes early or 3 hours late means the feed is broken,
-- not the bus. The allowed window is -600 s (10 min early) to 10800 s (3 hours late).
-- Rows with no delay value (no realtime) are skipped: an empty delay is allowed by ADR-002.
select
    departure_sk,
    delay_seconds
from {{ ref('fct_departures') }}
where delay_seconds is not null
    and (delay_seconds < -600 or delay_seconds > 10800)
