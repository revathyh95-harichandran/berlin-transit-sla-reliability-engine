-- A live prediction may be a little early (VBB shows -120 s in the real data), but not more than 10 minutes early.
-- Like a train announcement that says "arriving 15 minutes before the timetable": the feed is wrong, not the train.
select
    departure_sk,
    planned_departure_berlin,
    predicted_departure_berlin
from {{ ref('fct_departures') }}
where predicted_departure_berlin is not null
    and predicted_departure_berlin < planned_departure_berlin - interval '10 minutes'
