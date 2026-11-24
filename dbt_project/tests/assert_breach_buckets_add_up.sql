-- Every departure lands in exactly one bucket (ADR-002 counting rules):
--   cancelled + no realtime + rated = scheduled
-- and breaches can only come from rated departures: breach_departures <= rated_departures.
-- Like splitting a class into "absent", "no marks yet" and "marked": the three piles must add up to the class.
select
    summary_key,
    scheduled_departures,
    cancelled_departures,
    no_realtime_departures,
    rated_departures,
    breach_departures
from {{ ref('mart_sla_breach_summary') }}
where scheduled_departures != cancelled_departures + no_realtime_departures + rated_departures
    or breach_departures > rated_departures
