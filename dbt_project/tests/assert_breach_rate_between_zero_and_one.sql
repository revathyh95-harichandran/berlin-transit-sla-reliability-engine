-- A rate is a share, so it can never be below 0% or above 100%.
-- Like a class quiz score: 7 wrong out of 10 is 70%, but 12 wrong out of 10 means the marking went wrong.
select
    summary_key,
    breach_departures,
    rated_departures,
    breach_rate
from {{ ref('mart_sla_breach_summary') }}
where breach_rate is not null
    and (breach_rate < 0 or breach_rate > 1)
