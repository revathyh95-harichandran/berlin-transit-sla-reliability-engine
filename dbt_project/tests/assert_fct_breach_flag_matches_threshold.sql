-- Breach rule from ADR-002, written out again so the test reads like the rule book:
--   * a cancelled departure is never a breach
--   * a departure with no delay value is never a breach
--   * otherwise it is a breach when the delay is more than sla_breach_seconds (300 s = 5 minutes)
-- Like a referee with a stopwatch: the flag stored in the fact table must match the rule exactly.
with expected as (

    select
        departure_sk,
        delay_seconds,
        is_sla_breach,
        case
            when is_cancelled then false
            when delay_seconds is null then false
            when delay_seconds > {{ var('sla_breach_seconds') }} then true
            else false
        end as expected_breach
    from {{ ref('fct_departures') }}

)

select *
from expected
where is_sla_breach != expected_breach
