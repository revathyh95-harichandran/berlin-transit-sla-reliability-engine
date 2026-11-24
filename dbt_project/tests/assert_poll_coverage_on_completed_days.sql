-- Coverage = share of the expected 96 polls per station per day that really ran (ADR-002).
-- A finished real day below 90 % means scheduled runs were skipped or late.
-- Warn only: GitHub Actions can start runs late, so this depends on live scheduling, not on code.
-- Today is skipped because the day is still growing.
{{ config(severity='warn') }}

select
    date,
    coverage_pct,
    polls_expected,
    polls_ok,
    polls_ok_empty,
    polls_error
from {{ ref('mart_data_quality') }}
where not is_synthetic
    and not is_partial_day
    and coverage_pct < 90
