-- Fan-out guard: the fact table must hold exactly as many rows as the cleaned departures in Silver.
-- If a dimension join ever multiplied rows (like a receipt line printed twice), the counts would differ.
-- Skipped automatically when a single day is being backfilled with run_date.
{% if var('run_date', none) is none %}
select
    fact.row_count as fact_rows,
    staging.row_count as staging_rows
from (select count(*) as row_count from {{ ref('fct_departures') }}) as fact
cross join (select count(*) as row_count from {{ ref('stg_departures') }}) as staging
where fact.row_count != staging.row_count
{% else %}
select 1 as fact_rows, 1 as staging_rows where 1 = 0
{% endif %}
