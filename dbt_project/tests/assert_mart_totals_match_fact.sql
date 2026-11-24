-- Fan-out guard: summing the mart must give back exactly the number of departures in the fact table.
-- If a join dropped or doubled rows on the way to the mart, the totals would differ.
select
    fact.row_count as fact_rows,
    mart.total_departures as mart_departures
from (select count(*) as row_count from {{ ref('fct_departures') }}) as fact
cross join (select sum(scheduled_departures) as total_departures from {{ ref('mart_sla_breach_summary') }}) as mart
where fact.row_count != mart.total_departures
