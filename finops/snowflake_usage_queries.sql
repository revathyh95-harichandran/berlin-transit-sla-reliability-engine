-- =============================================================================
-- How to measure REAL Snowflake usage for the Berlin transit pipeline.
-- Run these in Snowsight (Worksheets) with a role that can read ACCOUNT_USAGE,
-- for example ACCOUNTADMIN or a role granted the SNOWFLAKE database role.
--
-- Everyday picture: the warehouse is a taxi meter. WAREHOUSE_METERING_HISTORY is the
-- meter's receipt (how many minutes it ran, per hour). QUERY_HISTORY is the trip log
-- (which passenger asked for which ride, and how long it took).
--
-- Note: ACCOUNT_USAGE views lag behind real time by up to about 3 hours.
-- =============================================================================


-- 1. Credits used per day by the pipeline warehouse (last 30 days).
--    Compare the daily total with the 4-credit cap in infra/rbac_masking.sql.
select
    date_trunc('day', start_time) as usage_day,
    warehouse_name,
    round(sum(credits_used_compute), 3) as compute_credits,
    round(sum(credits_used_cloud_services), 3) as cloud_service_credits,
    round(sum(credits_used), 3) as total_credits
from snowflake.account_usage.warehouse_metering_history
where
    warehouse_name = 'TRANSIT_WH'
    and start_time >= dateadd('day', -30, current_timestamp())
group by
    date_trunc('day', start_time),
    warehouse_name
order by
    usage_day desc;


-- 2. Hourly view: how many credits does each hour of warehouse running cost?
--    Expect about 4 collector wakes per hour and 1 transform wake per hour,
--    so the hourly totals should show a steady 15-minute rhythm.
select
    date_trunc('hour', start_time) as usage_hour,
    round(sum(credits_used), 4) as credits_in_hour
from snowflake.account_usage.warehouse_metering_history
where
    warehouse_name = 'TRANSIT_WH'
    and start_time >= dateadd('day', -2, current_timestamp())
group by
    date_trunc('hour', start_time)
order by
    usage_hour desc;


-- 3. Which jobs ran, and how long did they take? (dbt queries carry the tag set in profiles.yml)
select
    date_trunc('day', start_time) as query_day,
    coalesce(query_tag, '(no tag: collector loads)') as job,
    count(*) as query_count,
    round(avg(total_elapsed_time) / 1000, 1) as avg_seconds_per_query,
    round(sum(total_elapsed_time) / 1000 / 60, 1) as total_minutes
from snowflake.account_usage.query_history
where
    warehouse_name = 'TRANSIT_WH'
    and start_time >= dateadd('day', -7, current_timestamp())
group by
    date_trunc('day', start_time),
    coalesce(query_tag, '(no tag: collector loads)')
order by
    query_day desc,
    total_minutes desc;


-- 4. Slowest queries in the last 7 days (candidates for optimisation).
select
    start_time,
    query_tag,
    round(total_elapsed_time / 1000, 1) as seconds,
    bytes_scanned,
    rows_produced,
    left(query_text, 200) as query_start
from snowflake.account_usage.query_history
where
    warehouse_name = 'TRANSIT_WH'
    and start_time >= dateadd('day', -7, current_timestamp())
    and execution_status = 'SUCCESS'
order by
    total_elapsed_time desc
limit 20;


-- 5. Storage growth of the RAW layer (bytes per table, latest figures).
select
    table_catalog,
    table_schema,
    table_name,
    row_count,
    round(active_bytes / power(1024, 2), 2) as active_mb,
    round(time_travel_bytes / power(1024, 2), 2) as time_travel_mb
from snowflake.account_usage.table_storage_metrics
where
    table_catalog = 'RAW'
order by
    active_bytes desc;
