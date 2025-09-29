-- drafted on request by custom private GPT, 2025-09-29
set global_credit_budget = ;

-- Read the most recent snapshot replicated from primary
with latest as (
  select warehouse_name, credits_mtd
  from ADMIN.MONITORING.CREDITS_SNAPSHOT
  qualify row_number() over (partition by warehouse_name order by snapshot_ts desc) = 1
),
primary_total as (
  select coalesce(sum(credits_mtd),0) as primary_mtd
  from latest
),
failover_total as (
  -- EU account's current month usage to date
  select coalesce(sum(credits_used),0) as failover_mtd
  from SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
  where start_time >= date_trunc('month', current_date())
)

select
  (select primary_mtd from primary_total) as prmary_mtd,
  (select failover_mtd from failover_total) as failover_mtd,
  $global_credit_budget as global_budget,
  $global_credit_budget - ((select primary_mtd from primary_total) + (select failover_mtd from failover_total)) as remaining_global;
