-- drafted on request by custom private GPT, 2025-09-29
-- purpose: if the latency on ORG_USAGE is not accurate enough for failover credit use monitoring...

-- Create a table that will be replicated in your failover group
create schema if not exists ADMIN.MONITORING;

create or replace table ADMIN.MONITORING.CREDITS_SNAPSHOT (
  snapshot_ts timestamp_ntz,
  warehouse_name string,
  credits_mtd number(38,3)
);

-- Task to refresh the snapshot every 5 minutes
create or replace task ADMIN.MONITORING.CREDIT_SNAP_TASK
  warehouse = PROD_WH
  schedule = '5 MINUTE'
as
  merge into ADMIN.MONITORING.CREDITS_SNAPSHOT t
  using (
    select
      current_timestamp() as snapshot_ts,
      warehouse_name,
      sum(credits_used) over (partition by warehouse_name) as credits_mtd
    from SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    where start_time >= date_trunc('month', current_date())
  ) s
  on t.snapshot_ts = s.snapshot_ts and t.warehouse_name = s.warehouse_name
  when not matched then insert values (s.snapshot_ts, s.warehouse_name, s.credits_mtd);

-- alter task ALTER.MONITORING.CREDIT_SNAP_TASK resume;
