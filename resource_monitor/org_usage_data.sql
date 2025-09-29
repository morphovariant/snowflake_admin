-- query drafted on request by custom private GPT, 2025-09-29
-- use case: the primary account is down and failover has occured. 
-- because resource monitors and warehouses are account-specific and therefore not replicated
-- if there is a resource monitor that must be globally ensured, this query will (with latency)
-- report on credits used and may be used to set the correct remaining quota on the failover account resource monitor.

use role ORGADMIN;

set primary_account_name = '';
set failover_account_name = '';
set global_credit_budget = ;

-- 1) MTD credits in the primary account
with primary_acct as (
  select
    sum(credits_used) as credits_mtd
  from ORGANIZATION_USAGE.CREDITS_USED
  where start_time >= date_trunc('month', current_date())
    and account_name = $primary_account_name         -- or ACCOUNT_LOCATOR = 'XYZ12345'
    and service_type = 'WAREHOUSE_METERING'   -- optional: focus on warehouses
),

-- 2) MTD credits in the failover account
failover_acct as (
  select
    sum(credits_used) as credits_mtd
  from ORGANIZATION_USAGE.CREDITS_USED
  where start_time >= date_trunc('month', current_date())
    and account_name = $failover_account_name
    and service_type = 'WAREHOUSE_METERING'
)

select
  coalesce((select credits_mtd from primary_acct),0)      as us_east_mtd,
  coalesce((select credits_mtd from failover_acct),0)   as eu_mtd,
  $global_credit_budget                                   as global_budget,
  $global_credit_budget - (coalesce((select credits_mtd from primary_acct),0)
        + coalesce((select credits_mtd from failover_acct),0)) as remaining_global;
