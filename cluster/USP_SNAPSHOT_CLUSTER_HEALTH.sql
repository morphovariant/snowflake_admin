-- drafted on request by custom private GPT, 2025-09-29
-- This procedure loops through all clustered tables, captures depth and info JSON, and aggregates auto-clustering credits for the window.
-- Procedure to refresh snapshot
create or replace procedure ADMIN.MONITORING.SNAPSHOT_CLUSTER_HEALTH()
returns string
language sql
as
$$
declare
  v_start timestamp_ltz := dateadd(day, -7, current_timestamp());
begin
  -- Iterate clustered tables
  for r in (
    select table_catalog, table_schema, table_name, clustering_key, auto_clustering_on
    from snowflake.account_usage.tables
    where clustering_key is not null
  )
  do
    let fqtn string := quote_ident(r.table_catalog)||'.'||quote_ident(r.table_schema)||'.'||quote_ident(r.table_name);

    insert into ADMIN_MON.CLUSTER_HEALTH
    select
      current_timestamp(),
      r.table_catalog, r.table_schema, r.table_name,
      r.clustering_key,
      r.auto_clustering_on,
      -- depth uses the defined key by default
      to_double(system$clustering_depth(:fqtn)),
      -- full JSON (includes overlap %, histogram, recent AC errors)
      parse_json(system$clustering_information(:fqtn)),
      coalesce((
        select round(sum(credits_used),3)
        from snowflake.account_usage.automatic_clustering_history
        where table_name = r.table_name
          and schema_name = r.table_schema
          and database_name = r.table_catalog
          and start_time >= v_start
      ),0)
    ;
  end for;

  return 'OK';
end;
$$;
