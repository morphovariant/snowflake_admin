-- Drafted by custom private GPT on request, 2025-09-29
-- How to use it:
-- High scan_to_size_ratio + many filter_refs on a column ⇒ strong candidate for a cluster key (put lowest-cardinality first).
-- Start with 1–2 columns only, and verify with SYSTEM$CLUSTERING_INFORMATION “what-if” runs on those columns before you commit.

-- Parameters you can tune
set MIN_ACTIVE_BYTES := 100*1024*1024*1024;  -- ≥100 GB
set LOOKBACK_DAYS    := 30;

with tbl_size as (
  select table_catalog, table_schema, table_name,
         active_bytes
  from snowflake.account_usage.table_storage_metrics
  qualify row_number() over (
    partition by table_catalog, table_schema, table_name
    order by recorded_on desc
  ) = 1
  and active_bytes >= $MIN_ACTIVE_BYTES
),
q as (
  -- Cost and basic stats per query
  select query_id, start_time, user_name, bytes_scanned
  from snowflake.account_usage.query_history
  where start_time >= dateadd(day, -$LOOKBACK_DAYS, current_timestamp())
),
ah as (
  -- Explode access history to (db, schema, table, column) used as FILTER
  select
    q.query_id,
    (value:objectName)::string      as object_name,
    (value:objectDomain)::string    as object_domain,
    (value:columns)::array          as cols,
    q.bytes_scanned
  from snowflake.account_usage.access_history ah
  join q on q.query_id = ah.query_id
  , lateral flatten(input => ah.base_objects_accessed) bo
  where ah.query_start_time >= dateadd(day, -$LOOKBACK_DAYS, current_timestamp())
    and object_domain in ('Table','View')  -- focus tables/views
),
filters as (
  -- Extract only columns used to FILTER (WHERE/JOIN predicates)
  select
    split_part(object_name, '.', 1) as table_catalog,
    split_part(object_name, '.', 2) as table_schema,
    split_part(object_name, '.', 3) as table_name,
    lower(value:name::string)       as column_name,
    count(*)                        as filter_refs,
    sum(bytes_scanned)              as bytes_scanned_sum
  from ah, lateral flatten(input => ah.cols)
  where value:usage_type::string in ('PREDICATE','JOIN_PREDICATE')
  group by 1,2,3,4
),
ranked as (
  select f.*, t.active_bytes,
         bytes_scanned_sum / nullif(active_bytes,0) as scan_to_size_ratio
  from filters f
  join tbl_size t
    on t.table_catalog = f.table_catalog
   and t.table_schema  = f.table_schema
   and t.table_name    = f.table_name
)
select *
from ranked
where scan_to_size_ratio > 5  -- heuristic: scanning >> table size ⇒ poor pruning
order by scan_to_size_ratio desc, filter_refs desc;
