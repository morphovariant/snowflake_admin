-- drafted on request by custom private GPT, 2025-09-29
-- Interpretations: High depth / overlap % + notable AC credits ⇒ you’re paying to maintain a key that isn’t yielding good pruning.

-- Heuristics you can tune:
--  avg_depth > 8  ⇒ poor clustering
--  ac_credits_7d > 5 ⇒ spending meaningful credits
--  overlap_pct > 0.30 ⇒ poor pruning (from info JSON)

with last as (
  select *
  from ADMIN.MONITORING.CLUSTER_HEALTH
  qualify row_number() over (
    partition by table_catalog, table_schema, table_name
    order by measured_at desc
  ) = 1
),
calc as (
  select
    measured_at, table_catalog, table_schema, table_name,
    clustering_key, auto_clustering_on, avg_depth, ac_credits_7d,
    try_to_double(info:"average_depth")        as depth_from_info,
    try_to_double(info:"overlap_pct")          as overlap_pct
  from last
)
select *
from calc
where (coalesce(avg_depth, depth_from_info, 0) > 8
       or coalesce(overlap_pct, 0) > 0.30)
  and ac_credits_7d > 5
order by coalesce(overlap_pct,0) desc, avg_depth desc;
