WITH last_access AS (
  SELECT 
    f.value:objectDomain::string as object_type,
    f.value:objectName::string as object_name,
    MAX(query_start_time) as last_accessed_time
  FROM snowflake.account_usage.access_history,
    LATERAL FLATTEN(input => direct_objects_accessed) f
  WHERE query_start_time >= DATEADD(days, -365, CURRENT_TIMESTAMP())
  GROUP BY 1, 2
)
SELECT 
  object_type,
  object_name,
  last_accessed_time,
  DATEDIFF(days, last_accessed_time, CURRENT_TIMESTAMP()) as days_since_last_access
FROM last_access
WHERE DATEDIFF(days, last_accessed_time, CURRENT_TIMESTAMP()) >= 90
ORDER BY days_since_last_access DESC;
