-- Same target_date as above
SET target_date := '2025-11-20';

SELECT
    CASE
        WHEN DATE(table_created) = TO_DATE($target_date)
            THEN 'TABLE_CREATED'
        WHEN DATE(table_dropped) = TO_DATE($target_date)
            THEN 'TABLE_DROPPED (moves to Time Travel)'
        WHEN DATE(table_entered_failsafe) = TO_DATE($target_date)
            THEN 'TABLE_ENTERED_FAILSAFE'
    END                                          AS lifecycle_event,
    table_catalog                                AS database_name,
    table_schema                                 AS schema_name,
    table_name                                   AS object_name,
    active_bytes,
    time_travel_bytes,
    failsafe_bytes,
    retained_for_clone_bytes,
    table_created,
    table_dropped,
    table_entered_failsafe
FROM snowflake.account_usage.table_storage_metrics
WHERE
    DATE(table_created)          = TO_DATE($target_date)
 OR DATE(table_dropped)          = TO_DATE($target_date)
 OR DATE(table_entered_failsafe) = TO_DATE($target_date)
ORDER BY database_name, schema_name, object_name, lifecycle_event;
