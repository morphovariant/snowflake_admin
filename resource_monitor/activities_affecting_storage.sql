-- Pick the day you care about (session TZ).
-- Account Usage data is stored in UTC; adjust target_date accordingly if needed.
SET target_date := '2025-11-20';

WITH base AS (
    SELECT
        q.query_id,
        q.query_text,
        q.query_type,
        q.start_time,
        q.end_time,
        q.user_name,
        q.role_name,
        q.role_type,
        q.rows_inserted,
        q.rows_updated,
        q.rows_deleted,
        q.bytes_scanned,
        q.bytes_written_to_result,
        a.objects_modified,
        a.object_modified_by_ddl
    FROM snowflake.account_usage.query_history q
    LEFT JOIN snowflake.account_usage.access_history a
      ON a.query_id = q.query_id
    WHERE TO_DATE(q.start_time) = TO_DATE($target_date)  -- date in UTC
      AND (
            q.query_type IN (
              'COPY', 'COPY INTO',
              'INSERT', 'UPDATE', 'DELETE', 'MERGE', 'TRUNCATE_TABLE',
              'CREATE_TABLE', 'CREATE_TABLE_AS_SELECT', 'CREATE_TABLE_CLONE',
              'CREATE_SCHEMA', 'CREATE_SCHEMA_CLONE',
              'CREATE_DATABASE', 'CREATE_DATABASE_CLONE',
              'DROP_TABLE', 'DROP_SCHEMA', 'DROP_DATABASE',
              'UNDROP_TABLE', 'UNDROP_SCHEMA', 'UNDROP_DATABASE',
              'ALTER_TABLE', 'ALTER_SCHEMA', 'ALTER_DATABASE'
            )
            OR UPPER(q.query_text) LIKE 'PUT %'
            OR UPPER(q.query_text) LIKE 'COPY INTO %'
          )
),

-- DML + load/unload targets (tables, stages, pipes, streams, etc.)
dml_objects AS (
    SELECT
        b.query_id,
        b.query_text,
        b.query_type,
        b.start_time,
        b.end_time,
        b.user_name,
        b.role_name,
        b.role_type,
        'DML/LOAD' AS activity_scope,
        o.value:objectDomain::string        AS object_domain,
        o.value:objectName::string          AS fq_object_name,
        SPLIT_PART(o.value:objectName::string, '.', 1) AS database_name,
        SPLIT_PART(o.value:objectName::string, '.', 2) AS schema_name,
        SPLIT_PART(o.value:objectName::string, '.', 3) AS object_name,
        b.rows_inserted,
        b.rows_updated,
        b.rows_deleted
    FROM base b,
         LATERAL FLATTEN(input => b.objects_modified) o
),

-- DDL affecting lifecycle (create/drop/undrop/clone/alter)
ddl_objects AS (
    SELECT
        b.query_id,
        b.query_text,
        b.query_type,
        b.start_time,
        b.end_time,
        b.user_name,
        b.role_name,
        b.role_type,
        'DDL' AS activity_scope,
        b.object_modified_by_ddl:objectDomain::string   AS object_domain,
        b.object_modified_by_ddl:objectName::string     AS fq_object_name,
        SPLIT_PART(b.object_modified_by_ddl:objectName::string, '.', 1) AS database_name,
        SPLIT_PART(b.object_modified_by_ddl:objectName::string, '.', 2) AS schema_name,
        SPLIT_PART(b.object_modified_by_ddl:objectName::string, '.', 3) AS object_name,
        b.rows_inserted,
        b.rows_updated,
        b.rows_deleted
    FROM base b
    WHERE b.object_modified_by_ddl IS NOT NULL
),

all_query_activities AS (
    SELECT * FROM dml_objects
    UNION ALL
    SELECT * FROM ddl_objects
),

classified AS (
    SELECT
        a.*,
        CASE
            WHEN UPPER(query_text) LIKE 'PUT %'
              THEN 'PUT into stage'
            WHEN query_type LIKE 'COPY%' AND object_domain = 'STAGE'
              THEN 'COPY INTO stage (unload)'
            WHEN query_type LIKE 'COPY%' AND object_domain IN ('TABLE','VIEW')
              THEN 'COPY INTO table (load)'
            WHEN query_type IN ('INSERT','MERGE')
              THEN 'Insert / Merge'
            WHEN query_type = 'UPDATE'
              THEN 'Update'
            WHEN query_type IN ('DELETE','TRUNCATE_TABLE')
              THEN 'Delete / Truncate'
            WHEN query_type LIKE 'CREATE%CLONE'
              THEN 'Clone (time-travel based)'
            WHEN query_type = 'CREATE_TABLE_AS_SELECT'
              THEN 'CTAS (new storage)'
            WHEN query_type LIKE 'CREATE_%'
              THEN 'Create object'
            WHEN query_type LIKE 'DROP_%'
              THEN 'Drop object (moves to TT/Fail-safe)'
            WHEN query_type LIKE 'UNDROP_%'
              THEN 'Undrop object (from TT/Fail-safe)'
            WHEN query_type LIKE 'ALTER_%'
              THEN 'Alter object (may affect retention)'
            ELSE query_type
        END AS logical_activity_type
    FROM all_query_activities a
),

-- Account-level daily averages for the same date, for context
account_storage_day AS (
    SELECT
        s.usage_date,
        s.storage_bytes,    -- tables (active + TT)
        s.failsafe_bytes,
        s.stage_bytes
    FROM snowflake.account_usage.storage_usage s
    WHERE s.usage_date = TO_DATE($target_date)
)

SELECT
    c.start_time,
    c.user_name,
    c.role_name,
    c.role_type,
    c.logical_activity_type              AS activity_type,
    c.activity_scope,
    c.object_domain,
    c.database_name,
    c.schema_name,
    c.object_name,
    c.rows_inserted,
    c.rows_updated,
    c.rows_deleted,
    c.query_id,
    c.query_type,
    c.query_text,
    a.storage_bytes                      AS acct_storage_bytes_avg,
    a.failsafe_bytes                     AS acct_failsafe_bytes_avg,
    a.stage_bytes                        AS acct_stage_bytes_avg
FROM classified c
LEFT JOIN account_storage_day a
  ON 1 = 1     -- same per row, but handy context
WHERE c.object_domain IN ('TABLE','VIEW','STAGE','PIPE','STREAM')
ORDER BY c.start_time, c.query_id;
