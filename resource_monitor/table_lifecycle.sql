-- Long-format lifecycle model for tables based on TABLE_STORAGE_METRICS
-- One row per date, table_type, is_clone, state_name
-- Columns:
--   event_date   : DATE
--   table_type   : PERMANENT / TRANSIENT / TEMPORARY
--   is_clone     : TRUE/FALSE
--   state_name   : CREATED / DROPPED / FAILSAFE
--   event_delta  : net change in that state on that date
--   state_count  : cumulative count of tables in that state as of that date

-- Adjust this if your effective fail-safe retention is different.
SET failsafe_retention_days := 7;

WITH base AS (
    SELECT
        table_catalog,
        table_schema,
        table_name,
        case when id = clone_group_id and not is_transient then 'PERMANENT'
             when id = clone_group_id and is_transient then 'TRANSIENT'
             when id <> clone_group_id then 'CLONE'
        end as table_type
        DATE(table_created)              AS created_date,
        /* Treat "dropped" as the earliest of table/schema/catalog drop */
        LEAST(
            DATE(table_dropped),
            DATE(schema_dropped),
            DATE(catalog_dropped)
        )                          AS dropped_date,
        DATE(table_entered_failsafe)     AS failsafe_date
    FROM snowflake.account_usage.table_storage_metrics
    WHERE table_type IN ('PERMANENT', 'TRANSIENT', 'TEMPORARY')
),

-- Optional: compute an approximate purge date (leaving FAILSAFE)
-- If you don't want tables to ever leave FAILSAFE, just comment out
-- the PURGED event below and ignore this column.
with_purge AS (
    SELECT
        *,
        CASE
            WHEN failsafe_date IS NOT NULL THEN
                DATEADD('day', $failsafe_retention_days, failsafe_date)
            ELSE NULL
        END AS purge_date
    FROM base
),

events AS (
    -------------------------------------------------------------------
    -- ENTER CREATED state
    -------------------------------------------------------------------
    SELECT
        created_date       AS event_date,
        table_type,
        is_clone,
        'CREATED'          AS state_name,
        1                  AS delta
    FROM with_purge
    WHERE created_date IS NOT NULL

    UNION ALL

    -------------------------------------------------------------------
    -- LEAVE CREATED (when dropped) and ENTER DROPPED
    -------------------------------------------------------------------
    SELECT
        dropped_date       AS event_date,
        table_type,
        is_clone,
        'CREATED'          AS state_name,
        -1                 AS delta
    FROM with_purge
    WHERE dropped_date IS NOT NULL

    UNION ALL

    SELECT
        dropped_date       AS event_date,
        table_type,
        is_clone,
        'DROPPED'          AS state_name,
        1                  AS delta
    FROM with_purge
    WHERE dropped_date IS NOT NULL

    UNION ALL

    -------------------------------------------------------------------
    -- LEAVE DROPPED (when entering FAILSAFE) and ENTER FAILSAFE
    -------------------------------------------------------------------
    SELECT
        failsafe_date      AS event_date,
        table_type,
        is_clone,
        'DROPPED'          AS state_name,
        -1                 AS delta
    FROM with_purge
    WHERE failsafe_date IS NOT NULL

    UNION ALL

    SELECT
        failsafe_date      AS event_date,
        table_type,
        is_clone,
        'FAILSAFE'         AS state_name,
        1                  AS delta
    FROM with_purge
    WHERE failsafe_date IS NOT NULL

    UNION ALL

    -------------------------------------------------------------------
    -- OPTIONAL: LEAVE FAILSAFE (when purged)
    -- If you *don’t* want the FAILSAFE line to ever go back down,
    -- comment out this block.
    -------------------------------------------------------------------
    SELECT
        purge_date         AS event_date,
        table_type,
        is_clone,
        'FAILSAFE'         AS state_name,
        -1                 AS delta
    FROM with_purge
    WHERE purge_date IS NOT NULL
),

-- Aggregate per day so we have one delta per day/state/type/clone
daily_deltas AS (
    SELECT
        event_date,
        table_type,
        is_clone,
        state_name,
        SUM(delta) AS event_delta
    FROM events
    WHERE event_date IS NOT NULL
    GROUP BY event_date, table_type, is_clone, state_name
),

-- Running totals => "how many tables are in this state as of this date?"
state_counts AS (
    SELECT
        event_date,
        table_type,
        is_clone,
        state_name,
        event_delta,
        SUM(event_delta) OVER (
            PARTITION BY table_type, is_clone, state_name
            ORDER BY event_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS state_count
    FROM daily_deltas
)

SELECT
    event_date,
    table_type,
    is_clone,
    state_name,
    event_delta,
    state_count
FROM state_counts
ORDER BY event_date, table_type, is_clone, state_name;
