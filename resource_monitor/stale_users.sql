WITH user_activity AS (
  SELECT 
    u.name as user_name,
    u.deleted as is_deleted,
    GREATEST(
      MAX(l.event_timestamp), -- last login
      MAX(q.start_time),      -- last query
      MAX(a.query_start_time) -- last data access
    ) as last_activity_date
  FROM snowflake.account_usage.users u
  LEFT JOIN snowflake.account_usage.login_history l 
    ON u.name = l.user_name 
  LEFT JOIN snowflake.account_usage.query_history q 
    ON u.name = q.user_name
  LEFT JOIN snowflake.account_usage.access_history a 
    ON u.name = a.user_name
  WHERE u.deleted IS NULL
  GROUP BY 1, 2
)
SELECT 
  user_name,
  last_activity_date,
  DATEDIFF(days, last_activity_date, CURRENT_TIMESTAMP()) as days_inactive
FROM user_activity
WHERE DATEDIFF(days, last_activity_date, CURRENT_TIMESTAMP()) >= X
ORDER BY days_inactive DESC;
