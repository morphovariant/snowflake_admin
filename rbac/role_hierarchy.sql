-- Full role-inheritance closure from ACCOUNT_USAGE.GRANTS_TO_ROLES
WITH RECURSIVE role_edges AS (
  -- Each edge means: CHILD_ROLE (grantee_name) inherits from PARENT_ROLE (name)
  SELECT DISTINCT
      CASE  WHEN GRANTED_ON IN ('DATABASE_ROLE','APPLICATION_ROLE') 
            THEN CONCAT_WS('.', TABLE_CATALOG, GRANTEE_NAME)
            ELSE GRANTEE_NAME
      END AS parent_role,
      CASE  WHEN GRANTED_TO IN ('DATABASE_ROLE','APPLICATION_ROLE')
            THEN CONCAT_WS('.', TABLE_CATALOG, NAME)
            ELSE NAME
      END AS child_role
  FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES
  WHERE   GRANTED_ON LIKE '%ROLE%'    -- captures all role to role grants
      AND DELETED_ON IS NULL          -- focus on active roles only
      AND GRANTED_BY <> 'SNOWFLAKE'   -- exclude standard grants
      AND PRIVILEGE = 'USAGE'         -- focus on usage inheritance, not ownership
),
closure AS (
  -- Seed: direct grants (depth = 1)
  SELECT
      child_role                 AS role,             -- descendant
      parent_role                AS inherited_from,   -- ancestor
      1                          AS depth,
      ARRAY_CONSTRUCT(parent_role, child_role) AS path
  FROM role_edges

  UNION ALL

  -- Recurse: walk downward (ancestor fixed, keep adding deeper descendants)
  SELECT
      e.child_role               AS role,
      c.inherited_from           AS inherited_from,
      c.depth + 1                AS depth,
      ARRAY_CAT(c.path, ARRAY_CONSTRUCT(e.child_role)) AS path
  FROM closure c
  JOIN role_edges e
    ON e.parent_role = c.role
  -- Guard against cycles (just in case)
  WHERE NOT ARRAY_CONTAINS(TO_VARIANT(e.child_role), c.path)
),
with_self AS (
  -- Optional: include each role inheriting from itself (depth = 0),
  -- useful when you want a complete “who has what” join later.
  SELECT
      r.ROLE_NAME AS role,
      r.ROLE_NAME AS inherited_from,
      0           AS depth,
      ARRAY_CONSTRUCT(r.ROLE_NAME) AS path
  FROM SNOWFLAKE.ACCOUNT_USAGE.ROLES r

  UNION ALL

  SELECT role, inherited_from, depth, path
  FROM closure
)
SELECT
    role,                       -- descendant role
    inherited_from,             -- ancestor role it (directly/indirectly) inherits
    depth,
    ARRAY_TO_STRING(path, ' -> ') AS path,
    (depth = 1) AS is_direct
FROM with_self
ORDER BY role, depth, inherited_from;
