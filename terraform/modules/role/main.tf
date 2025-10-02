# reusable resources for managing grants to users and between roles (hierarchy)

resource "snowflake_role" "this" {
  name    = var.name
  comment = var.comment
}

resource "snowflake_role_grants" "to_users" {
  count     = length(var.users) > 0 ? 1 : 0
  role_name = snowflake_role.this.name
  users     = var.users
}

resource "snowflake_role_grants" "inherit_from" {
  for_each  = toset(var.inherit_from_roles)
  role_name = each.value                      # upstream
  roles     = [snowflake_role.this.name]      # grant upstream to THIS
}
