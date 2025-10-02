# see roles.yaml for actual instantiated roles and relationships.

locals {
  roles = yamldecode(file("${path.module}/roles.yaml"))
}

module "roles" {
  source   = "./modules/role"
  for_each = local.roles

  name               = each.key
  comment            = try(each.value.comment, null)
  users              = try(each.value.users, [])
  grant_to_roles     = try(each.value.grant_to_roles, [])
  inherit_from_roles = try(each.value.inherit_from_roles, [])
}
