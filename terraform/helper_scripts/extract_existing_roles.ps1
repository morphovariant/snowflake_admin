# Requires: snowsql in PATH and logged-in profile
$roles = snowsql -q "select name from snowflake.account_usage.roles where deleted_on is null order by name" `
                  -o output_format=csv -o header=false

function Slugify([string]$s) {
  $s = $s.ToLower()
  $s = [regex]::Replace($s, "[^a-z0-9]+", "_")
  return $s.Trim("_")
}

# roles.tf
"// generated" | Out-File -FilePath roles.tf -Encoding utf8
foreach ($role in $roles) {
  if (-not $role) { continue }
  $res = "role_{0}" -f (Slugify $role)
  @"
resource "snowflake_role" "$res" {
  name = "$role"
}
"@ | Add-Content roles.tf
}

# import_roles.ps1
"param()" | Out-File import_roles.ps1 -Encoding utf8
Add-Content import_roles.ps1 '$ErrorActionPreference = "Stop"'
foreach ($role in $roles) {
  if (-not $role) { continue }
  $res = "role_{0}" -f (Slugify $role)
  Add-Content import_roles.ps1 "terraform import snowflake_role.$res `"$role`""
}
Write-Host "Generated roles.tf and import_roles.ps1. Run: terraform init; ./import_roles.ps1"
