terraform {
  required_version = ">= 1.6.0"
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = ">= 0.94.0"
    }
  }
}
