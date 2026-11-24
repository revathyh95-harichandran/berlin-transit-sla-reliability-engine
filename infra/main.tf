# =============================================================================
# Berlin Transit SLA Reliability Engine: Snowflake infrastructure as code
# =============================================================================
# This file describes the same Snowflake objects as infra/rbac_masking.sql,
# but in Terraform. Think of it as the architect's blueprint of a building:
# anyone can read it and rebuild the same building from it.
#
# IMPORTANT: the live account was set up by running the SQL script, not by
# this file. Do NOT run "terraform apply" against that account until every
# existing object has been imported into the Terraform state
# (terraform import ...). Otherwise Terraform would try to create objects that
# already exist and fail, or replace them.
# Safe commands: terraform fmt, terraform validate, terraform plan.
#
# Region: AWS eu-central-1 (Frankfurt), so all data stays in the EU.
# No secrets are stored here: logins come from variables / environment.
# =============================================================================

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "= 2.2.0"
    }
  }
}


# -----------------------------------------------------------------------------
# Variables (inputs). Set them with TF_VAR_<name> environment variables or a
# terraform.tfvars file (which is git-ignored).
# -----------------------------------------------------------------------------
variable "snowflake_organization_name" {
  description = "Snowflake organization name (first half of the account identifier)"
  type        = string
}

variable "snowflake_account_name" {
  description = "Snowflake account name (second half of the account identifier)"
  type        = string
}

variable "snowflake_admin_user" {
  description = "User that Terraform logs in as (needs ACCOUNTADMIN)"
  type        = string
}

variable "snowflake_admin_private_key" {
  description = "Private key (PEM text) of the admin user, for key-pair login"
  type        = string
  sensitive   = true
}

variable "svc_ingest_public_key" {
  description = "Public key of SVC_INGEST, without BEGIN/END lines or line breaks"
  type        = string
  default     = null
}

variable "svc_transform_public_key" {
  description = "Public key of SVC_TRANSFORM, without BEGIN/END lines or line breaks"
  type        = string
  default     = null
}

variable "daily_credit_quota" {
  description = "Daily credit budget of the warehouse (cost guard)"
  type        = number
  default     = 4
}


# -----------------------------------------------------------------------------
# Provider: how Terraform logs in to Snowflake (key pair, no password).
# -----------------------------------------------------------------------------
provider "snowflake" {
  organization_name = var.snowflake_organization_name
  account_name      = var.snowflake_account_name
  user              = var.snowflake_admin_user
  authenticator     = "SNOWFLAKE_JWT"
  private_key       = var.snowflake_admin_private_key
  role              = "ACCOUNTADMIN"
}


# -----------------------------------------------------------------------------
# Step 2 (created first, because the warehouse refers to it):
# daily spending cap, like a prepaid phone card.
# -----------------------------------------------------------------------------
resource "snowflake_resource_monitor" "daily_cap" {
  name            = "TRANSIT_DAILY_CAP"
  credit_quota    = var.daily_credit_quota
  frequency       = "DAILY"
  start_timestamp = "IMMEDIATELY"

  notify_triggers            = [80]
  suspend_trigger            = 100
  suspend_immediate_trigger  = 110
}


# -----------------------------------------------------------------------------
# Step 1: Warehouse (the compute engine). XSMALL, sleeps after 60 seconds.
# -----------------------------------------------------------------------------
resource "snowflake_warehouse" "transit" {
  name                = "TRANSIT_WH"
  warehouse_size      = "XSMALL"
  auto_suspend        = 60
  auto_resume         = true
  initially_suspended = true
  resource_monitor    = snowflake_resource_monitor.daily_cap.name
  comment             = "Berlin transit pipeline: ingestion, dbt and BI refresh"
}


# -----------------------------------------------------------------------------
# Step 3: Databases for the three data layers (medallion architecture).
# -----------------------------------------------------------------------------
resource "snowflake_database" "raw" {
  name    = "RAW"
  comment = "Bronze layer: raw API loads"
}

resource "snowflake_database" "staging" {
  name    = "STAGING"
  comment = "Silver layer: cleaned data (dbt)"
}

resource "snowflake_database" "prod" {
  name    = "PROD"
  comment = "Gold layer: star schema (dbt)"
}

# A schema to keep governance objects (masking policies) in one place.
resource "snowflake_schema" "governance" {
  database = snowflake_database.prod.name
  name     = "GOVERNANCE"
}


# -----------------------------------------------------------------------------
# Step 4: Roles (least privilege), each granted to SYSADMIN so the account
# owner can manage them.
# -----------------------------------------------------------------------------
resource "snowflake_account_role" "ingest" {
  name    = "INGEST_ROLE"
  comment = "Loads API data into RAW"
}

resource "snowflake_account_role" "transform" {
  name    = "TRANSFORM_ROLE"
  comment = "Runs dbt: RAW -> STAGING -> PROD"
}

resource "snowflake_account_role" "bi_reader" {
  name    = "BI_READER_ROLE"
  comment = "Read-only access to PROD for Power BI"
}

resource "snowflake_grant_account_role" "ingest_to_sysadmin" {
  role_name        = snowflake_account_role.ingest.name
  parent_role_name = "SYSADMIN"
}

resource "snowflake_grant_account_role" "transform_to_sysadmin" {
  role_name        = snowflake_account_role.transform.name
  parent_role_name = "SYSADMIN"
}

resource "snowflake_grant_account_role" "bi_reader_to_sysadmin" {
  role_name        = snowflake_account_role.bi_reader.name
  parent_role_name = "SYSADMIN"
}

# Every role may use the warehouse (run queries) but not change its size.
resource "snowflake_grant_privileges_to_account_role" "wh_usage" {
  for_each = {
    ingest    = snowflake_account_role.ingest.name
    transform = snowflake_account_role.transform.name
    bi_reader = snowflake_account_role.bi_reader.name
  }

  account_role_name = each.value
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.transit.name
  }
}


# 4a. INGEST_ROLE: create its own schemas and tables inside RAW only.
resource "snowflake_grant_privileges_to_account_role" "ingest_raw_db" {
  account_role_name = snowflake_account_role.ingest.name
  privileges        = ["USAGE", "CREATE SCHEMA"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.raw.name
  }
}


# 4b. TRANSFORM_ROLE: read everything in RAW (now and in the future) ...
resource "snowflake_grant_privileges_to_account_role" "transform_raw_db" {
  account_role_name = snowflake_account_role.transform.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.raw.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "transform_raw_schemas_all" {
  account_role_name = snowflake_account_role.transform.name
  privileges        = ["USAGE"]
  on_schema {
    all_schemas_in_database = snowflake_database.raw.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "transform_raw_schemas_future" {
  account_role_name = snowflake_account_role.transform.name
  privileges        = ["USAGE"]
  on_schema {
    future_schemas_in_database = snowflake_database.raw.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "transform_raw_tables_all" {
  account_role_name = snowflake_account_role.transform.name
  privileges        = ["SELECT"]
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_database        = snowflake_database.raw.name
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "transform_raw_tables_future" {
  account_role_name = snowflake_account_role.transform.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_database        = snowflake_database.raw.name
    }
  }
}

# ... and build its own schemas in STAGING and PROD.
resource "snowflake_grant_privileges_to_account_role" "transform_staging_db" {
  account_role_name = snowflake_account_role.transform.name
  privileges        = ["USAGE", "CREATE SCHEMA"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.staging.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "transform_prod_db" {
  account_role_name = snowflake_account_role.transform.name
  privileges        = ["USAGE", "CREATE SCHEMA"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.prod.name
  }
}


# 4c. BI_READER_ROLE: read-only on PROD (tables and views, now and in future).
resource "snowflake_grant_privileges_to_account_role" "bi_prod_db" {
  account_role_name = snowflake_account_role.bi_reader.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.prod.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "bi_prod_schemas_all" {
  account_role_name = snowflake_account_role.bi_reader.name
  privileges        = ["USAGE"]
  on_schema {
    all_schemas_in_database = snowflake_database.prod.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "bi_prod_schemas_future" {
  account_role_name = snowflake_account_role.bi_reader.name
  privileges        = ["USAGE"]
  on_schema {
    future_schemas_in_database = snowflake_database.prod.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "bi_prod_objects_all" {
  for_each = toset(["TABLES", "VIEWS"])

  account_role_name = snowflake_account_role.bi_reader.name
  privileges        = ["SELECT"]
  on_schema_object {
    all {
      object_type_plural = each.value
      in_database        = snowflake_database.prod.name
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "bi_prod_objects_future" {
  for_each = toset(["TABLES", "VIEWS"])

  account_role_name = snowflake_account_role.bi_reader.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = each.value
      in_database        = snowflake_database.prod.name
    }
  }
}


# -----------------------------------------------------------------------------
# Steps 5 and 7: Service users (robot accounts, key-pair login only).
# Each has its own key pair, so one can be revoked without touching the other.
# -----------------------------------------------------------------------------
resource "snowflake_service_user" "svc_ingest" {
  name              = "SVC_INGEST"
  default_role      = snowflake_account_role.ingest.name
  default_warehouse = snowflake_warehouse.transit.name
  rsa_public_key    = var.svc_ingest_public_key
  comment           = "Scheduled collector: loads VBB + weather data into RAW"
}

resource "snowflake_grant_account_role" "ingest_to_svc" {
  role_name = snowflake_account_role.ingest.name
  user_name = snowflake_service_user.svc_ingest.name
}

resource "snowflake_service_user" "svc_transform" {
  name              = "SVC_TRANSFORM"
  default_role      = snowflake_account_role.transform.name
  default_warehouse = snowflake_warehouse.transit.name
  rsa_public_key    = var.svc_transform_public_key
  comment           = "Scheduled dbt run: RAW -> STAGING -> PROD"
}

resource "snowflake_grant_account_role" "transform_to_svc" {
  role_name = snowflake_account_role.transform.name
  user_name = snowflake_service_user.svc_transform.name
}


# -----------------------------------------------------------------------------
# Step 6: Masking policy. Only the transform role (and admins) see the real
# value; everyone else sees ***MASKED***, like the hidden digits on a bank card.
# -----------------------------------------------------------------------------
resource "snowflake_masking_policy" "mask_technical_id" {
  database = snowflake_database.prod.name
  schema   = snowflake_schema.governance.name
  name     = "MASK_TECHNICAL_ID"

  argument {
    name = "val"
    type = "STRING"
  }
  return_data_type = "STRING"

  body = <<-EOT
    CASE
      WHEN IS_ROLE_IN_SESSION('TRANSFORM_ROLE') THEN val
      WHEN IS_ROLE_IN_SESSION('ACCOUNTADMIN')   THEN val
      ELSE '***MASKED***'
    END
  EOT

  comment = "Hides hashed trip/vehicle IDs from BI readers"
}

# dbt (running as TRANSFORM_ROLE) may attach this policy to its own columns.
resource "snowflake_grant_privileges_to_account_role" "transform_governance_schema" {
  account_role_name = snowflake_account_role.transform.name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = "\"${snowflake_database.prod.name}\".\"${snowflake_schema.governance.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "transform_apply_mask" {
  account_role_name = snowflake_account_role.transform.name
  privileges        = ["APPLY"]
  on_schema_object {
    object_type = "MASKING POLICY"
    object_name = "\"${snowflake_database.prod.name}\".\"${snowflake_schema.governance.name}\".\"${snowflake_masking_policy.mask_technical_id.name}\""
  }
}
