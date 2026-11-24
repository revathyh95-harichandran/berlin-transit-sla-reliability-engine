-- =============================================================================
-- Berlin Transit SLA Reliability Engine: Snowflake one-time setup
-- =============================================================================
-- How to run:
--   1. Log in to Snowflake (Snowsight) with your own user.
--   2. Open a new SQL worksheet, paste this whole file, and click "Run All".
--   3. It is safe to run again: every statement uses IF NOT EXISTS / OR REPLACE.
--
-- What it creates:
--   * 1 small warehouse (the "engine" that runs queries) that sleeps after 60 s
--   * 1 resource monitor (a daily spending cap on that warehouse)
--   * 3 databases:  RAW (Bronze)  ->  STAGING (Silver)  ->  PROD (Gold)
--   * 3 roles with least privilege (each role only gets what it needs)
--   * 1 service user SVC_INGEST that logs in with a key pair (no password)
--   * 1 masking policy to hide technical IDs from report readers
--
-- Region: this account runs in AWS eu-central-1 (Frankfurt), so all data stays
-- inside the EU.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Step 0: Use the admin role.
-- Resource monitors and account-wide grants can only be created by ACCOUNTADMIN.
-- -----------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;


-- -----------------------------------------------------------------------------
-- Step 1: Warehouse (the compute engine).
-- XSMALL is the smallest size (1 credit per hour while running).
-- AUTO_SUSPEND = 60 means it goes to sleep after 60 seconds of no work,
-- like a light on a motion sensor. AUTO_RESUME wakes it on the next query.
-- -----------------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS TRANSIT_WH
    WAREHOUSE_SIZE      = 'XSMALL'
    AUTO_SUSPEND        = 60
    AUTO_RESUME         = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT             = 'Berlin transit pipeline: ingestion, dbt and BI refresh';

-- Re-apply the cost settings in case the warehouse already existed.
ALTER WAREHOUSE TRANSIT_WH SET
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND   = 60
    AUTO_RESUME    = TRUE;


-- -----------------------------------------------------------------------------
-- Step 2: Daily spending cap (resource monitor).
-- Like a prepaid phone card: when the daily credit budget is used up, the
-- warehouse is suspended until the next day.
--   80 %  -> send a notification
--   100 % -> suspend after running queries finish
--   110 % -> suspend immediately
-- Expected use is about 1.5-2 credits per day, so 4 leaves room for dbt runs.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE RESOURCE MONITOR TRANSIT_DAILY_CAP
    WITH CREDIT_QUOTA    = 4
         FREQUENCY       = DAILY
         START_TIMESTAMP = IMMEDIATELY
    TRIGGERS ON 80  PERCENT DO NOTIFY
             ON 100 PERCENT DO SUSPEND
             ON 110 PERCENT DO SUSPEND_IMMEDIATE;

ALTER WAREHOUSE TRANSIT_WH SET RESOURCE_MONITOR = TRANSIT_DAILY_CAP;


-- -----------------------------------------------------------------------------
-- Step 3: Databases for the three data layers (medallion architecture).
--   RAW     = Bronze: data exactly as it arrived from the APIs
--   STAGING = Silver: cleaned, typed, deduplicated
--   PROD    = Gold:   star schema used by Power BI
-- -----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS RAW     COMMENT = 'Bronze layer: raw API loads';
CREATE DATABASE IF NOT EXISTS STAGING COMMENT = 'Silver layer: cleaned data (dbt)';
CREATE DATABASE IF NOT EXISTS PROD    COMMENT = 'Gold layer: star schema (dbt)';

-- A schema to keep governance objects (masking policies) in one place.
CREATE SCHEMA IF NOT EXISTS PROD.GOVERNANCE;


-- -----------------------------------------------------------------------------
-- Step 4: Roles (least privilege).
-- Think of these as library cards: each card opens only the rooms it needs.
--   INGEST_ROLE    -> can only write into RAW
--   TRANSFORM_ROLE -> reads RAW, builds STAGING and PROD
--   BI_READER_ROLE -> can only read PROD
-- All three roles are granted to SYSADMIN so the account owner can manage them.
-- -----------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS INGEST_ROLE    COMMENT = 'Loads API data into RAW';
CREATE ROLE IF NOT EXISTS TRANSFORM_ROLE COMMENT = 'Runs dbt: RAW -> STAGING -> PROD';
CREATE ROLE IF NOT EXISTS BI_READER_ROLE COMMENT = 'Read-only access to PROD for Power BI';

GRANT ROLE INGEST_ROLE    TO ROLE SYSADMIN;
GRANT ROLE TRANSFORM_ROLE TO ROLE SYSADMIN;
GRANT ROLE BI_READER_ROLE TO ROLE SYSADMIN;

-- Every role may use the warehouse (run queries) but not change its size.
GRANT USAGE ON WAREHOUSE TRANSIT_WH TO ROLE INGEST_ROLE;
GRANT USAGE ON WAREHOUSE TRANSIT_WH TO ROLE TRANSFORM_ROLE;
GRANT USAGE ON WAREHOUSE TRANSIT_WH TO ROLE BI_READER_ROLE;


-- 4a. INGEST_ROLE: create its own schemas and tables inside RAW only.
--     The loading tool creates one schema per source the first time it runs.
GRANT USAGE         ON DATABASE RAW TO ROLE INGEST_ROLE;
GRANT CREATE SCHEMA ON DATABASE RAW TO ROLE INGEST_ROLE;


-- 4b. TRANSFORM_ROLE: read everything in RAW (now and in the future) ...
GRANT USAGE  ON DATABASE RAW                       TO ROLE TRANSFORM_ROLE;
GRANT USAGE  ON ALL SCHEMAS    IN DATABASE RAW     TO ROLE TRANSFORM_ROLE;
GRANT USAGE  ON FUTURE SCHEMAS IN DATABASE RAW     TO ROLE TRANSFORM_ROLE;
GRANT SELECT ON ALL TABLES     IN DATABASE RAW     TO ROLE TRANSFORM_ROLE;
GRANT SELECT ON FUTURE TABLES  IN DATABASE RAW     TO ROLE TRANSFORM_ROLE;

--     ... and build its own schemas in STAGING and PROD.
GRANT USAGE, CREATE SCHEMA ON DATABASE STAGING TO ROLE TRANSFORM_ROLE;
GRANT USAGE, CREATE SCHEMA ON DATABASE PROD    TO ROLE TRANSFORM_ROLE;


-- 4c. BI_READER_ROLE: read-only on PROD (tables and views, now and in future).
GRANT USAGE  ON DATABASE PROD                   TO ROLE BI_READER_ROLE;
GRANT USAGE  ON ALL SCHEMAS    IN DATABASE PROD TO ROLE BI_READER_ROLE;
GRANT USAGE  ON FUTURE SCHEMAS IN DATABASE PROD TO ROLE BI_READER_ROLE;
GRANT SELECT ON ALL TABLES     IN DATABASE PROD TO ROLE BI_READER_ROLE;
GRANT SELECT ON FUTURE TABLES  IN DATABASE PROD TO ROLE BI_READER_ROLE;
GRANT SELECT ON ALL VIEWS      IN DATABASE PROD TO ROLE BI_READER_ROLE;
GRANT SELECT ON FUTURE VIEWS   IN DATABASE PROD TO ROLE BI_READER_ROLE;


-- -----------------------------------------------------------------------------
-- Step 5: Service user for the scheduled collector.
-- A "service user" is a robot account: it cannot log in with a password or
-- through the website, only with a key pair. Like a staff badge that opens
-- one door, instead of lending out my own house key.
-- The public key is added in a separate step (see the end of this file).
-- -----------------------------------------------------------------------------
CREATE USER IF NOT EXISTS SVC_INGEST
    TYPE              = SERVICE
    DEFAULT_ROLE      = INGEST_ROLE
    DEFAULT_WAREHOUSE = TRANSIT_WH
    COMMENT           = 'Scheduled collector: loads VBB + weather data into RAW';

GRANT ROLE INGEST_ROLE TO USER SVC_INGEST;


-- -----------------------------------------------------------------------------
-- Step 6: Masking policy (dynamic data masking).
-- The transit data has no personal data, but trip and vehicle IDs are still
-- hashed in Silver. This policy hides even the hashed IDs from report readers:
-- only the transform role (and admins) see the real value; everyone else sees
-- '***MASKED***'. Like a bank statement that only shows the last digits.
-- The policy is attached to columns later, when dbt has created the tables.
-- -----------------------------------------------------------------------------
CREATE MASKING POLICY IF NOT EXISTS PROD.GOVERNANCE.MASK_TECHNICAL_ID
    AS (val STRING) RETURNS STRING ->
    CASE
        WHEN IS_ROLE_IN_SESSION('TRANSFORM_ROLE') THEN val
        WHEN IS_ROLE_IN_SESSION('ACCOUNTADMIN')   THEN val
        ELSE '***MASKED***'
    END
    COMMENT = 'Hides hashed trip/vehicle IDs from BI readers';

-- dbt (running as TRANSFORM_ROLE) may attach this policy to its own columns.
GRANT USAGE ON SCHEMA PROD.GOVERNANCE TO ROLE TRANSFORM_ROLE;
GRANT APPLY ON MASKING POLICY PROD.GOVERNANCE.MASK_TECHNICAL_ID TO ROLE TRANSFORM_ROLE;


-- -----------------------------------------------------------------------------
-- Step 7: Quick check. You should see the warehouse, 3 roles and the user.
-- -----------------------------------------------------------------------------
SHOW WAREHOUSES LIKE 'TRANSIT_WH';
SHOW ROLES LIKE '%_ROLE';
SHOW USERS LIKE 'SVC_INGEST';


-- =============================================================================
-- LATER (after creating the key pair): attach the public key to SVC_INGEST.
-- Paste the text of rsa_key.pub WITHOUT the "-----BEGIN/END PUBLIC KEY-----"
-- lines and without line breaks, then run this single statement:
--
-- ALTER USER SVC_INGEST SET RSA_PUBLIC_KEY = 'MIIBIjANBgkqh...';
-- DESC USER SVC_INGEST;   -- RSA_PUBLIC_KEY_FP should now show a fingerprint
-- =============================================================================
