-- =============================================================================
-- Add the dbt service user SVC_TRANSFORM (run this on its own)
-- =============================================================================
-- Use this when the main setup (infra/rbac_masking.sql) has already been run
-- and only the dbt user is missing. Safe to run again.
--
-- SVC_TRANSFORM is a robot account: key-pair login only, no password.
-- It gets its own key pair, separate from SVC_INGEST.
-- =============================================================================

USE ROLE ACCOUNTADMIN;

CREATE USER IF NOT EXISTS SVC_TRANSFORM
    TYPE              = SERVICE
    DEFAULT_ROLE      = TRANSFORM_ROLE
    DEFAULT_WAREHOUSE = TRANSIT_WH
    COMMENT           = 'Scheduled dbt run: RAW -> STAGING -> PROD';

GRANT ROLE TRANSFORM_ROLE TO USER SVC_TRANSFORM;

-- After creating a second key pair, paste the public key (no BEGIN/END lines,
-- no line breaks) and run:
-- ALTER USER SVC_TRANSFORM SET RSA_PUBLIC_KEY = 'MIIBIjANBgkqh...';

-- Check: RSA_PUBLIC_KEY_FP should show a fingerprint once the key is set.
DESC USER SVC_TRANSFORM;
