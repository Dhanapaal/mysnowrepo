-- =========================================================
-- 1. STORAGE INTEGRATION
-- =========================================================
--DROP STORAGE INTEGRATION AZ_BLOB_INT;
CREATE OR REPLACE STORAGE INTEGRATION AZ_BLOB_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = '649a6719-9c20-40f6-9c1b-714671fb0cc3'
  STORAGE_ALLOWED_LOCATIONS = (
    'azure://storedpdfsaccount.blob.core.windows.net/pdfs/'
  );

-- Run this and complete Azure consent / RBAC for the Snowflake service principal
DESC STORAGE INTEGRATION AZ_BLOB_INT;

--SELECT SYSTEM$VALIDATE_STORAGE_INTEGRATION(  'AZ_BLOB_INT',  'azure://storedpdfsaccount.blob.core.windows.net/pdfs/',  'snowflake_validate.txt',  'all');