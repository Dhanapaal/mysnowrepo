-- =========================================================
-- 2. NOTIFICATION INTEGRATION (QUEUE)
-- =========================================================
DROP NOTIFICATION INTEGRATION AZ_PDF_EVENTS_INT;
CREATE OR REPLACE NOTIFICATION INTEGRATION AZ_PDF_EVENTS_INT
  ENABLED = TRUE
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
  AZURE_STORAGE_QUEUE_PRIMARY_URI = 'https://storedpdfsaccount.queue.core.windows.net/snowflake-pdf-events'
  AZURE_TENANT_ID = '649a6719-9c20-40f6-9c1b-714671fb0cc3';

-- Run this and complete Azure consent / RBAC for the Snowflake service principal
DESC NOTIFICATION INTEGRATION AZ_PDF_EVENTS_INT;