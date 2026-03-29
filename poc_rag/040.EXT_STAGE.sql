USE DATABASE PDF_RAG_DB;
USE SCHEMA APP;
--DROP STAGE AZ_PDF_STAGE;
-- =========================================================
-- 3. EXTERNAL STAGE WITH DIRECTORY TABLE
-- =========================================================
CREATE OR REPLACE STAGE AZ_PDF_STAGE
  URL = 'azure://storedpdfsaccount.blob.core.windows.net/pdfs/'
  STORAGE_INTEGRATION = AZ_BLOB_INT
  DIRECTORY = (
    ENABLE = TRUE
    AUTO_REFRESH = TRUE
    NOTIFICATION_INTEGRATION = 'AZ_PDF_EVENTS_INT'
  );

--select * from DIRECTORY(PDF_STAGE)
-- Seed the directory table once
ALTER STAGE AZ_PDF_STAGE REFRESH; 

SELECT * FROM DIRECTORY(@AZ_PDF_STAGE);