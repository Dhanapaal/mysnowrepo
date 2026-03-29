USE DATABASE PDF_RAG_DB;
USE SCHEMA APP; 
-- =========================================================
-- 05. STREAM ON DIRECTORY TABLE
-- =========================================================
CREATE OR REPLACE STREAM AZ_PDF_STAGE_STREAM
  ON STAGE AZ_PDF_STAGE;

-- Optional: inspect initial rows after refresh
SELECT * FROM AZ_PDF_STAGE_STREAM;