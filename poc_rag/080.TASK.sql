USE DATABASE PDF_RAG_DB;
USE SCHEMA APP; 
-- =========================================================
-- 07. TASK SCHEDULE
-- =========================================================
CREATE OR REPLACE TASK TASK_PROCESS_PDF_DELTAS
  WAREHOUSE = PDF_WH
  SCHEDULE = '10 MINUTE'
AS
  CALL SP_PROCESS_PDF_DELTAS();

-- Start it
ALTER TASK TASK_PROCESS_PDF_DELTAS RESUME;

-- Manual first run
CALL SP_PROCESS_PDF_DELTAS();

