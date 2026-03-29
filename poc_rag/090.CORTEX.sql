USE DATABASE PDF_RAG_DB;
USE SCHEMA APP; 
-- =========================================================
-- 08. CORTEX SEARCH SERVICE
-- =========================================================
CREATE OR REPLACE CORTEX SEARCH SERVICE PDF_CHUNKS_SEARCH
  ON CHUNK_TEXT
  ATTRIBUTES RELATIVE_PATH, LAST_MODIFIED, MD5
  WAREHOUSE = PDF_WH
  TARGET_LAG = '15 minutes'
  EMBEDDING_MODEL = 'snowflake-arctic-embed-l-v2.0'
  AS (
    SELECT
      CHUNK_PK,
      CHUNK_TEXT,
      RELATIVE_PATH,
      LAST_MODIFIED,
      MD5
    FROM PDF_CHUNKS
  );

/*
  SELECT PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'PDF_CHUNKS_SEARCH',
    '{
      "query": "metformin",
      "columns": ["CHUNK_TEXT", "RELATIVE_PATH", "LAST_MODIFIED"],
      "limit": 5
    }'
  )
)['results'] AS RESULTS; 


-- 1) Do files exist on the stage at all?
LIST @AZ_PDF_STAGE;

-- 2) Does the directory table know about them?
SELECT *
FROM DIRECTORY(@AZ_PDF_STAGE)
WHERE LOWER(RELATIVE_PATH) LIKE '%.pdf';

-- 3) Force a manual refresh once
ALTER STAGE AZ_PDF_STAGE REFRESH;

-- 4) Check again
SELECT *
FROM DIRECTORY(@AZ_PDF_STAGE)
WHERE LOWER(RELATIVE_PATH) LIKE '%.pdf';

-- 5) Then check the stream
SELECT *
FROM AZ_PDF_STAGE_STREAM
WHERE LOWER(RELATIVE_PATH) LIKE '%.pdf';

SELECT * FROM PDF_DOCUMENTS ;
SELECT * FROM PDF_CHUNKS LIMIT 20;
*/