USE DATABASE PDF_RAG_DB;
USE SCHEMA APP; 

-- =========================================================
-- 06. STORED PROCEDURE: PROCESS PDF DELTAS
-- =========================================================

CREATE OR REPLACE PROCEDURE SP_PROCESS_PDF_DELTAS()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
  -- Safety sync: even though AUTO_REFRESH is enabled, this keeps the directory
  -- from drifting if an event was missed or queue permissions were fixed late.
  ALTER STAGE AZ_PDF_STAGE REFRESH;

  -- Snapshot the stream once so we can reuse the same delta set
  CREATE OR REPLACE TEMP TABLE TMP_STAGE_CHANGES AS
  SELECT *
  FROM AZ_PDF_STAGE_STREAM
  WHERE LOWER(RELATIVE_PATH) LIKE '%.pdf';

  -- -------------------------------------------------------
  -- 1) Handle deleted PDFs
  -- -------------------------------------------------------
  DELETE FROM PDF_CHUNKS
  WHERE RELATIVE_PATH IN (
    SELECT RELATIVE_PATH
    FROM TMP_STAGE_CHANGES
    WHERE METADATA$ACTION = 'DELETE'
  );

  UPDATE PDF_DOCUMENTS
  SET IS_ACTIVE = FALSE,
      UPDATED_AT = CURRENT_TIMESTAMP()
  WHERE RELATIVE_PATH IN (
    SELECT RELATIVE_PATH
    FROM TMP_STAGE_CHANGES
    WHERE METADATA$ACTION = 'DELETE'
  );

  -- -------------------------------------------------------
  -- 2) Remove any old copies for re-added/updated files
  -- -------------------------------------------------------
  DELETE FROM PDF_CHUNKS
  WHERE RELATIVE_PATH IN (
    SELECT RELATIVE_PATH
    FROM TMP_STAGE_CHANGES
    WHERE METADATA$ACTION = 'INSERT'
  );

  DELETE FROM PDF_DOCUMENTS
  WHERE RELATIVE_PATH IN (
    SELECT RELATIVE_PATH
    FROM TMP_STAGE_CHANGES
    WHERE METADATA$ACTION = 'INSERT'
  );

  -- -------------------------------------------------------
  -- 3) OCR new PDFs into document table
  -- -------------------------------------------------------
  INSERT INTO PDF_DOCUMENTS
  (
    RELATIVE_PATH,
    SIZE_BYTES,
    LAST_MODIFIED,
    MD5,
    FILE_URL,
    OCR_TEXT,
    INGESTED_AT,
    UPDATED_AT,
    IS_ACTIVE
  )
  SELECT
    RELATIVE_PATH,
    SIZE,
    LAST_MODIFIED,
    MD5,
    FILE_URL,
      AI_PARSE_DOCUMENT(
        TO_FILE('@AZ_PDF_STAGE', RELATIVE_PATH),
        {'mode': 'OCR'}
    ):"content"::STRING AS OCR_TEXT,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    TRUE
  FROM TMP_STAGE_CHANGES
  WHERE METADATA$ACTION = 'INSERT';

  -- -------------------------------------------------------
  -- 4) Chunk OCR text for Cortex Search
  --    Using ~1500 chars / 200 overlap as a practical start.
  -- -------------------------------------------------------
  INSERT INTO PDF_CHUNKS
  (
    CHUNK_PK,
    RELATIVE_PATH,
    CHUNK_NUM,
    CHUNK_TEXT,
    LAST_MODIFIED,
    MD5,
    INGESTED_AT
  )
  WITH CHUNKED AS (
    SELECT
      d.RELATIVE_PATH,
      d.LAST_MODIFIED,
      d.MD5,
      SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(
        d.OCR_TEXT,
        'none',
        1500,
        200
      ) AS CHUNK_ARRAY
    FROM PDF_DOCUMENTS d
    WHERE d.RELATIVE_PATH IN (
      SELECT RELATIVE_PATH
      FROM TMP_STAGE_CHANGES
      WHERE METADATA$ACTION = 'INSERT'
    )
      AND d.IS_ACTIVE = TRUE
  )
  SELECT
    RELATIVE_PATH || '::' || f.index::STRING AS CHUNK_PK,
    RELATIVE_PATH,
    f.index::NUMBER AS CHUNK_NUM,
    f.value::STRING AS CHUNK_TEXT,
    LAST_MODIFIED,
    MD5,
    CURRENT_TIMESTAMP()
  FROM CHUNKED,
       LATERAL FLATTEN(INPUT => CHUNK_ARRAY) f;

  RETURN 'Processed stage delta successfully';
END;
$$;