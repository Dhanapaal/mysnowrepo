--CREATE DATABASE DP;
--CREATE OR REPLACE TABLE Study (    studyId NUMBER,    studyname STRING);
--INSERT INTO Study (studyId, studyname) VALUES
    --(1, 'Cardio Study'),
   -- (2, 'Neuro_Test'),
 --   (3, 'Cancer-Phase#1'),
 --   (4, 'Diabetes@Trial'),
 --   (5, 'Heart&Lung'),
 --   (6, 'Vision Study'),
  --  (7, 'Brain/Memory'),
 --   (8, 'Pediatric (Pilot)'),
 --   (9, 'Oncology%2026'),
 --   (10, 'GeneralStudy');

SELECT *
FROM Study
WHERE NOT REGEXP_LIKE(studyname, '.*[^A-Za-z0-9 ].*');