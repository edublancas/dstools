BEGIN;

DROP TABLE IF EXISTS wine;

CREATE TABLE wine AS
SELECT *, 'red' AS color FROM red
UNION ALL
SELECT *, 'white' AS color FROM white

COMMIT;