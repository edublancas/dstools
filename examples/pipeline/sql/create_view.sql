BEGIN;

DROP VIEW IF EXISTS wine;

CREATE VIEW wine AS
SELECT *, 'red' AS color FROM red
UNION ALL
SELECT *, 'white' AS color FROM white

COMMIT;