BEGIN;

DROP TABLE IF EXISTS testing;

CREATE TABLE testing AS
SELECT
    "fixed acidity",
    density,
    "pH",
    alcohol_over_ph,
    label
FROM dataset
WHERE id > (SELECT 0.7 * COUNT(*) FROM dataset);

COMMIT;