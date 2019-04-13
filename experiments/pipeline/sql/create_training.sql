BEGIN;

DROP TABLE IF EXISTS training;

CREATE TABLE training AS
SELECT
    "fixed acidity",
    density,
    "pH",
    alcohol_over_ph,
    label
FROM dataset
WHERE id <= (SELECT 0.7 * COUNT(*) FROM dataset);

COMMIT;