BEGIN;

DROP TABLE IF EXISTS dataset;

CREATE TABLE dataset AS
SELECT
    "fixed acidity",
    density,
    "pH",
    alcohol / "pH" AS alcohol_over_ph,
    CASE WHEN color = 'white' THEN 0 ELSE 1 END AS label
FROM wine
ORDER BY RANDOM();

 ALTER TABLE dataset ADD COLUMN id SERIAL PRIMARY KEY;

COMMIT;