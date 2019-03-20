DROP TABLE IF EXISTS tmp_combined;
CREATE TEMP TABLE tmp_combined AS (
    SELECT
        i.{ints_id_col} AS id,
        tmp_controls.control,
        tmp_islands.island
    FROM
        {ints_schema}.{ints_table} i
        LEFT JOIN tmp_controls
            ON i.{ints_id_col} = tmp_controls.id
        LEFT JOIN tmp_islands
            ON i.{ints_id_col} = tmp_islands.id
);

UPDATE {ints_schema}.{ints_table} i
SET
    control = tmp_combined.control,
    island = tmp_combined.island
FROM tmp_combined
WHERE
    i.{ints_id_col} = tmp_combined.id
    AND (tmp_combined.control IS NOT NULL OR tmp_combined.island IS NOT NULL)
;

DROP TABLE tmp_combined;
