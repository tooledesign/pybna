DROP TABLE IF EXISTS tmp_zones;
SELECT
    array_agg({block_id_col}) AS block_ids,
    ST_Union({block_geom_col}) AS geom
FROM {blocks_schema}.{blocks_table}
WHERE FALSE
;

DELETE FROM tmp_zones;
