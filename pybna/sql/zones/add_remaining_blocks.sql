DROP TABLE IF EXISTS tmp_missingblocks;
SELECT
    blocks.block_id,
    blocks.geom
INTO TEMP TABLE tmp_missingblocks
FROM tmp_block_nodes blocks
WHERE NOT EXISTS (
    SELECT 1
    FROM {zones_schema}.{zones_table} zones
    WHERE blocks.block_id = ANY(zones.block_ids)
)
;

INSERT INTO {zones_schema}.{zones_table}
SELECT
    ARRAY[block_id],
    ST_Multi(geom)
FROM tmp_missingblocks
;
