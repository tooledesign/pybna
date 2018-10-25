-- get total count of blocks not already in zones
SELECT COUNT(blocks.block_id)
FROM tmp_block_nodes blocks
WHERE NOT EXISTS (
    SELECT 1
    FROM {zones_schema}.{zones_table} zones
    WHERE blocks.block_id = ANY(zones.block_ids)
)
;
