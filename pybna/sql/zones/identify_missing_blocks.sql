-- checks tmp_zones for any blocks that haven't already been added to a zone
-- and returns one for further processing
SELECT
    blocks.block_id,
    blocks.node_ids
FROM tmp_block_nodes blocks
WHERE NOT EXISTS (
    SELECT 1
    FROM {zones_schema}.{zones_table} zones
    WHERE blocks.block_id = ANY(zones.block_ids)
)
LIMIT 1
;
