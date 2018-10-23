-- checks tmp_zones for any blocks that haven't already been added to a zone
-- and returns one for further processing
SELECT blocks.{block_id_col}
FROM {blocks_schema}.{blocks_table} blocks
WHERE NOT EXISTS (
    SELECT 1
    FROM tmp_zones
    WHERE blocks.{block_id_col} = ANY(tmp_zones.block_ids)
)
LIMIT 1
;
