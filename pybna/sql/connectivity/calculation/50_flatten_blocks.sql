-- flatten blocks
DROP TABLE IF EXISTS tmp_unit_blocks;
CREATE TEMP TABLE tmp_unit_blocks AS (
    SELECT
        units.{blocks_id_col} AS id,
        units.{blocks_id_col} AS block_id
    FROM
        {blocks_schema}.{blocks_table} units,
        tmp_tile
    WHERE ST_DWithin(units.{blocks_geom_col},tmp_tile.geom,{connectivity_max_distance})
);

CREATE INDEX idx_tmp_unit_blocks_node_id ON tmp_unit_blocks (block_id);
ANALYZE tmp_unit_blocks;
