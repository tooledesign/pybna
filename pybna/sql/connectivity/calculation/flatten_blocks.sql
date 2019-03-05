-- flatten blocks
DROP TABLE IF EXISTS tmp_unit_blocks;
CREATE TEMP TABLE tmp_unit_blocks AS (
    SELECT
        units.{units_id_col} AS id,
        units.{units_id_col} AS block_id
    FROM
        {units_schema}.{units_table} units,
        tmp_tile
    WHERE ST_DWithin(units.{units_geom_col},tmp_tile.geom,{connectivity_max_distance})
);

CREATE INDEX idx_tmp_unit_blocks_node_id ON tmp_unit_blocks (block_id);
ANALYZE tmp_unit_blocks;
