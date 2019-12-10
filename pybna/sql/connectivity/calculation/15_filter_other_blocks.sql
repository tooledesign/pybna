-- buffer blocks
DROP TABLE IF EXISTS tmp_blocks;
CREATE TEMP TABLE tmp_blocks AS (
    SELECT
        blocks.{blocks_id_col}::{blocks_id_type} AS id,
        ST_Buffer(blocks.{blocks_geom_col},{blocks_roads_tolerance}) AS geom
    FROM
        {blocks_schema}.{blocks_table} blocks,
        tmp_this_block
    WHERE
        ST_DWithin(blocks.{blocks_geom_col},tmp_this_block.geom,{connectivity_max_distance})
        AND {destination_blocks_filter}
);
CREATE INDEX tsidx_b ON tmp_blocks USING GIST (geom);
ALTER TABLE tmp_blocks ADD PRIMARY KEY (id);
ANALYZE tmp_blocks;
