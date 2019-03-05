-- buffer blocks
DROP TABLE IF EXISTS tmp_blocks;
CREATE TEMP TABLE tmp_blocks AS (
    SELECT
        blocks.{blocks_id_col} AS id,
        ST_Buffer(blocks.{blocks_geom_col},{blocks_roads_tolerance}) AS geom
    FROM
        {blocks_schema}.{blocks_table} blocks,
        tmp_tile
    WHERE ST_DWithin(blocks.{blocks_geom_col},tmp_tile.geom,{connectivity_max_distance})
);
CREATE INDEX tsidx_b ON tmp_blocks USING GIST (geom);
ANALYZE tmp_blocks;

-- find matching roads
DROP TABLE IF EXISTS tmp_block_roads;
CREATE TEMP TABLE tmp_block_roads AS (
    SELECT
        blocks.id,
        roads.{roads_id_col} AS road_id
    FROM
        tmp_blocks blocks,
        {roads_schema}.{roads_table} roads
    WHERE
        ST_Intersects(blocks.geom,roads.geom)
        AND (
            ST_Contains(blocks.geom,roads.geom)
            OR ST_Length(ST_Intersection(blocks.geom,roads.geom)) > {blocks_min_road_length}
        )
);

DROP TABLE IF EXISTS tmp_unit_nodes;
CREATE TEMP TABLE tmp_unit_nodes AS (
    SELECT
        tmp_block_roads.id,
        nodes.{nodes_id_col} AS node_id
    FROM
        tmp_block_roads,
        {nodes_schema}.{nodes_table} nodes
    WHERE tmp_block_roads.road_id = nodes.road_id
);

CREATE INDEX idx_tmp_unit_nodes_node_id ON tmp_unit_nodes (node_id);
ANALYZE tmp_unit_nodes;

-- drop tables that don't need to be carried through
DROP TABLE IF EXISTS tmp_blocks;
DROP TABLE IF EXISTS tmp_block_roads;
