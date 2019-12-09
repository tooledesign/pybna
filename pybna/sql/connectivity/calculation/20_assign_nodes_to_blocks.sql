-- find matching roads
DROP TABLE IF EXISTS tmp_blocks_roads;
CREATE TEMP TABLE tmp_blocks_roads AS (
    SELECT
        blocks.id::{blocks_id_type},
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

DROP TABLE IF EXISTS tmp_blocks_nodes;
CREATE TEMP TABLE tmp_blocks_nodes AS (
    SELECT
        tmp_blocks_roads.id::{blocks_id_type},
        nodes.{nodes_id_col} AS node_id
    FROM
        tmp_blocks_roads,
        {nodes_schema}.{nodes_table} nodes
    WHERE tmp_blocks_roads.road_id = nodes.road_id
);

CREATE INDEX idx_tmp_blocks_nodes_node_id ON tmp_blocks_nodes (node_id);
ANALYZE tmp_blocks_nodes;

-- drop tables that don't need to be carried through
DROP TABLE IF EXISTS tmp_blocks_roads;
