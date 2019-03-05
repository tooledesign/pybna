DROP TABLE IF EXISTS tmp_block_roads;
CREATE TEMP TABLE tmp_block_roads AS (
    SELECT
        units.{units_id_col} AS id,
        unnest(units.road_ids) AS road_id
    FROM
        {units_schema}.{units_table} units,
        tmp_tile
    WHERE ST_DWithin(units.{units_geom_col},tmp_tile.geom,{connectivity_max_distance})
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
DROP TABLE IF EXISTS tmp_block_roads;
