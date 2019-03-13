-- DROP TABLE IF EXISTS tmp_tileunits;
-- SELECT
--     units.{blocks_id_col} AS id,
--     units.node_ids
-- INTO TEMP TABLE tmp_tileunits
-- FROM
--     {blocks_schema}.{blocks_table} units,
--     tmp_tile
-- WHERE
--     units.node_ids IS NOT NULL
--     AND ST_Intersects(tmp_tile.geom,units.{blocks_geom_col})
-- ;


DROP TABLE IF EXISTS tmp_unit_nodes;
SELECT
    units.{blocks_id_col} AS id,
    unnest(units.node_ids) AS node_id
INTO TEMP TABLE tmp_unit_nodes
FROM
    {blocks_schema}.{blocks_table} units,
    tmp_tile
WHERE ST_DWithin(units.{blocks_geom_col},tmp_tile.geom,{connectivity_max_distance})
;

CREATE INDEX idx_tmp_unit_nodes_node_id ON tmp_unit_nodes (node_id);
ANALYZE tmp_unit_nodes;
