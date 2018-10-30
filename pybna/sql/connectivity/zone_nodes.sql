DROP TABLE IF EXISTS tmp_tilezones;
SELECT
    zones.{zones_id_col} AS id,
    zones.node_ids
INTO TEMP TABLE tmp_tilezones
FROM
    {zones_schema}.{zones_table} zones,
    tmp_tile
WHERE
    ST_Intersects(tmp_tile.geom,zones.{zones_geom_col})
;


DROP TABLE IF EXISTS tmp_zone_nodes;
SELECT
    zones.{zones_id_col} AS id,
    unnest(zones.node_ids) AS node_id
INTO TEMP TABLE tmp_zone_nodes
FROM
    {zones_schema}.{zones_table} zones
    tmp_tile
WHERE ST_DWithin(zones.{zones_geom_col},tmp_tile.geom,{connectivity_max_distance})
;

CREATE INDEX idx_tmp_zone_nodes_node_id ON tmp_zone_nodes (node_id);
ANALYZE tmp_zone_nodes;
