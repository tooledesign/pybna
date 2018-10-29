DROP TABLE IF EXISTS tmp_blockzones;
SELECT
    {zones_id_col} AS zone_id,
    unnest(block_ids) AS block_id,
    ST_Centroid({zones_geom_col}) AS centroid,
    ST_Buffer({zones_geom_col},(-1 * {blocks_roads_tolerance})) AS inner_1, --roads_tolerance
    ST_Buffer({zones_geom_col},(-3 * {blocks_roads_tolerance})) AS inner_2,
    ST_Buffer({zones_geom_col},(-6 * {blocks_roads_tolerance})) AS inner_3
INTO TEMP TABLE tmp_blockzones
FROM {zones_schema}.{zones_table}
WHERE array_length(block_ids,1) > 1
;
CREATE INDEX tidx_tmp_blockzones_block_id ON tmp_blockzones (block_id);
CREATE INDEX tsidx_tmp_blockzones_centroid ON tmp_blockzones USING GIST (centroid);
CREATE INDEX tsidx_tmp_blockzones_inner_1 ON tmp_blockzones USING GIST (inner_1);
CREATE INDEX tsidx_tmp_blockzones_inner_2 ON tmp_blockzones USING GIST (inner_2);
CREATE INDEX tsidx_tmp_blockzones_inner_3 ON tmp_blockzones USING GIST (inner_3);
ANALYZE tmp_blockzones;

DROP TABLE IF EXISTS tmp_blockunnest;
SELECT
    {blocks_id_col} AS block_id,
    unnest(road_ids) AS road_id
INTO TEMP TABLE tmp_blockunnest
FROM {blocks_schema}.{blocks_table}
;

DROP TABLE IF EXISTS tmp_block_nodes_unnest;
SELECT
    tmp_blockunnest.block_id,
    nodes.{nodes_id_col} AS node_id,
    nodes.{nodes_geom_col} AS geom
INTO TEMP TABLE tmp_block_nodes_unnest
FROM
    tmp_blockunnest,
    {nodes_schema}.{nodes_table} nodes
WHERE tmp_blockunnest.road_id = nodes.road_id
;
CREATE INDEX tidx_block_nodes_unnest_block_id ON tmp_block_nodes_unnest (block_id);
CREATE INDEX tidx_block_nodes_unnest_node_id ON tmp_block_nodes_unnest (node_id);
CREATE INDEX tsidx_block_nodes_unnest ON tmp_block_nodes_unnest USING GIST (geom);
ANALYZE tmp_block_nodes_unnest;

DROP TABLE IF EXISTS tmp_block_nodes;
SELECT
    tmp_blockunnest.block_id,
    array_agg(nodes.{nodes_id_col}) AS node_ids
INTO TEMP TABLE tmp_block_nodes
FROM
    tmp_blockunnest,
    {nodes_schema}.{nodes_table} nodes
WHERE tmp_blockunnest.road_id = nodes.road_id
GROUP BY tmp_blockunnest.block_id
;

UPDATE {zones_schema}.{zones_table} zones
SET node_ids = tmp_block_nodes.node_ids
FROM tmp_block_nodes
WHERE
    array_length(zones.block_ids,1) = 1
    AND tmp_block_nodes.block_id = ANY(zones.block_ids)
;
