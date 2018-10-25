DROP TABLE IF EXISTS tmp_blockunnest;
SELECT
    {blocks_id_col} AS block_id,
    {blocks_geom_col} AS geom,
    unnest(road_ids) AS road_id
INTO TEMP TABLE tmp_blockunnest
FROM {blocks_schema}.{blocks_table}
;

DROP TABLE IF EXISTS tmp_block_nodes;
SELECT
    tmp_blockunnest.block_id,
    tmp_blockunnest.geom,
    array_agg(nodes.{nodes_id_col}) AS node_ids
INTO TEMP TABLE tmp_block_nodes
FROM
    tmp_blockunnest,
    {nodes_schema}.{nodes_table} nodes
WHERE tmp_blockunnest.road_id = nodes.road_id
GROUP BY
    tmp_blockunnest.block_id,
    tmp_blockunnest.geom
;
