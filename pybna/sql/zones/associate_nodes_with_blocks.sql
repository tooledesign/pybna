DROP TABLE IF EXISTS tmp_blockunnest;
SELECT
    {block_id_col} AS block_id,
    unnest(road_ids) AS road_id
INTO TEMP TABLE tmp_blockunnest
FROM {blocks_schema}.{blocks_table}
;

DROP TABLE IF EXISTS tmp_block_nodes;
SELECT
    block_id,
    array_agg(nodes.{node_id}) AS node_ids
FROM
    tmp_blockunnest,
    {roads_schema}.{nodes} nodes
WHERE tmp_blockunnest.road_id = nodes.road_id
GROUP BY block_id
;
