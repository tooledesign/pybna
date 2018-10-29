DROP TABLE IF EXISTS tmp_zone_node;
SELECT DISTINCT ON (tmp_blockzones.zone_id)
    tmp_blockzones.zone_id,
    tmp_block_nodes_unnest.node_id
INTO TEMP TABLE tmp_zone_node
FROM
    tmp_blockzones,
    tmp_block_nodes_unnest
WHERE tmp_block_nodes_unnest.block_id = tmp_blockzones.block_id
ORDER BY
    tmp_blockzones.zone_id,
    ST_Distance(tmp_blockzones.centroid,tmp_block_nodes_unnest.geom)
;

UPDATE {zones_schema}.{zones_table} zones
SET node_ids = ARRAY[tmp_zone_node.node_id]
FROM tmp_zone_node
WHERE zones.{zones_id_col} = tmp_zone_node.zone_id
;
