DROP TABLE IF EXISTS tmp_zone_node_dist;
SELECT DISTINCT ON (tmp_blockzones.zone_id, candidate_nodes.node_id)
    tmp_blockzones.zone_id,
    candidate_nodes.node_id,
    ST_Intersects(tmp_blockzones.inner_3,candidate_nodes.geom) AS inner_3,
    ST_Intersects(tmp_blockzones.inner_2,candidate_nodes.geom) AS inner_2,
    ST_Intersects(tmp_blockzones.inner_1,candidate_nodes.geom) AS inner_1,
    ST_Distance(existing_nodes.geom,candidate_nodes.geom) AS dist
INTO TEMP TABLE tmp_zone_node_dist
FROM
    tmp_blockzones,
    {zones_schema}.{zones_table} zones,
    tmp_block_nodes_unnest existing_nodes,
    tmp_block_nodes_unnest candidate_nodes
WHERE
    tmp_blockzones.block_id = ANY(zones.block_ids)
    AND existing_nodes.node_id = ANY(zones.node_ids)
    AND existing_nodes.block_id = tmp_blockzones.block_id
    AND candidate_nodes.block_id = tmp_blockzones.block_id
    AND array_length(zones.block_ids,1) > {num_blocks}
ORDER BY
    tmp_blockzones.zone_id,
    candidate_nodes.node_id,
    ST_Distance(existing_nodes.geom,candidate_nodes.geom) ASC
;

DROP TABLE IF EXISTS tmp_zone_node;
SELECT DISTINCT ON (zone_id)
    zone_id,
    node_id
INTO TEMP TABLE tmp_zone_node
FROM tmp_zone_node_dist
ORDER BY
    zone_id,
    inner_3 DESC,
    inner_2 DESC,
    inner_1 DESC,
    dist DESC
;

UPDATE {zones_schema}.{zones_table} zones
SET node_ids = tmp_zone_node.node_id || zones.node_ids
FROM tmp_zone_node
WHERE zones.{zones_id_col} = tmp_zone_node.zone_id
;
