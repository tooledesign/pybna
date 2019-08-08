DROP TABLE IF EXISTS {cost_to_blocks};
SELECT DISTINCT ON (tmp_blocks_nodes.id)
    tmp_blocks_nodes.id::{blocks_id_type},
    agg_cost
INTO TEMP TABLE {cost_to_blocks}
FROM
    tmp_blocks_nodes,
    {distance_table} d
WHERE
    tmp_blocks_nodes.node_id = d.node_id
    AND agg_cost <= {connectivity_max_distance}
ORDER BY
    tmp_blocks_nodes.id::{blocks_id_type},
    agg_cost ASC
;

DROP TABLE {distance_table};
