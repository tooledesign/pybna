DROP TABLE IF EXISTS {cost_to_zones};
SELECT DISTINCT ON (id)
    tmp_zone_nodes.id,
    agg_cost
INTO TEMP TABLE {cost_to_zones}
FROM
    tmp_zone_nodes,
    {distance_table} d
WHERE
    tmp_zone_nodes.node_id = d.node_id
    AND agg_cost <= {connectivity_max_distance}
ORDER BY
    tmp_zone_nodes.id,
    agg_cost ASC
;

DROP TABLE {distance_table};
