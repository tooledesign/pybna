DROP TABLE IF EXISTS {cost_to_units};
SELECT DISTINCT ON (tmp_unit_nodes.id)
    tmp_unit_nodes.id,
    agg_cost
INTO TEMP TABLE {cost_to_units}
FROM
    tmp_unit_nodes,
    {distance_table} d
WHERE
    tmp_unit_nodes.node_id = d.node_id
    AND agg_cost <= {connectivity_max_distance}
ORDER BY
    tmp_unit_nodes.id,
    agg_cost ASC
;

DROP TABLE {distance_table};
