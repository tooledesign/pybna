DROP TABLE IF EXISTS {cost_table};
SELECT
    route.node,
    route.agg_cost
INTO TEMP TABLE {cost_table}
FROM pgr_drivingdistance(
        'SELECT * FROM {net_table}',
        {node_ids},
        {connectivity_max_distance},
        equicost:=TRUE,
        directed:=TRUE
    ) route
;
