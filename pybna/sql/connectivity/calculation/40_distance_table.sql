DROP TABLE IF EXISTS {distance_table};
SELECT
    route.node AS node_id,
    route.agg_cost
INTO TEMP TABLE {distance_table}
FROM pgr_drivingdistance(
        'SELECT * FROM {net_table}',
        {node_ids}::INTEGER[],
        {connectivity_max_distance},
        equicost:=TRUE,
        directed:=TRUE
    ) route
;
