-- takes a list of source nodes, dumps matching blocks into tmp_matches,
-- and adds the aggregation of blocks into tmp_zones
DROP TABLE IF EXISTS tmp_routes;
SELECT *
INTO TEMP TABLE tmp_routes
FROM pgr_drivingdistance(
        'SELECT
            link.link_id AS id,
            source_vert AS source,
            target_vert AS target,
            link_cost AS cost
        FROM
            neighborhood_ways_net_link link,
            tmp_ints_low_stress
        WHERE
            link.int_id = tmp_ints_low_stress.int_id',
        {source_nodes},
        {max_trip_distance},
        directed:=FALSE
    ) shed
;

DROP TABLE IF EXISTS tmp_matches;
SELECT DISTINCT
    b.{block_id_col} AS block_id,
    b.{block_geom_col} AS geom
INTO TEMP TABLE tmp_matches
FROM
    tmp_block_nodes b,
    {roads_schema}.{nodes} nodes,
    tmp_routes
WHERE tmp_routes.node = ANY(b.nodes)
;

INSERT INTO tmp_zones
SELECT
    array_agg(block_id),
    ST_Union(geom)
FROM tmp_matches
;

DROP TABLE IF EXISTS tmp_routes;
DROP TABLE IF EXISTS tmp_matches;
