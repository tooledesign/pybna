DROP TABLE IF EXISTS tmp_ints_all_stress;
SELECT
    i.int_id,
    GREATEST(r.tf_seg_stress,r.tf_int_stress) AS stress
INTO TEMP TABLE tmp_ints_all_stress
FROM
    neighborhood_ways_intersections i,
    neighborhood_ways r
WHERE i.int_id = r.intersection_from
UNION
SELECT
    i.int_id,
    GREATEST(r.ft_seg_stress,r.ft_int_stress)
FROM
    neighborhood_ways_intersections i,
    neighborhood_ways r
WHERE i.int_id = r.intersection_to
;

DROP TABLE IF EXISTS tmp_ints_low_stress;
SELECT int_id
INTO TEMP TABLE tmp_ints_low_stress
FROM tmp_ints_all_stress
GROUP BY int_id
HAVING MAX(stress) <= 1
;

CREATE INDEX tidx_ils ON tmp_ints_low_stress(int_id);
ANALYZE tmp_ints_low_stress;

DROP TABLE IF EXISTS tmp_block_nodes;
SELECT
    blocks.gid AS blockid,
    array_agg(nodes.vert_id) AS nodes
INTO TEMP TABLE tmp_block_nodes
FROM
    neighborhood_census_blocks blocks,
    neighborhood_ways roads,
    neighborhood_ways_net_vert nodes
WHERE
    ST_DWithin(blocks.geom,roads.geom,5)
    AND roads.road_id = nodes.road_id
    AND (
            ST_Contains(ST_Buffer(blocks.geom,5),roads.geom)
        OR  ST_Length(
                ST_Intersection(ST_Buffer(blocks.geom,5),roads.geom)
            ) > 35
        )
GROUP BY blockid
;



SELECT
    b.gid,
    b.geom
FROM
    tmp_block_nodes b,
    pgr_drivingdistance(
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
        ARRAY[19886,24856,34084,34429,34902,34905,35522,36432,38881,46134,46443,67510,67513,67514,114532],
        2000,
        directed:=FALSE
    ) shed
WHERE shed.node = ANY(b.nodes)
;





block id = 585958
"{19886,24856,34084,34429,34902,34905,35522,36432,38881,46134,46443,67510,67513,67514,114532}"
