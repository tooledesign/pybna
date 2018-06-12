-- two-way to two-way
INSERT INTO {schema}.{edges} (
    {int_id},
    source_vert,
    source_road_id,
    target_vert,
    target_road_id,
    geom
)
SELECT
    ints.{int_id},
    vert1.{node_id},
    vert2.{node_id},
    ST_Makeline(vert1.geom,vert2.geom)
FROM
    {schema}.{intersections} ints,
    {schema}.{nodes} vert1,
    {schema}.{roads} roads1,
    {schema}.{nodes} vert2,
    {schema}.{roads} roads2
WHERE
    vert1.road_id = roads1.{road_id}
    AND vert2.road_id = roads2.{road_id}
    AND ints.{int_id} IN (roads1.{road_source}, roads1.{road_target})
    AND ints.{int_id} IN (roads2.{road_source}, roads2.{road_target})
    AND roads1.{one_way} IS NULL
    AND roads2.{one_way} IS NULL
    AND roads1.{road_id} != roads2.{road_id};

-- two-way to from-to
INSERT INTO {schema}.{edges} (
    {int_id},
    source_vert,
    source_road_id,
    target_vert,
    target_road_id,
    geom
)
SELECT
    ints.{int_id},
    vert1.{node_id},
    vert2.{node_id},
    ST_Makeline(vert1.geom,vert2.geom)
FROM
    {schema}.{intersections} ints,
    {schema}.{nodes} vert1,
    {schema}.{roads} roads1,
    {schema}.{nodes} vert2,
    {schema}.{roads} roads2
WHERE
    vert1.road_id = roads1.{road_id}
    AND vert2.road_id = roads2.{road_id}
    AND ints.{int_id} IN (roads1.{road_source}, roads1.{road_target})
    AND ints.{int_id} = roads2.{road_source}
    AND roads1.{one_way} IS NULL
    AND roads2.{one_way} = {forward}
    AND roads1.{road_id} != roads2.{road_id};

-- two-way to to-from
INSERT INTO {schema}.{edges} (
    {int_id},
    source_vert,
    source_road_id,
    target_vert,
    target_road_id,
    geom
)
SELECT
    ints.{int_id},
    vert1.{node_id},
    vert2.{node_id},
    ST_Makeline(vert1.geom,vert2.geom)
FROM
    {schema}.{intersections} ints,
    {schema}.{nodes} vert1,
    {schema}.{roads} roads1,
    {schema}.{nodes} vert2,
    {schema}.{roads} roads2
WHERE
    vert1.road_id = roads1.{road_id}
    AND vert2.road_id = roads2.{road_id}
    AND ints.{int_id} IN (roads1.{road_source}, roads1.{road_target})
    AND ints.{int_id} = roads2.{road_target}
    AND roads1.{one_way} IS NULL
    AND roads2.{one_way} = {backward}
    AND roads1.{road_id} != roads2.{road_id};

-- from-to to two-way
INSERT INTO {schema}.{edges} (
    {int_id},
    source_vert,
    source_road_id,
    target_vert,
    target_road_id,
    geom
)
SELECT
    ints.{int_id},
    vert1.{node_id},
    vert2.{node_id},
    ST_Makeline(vert1.geom,vert2.geom)
FROM
    {schema}.{intersections} ints,
    {schema}.{nodes} vert1,
    {schema}.{roads} roads1,
    {schema}.{nodes} vert2,
    {schema}.{roads} roads2
WHERE
    vert1.road_id = roads1.{road_id}
    AND vert2.road_id = roads2.{road_id}
    AND ints.{int_id} = roads1.{road_target}
    AND ints.{int_id} IN (roads2.{road_source}, roads2.{road_target})
    AND roads1.{one_way} = {forward}
    AND roads2.{one_way} IS NULL
    AND roads1.{road_id} != roads2.{road_id};

-- from-to to from-to
INSERT INTO {schema}.{edges} (
    {int_id},
    source_vert,
    source_road_id,
    target_vert,
    target_road_id,
    geom
)
SELECT
    ints.{int_id},
    vert1.{node_id},
    vert2.{node_id},
    ST_Makeline(vert1.geom,vert2.geom)
FROM
    {schema}.{intersections} ints,
    {schema}.{nodes} vert1,
    {schema}.{roads} roads1,
    {schema}.{nodes} vert2,
    {schema}.{roads} roads2
WHERE
    vert1.road_id = roads1.{road_id}
    AND vert2.road_id = roads2.{road_id}
    AND ints.{int_id} = roads1.{road_target}
    AND ints.{int_id} = roads2.{road_source}
    AND roads1.{one_way} = {forward}
    AND roads2.{one_way} = {forward}
    AND roads1.{road_id} != roads2.{road_id};

-- from-to to to-from
INSERT INTO {schema}.{edges} (
    {int_id},
    source_vert,
    source_road_id,
    target_vert,
    target_road_id,
    geom
)
SELECT
    ints.{int_id},
    vert1.{node_id},
    vert2.{node_id},
    ST_Makeline(vert1.geom,vert2.geom)
FROM
    {schema}.{intersections} ints,
    {schema}.{nodes} vert1,
    {schema}.{roads} roads1,
    {schema}.{nodes} vert2,
    {schema}.{roads} roads2
WHERE
    vert1.road_id = roads1.{road_id}
    AND vert2.road_id = roads2.{road_id}
    AND ints.{int_id} = roads1.{road_target}
    AND ints.{int_id} = roads2.{road_target}
    AND roads1.{one_way} = {forward}
    AND roads2.{one_way} = {backward}
    AND roads1.{road_id} != roads2.{road_id};

-- to-from to two-way
INSERT INTO {schema}.{edges} (
    {int_id},
    source_vert,
    source_road_id,
    target_vert,
    target_road_id,
    geom
)
SELECT
    ints.{int_id},
    vert1.{node_id},
    vert2.{node_id},
    ST_Makeline(vert1.geom,vert2.geom)
FROM
    {schema}.{intersections} ints,
    {schema}.{nodes} vert1,
    {schema}.{roads} roads1,
    {schema}.{nodes} vert2,
    {schema}.{roads} roads2
WHERE
    vert1.road_id = roads1.{road_id}
    AND vert2.road_id = roads2.{road_id}
    AND ints.{int_id} = roads1.{road_source}
    AND ints.{int_id} IN (roads2.{road_source}, roads2.{road_target})
    AND roads1.{one_way} = {backward}
    AND roads2.{one_way} IS NULL
    AND roads1.{road_id} != roads2.{road_id};

-- to-from to to-from
INSERT INTO {schema}.{edges} (
    {int_id},
    source_vert,
    source_road_id,
    target_vert,
    target_road_id,
    geom
)
SELECT
    ints.{int_id},
    vert1.{node_id},
    vert2.{node_id},
    ST_Makeline(vert1.geom,vert2.geom)
FROM
    {schema}.{intersections} ints,
    {schema}.{nodes} vert1,
    {schema}.{roads} roads1,
    {schema}.{nodes} vert2,
    {schema}.{roads} roads2
WHERE
    vert1.road_id = roads1.{road_id}
AND vert2.road_id = roads2.{road_id}
AND ints.{int_id} = roads1.{road_source}
AND ints.{int_id} = roads2.{road_target}
AND roads1.{one_way} = {backward}
AND roads2.{one_way} = {backward}
AND roads1.{road_id} != roads2.{road_id};

-- to-from to from-to
INSERT INTO {schema}.{edges} (
    {int_id},
    source_vert,
    source_road_id,
    target_vert,
    target_road_id,
    geom
)
SELECT
    ints.{int_id},
    vert1.{node_id},
    vert2.{node_id},
    ST_Makeline(vert1.geom,vert2.geom)
FROM
    {schema}.{intersections} ints,
    {schema}.{nodes} vert1,
    {schema}.{roads} roads1,
    {schema}.{nodes} vert2,
    {schema}.{roads} roads2
WHERE
    vert1.road_id = roads1.{road_id}
    AND vert2.road_id = roads2.{road_id}
    AND ints.{int_id} = roads1.{road_source}
    AND ints.{int_id} = roads2.{road_source}
    AND roads1.{one_way} = {backward}
    AND roads2.{one_way} = {forward}
    AND roads1.{road_id} != roads2.{road_id};s
