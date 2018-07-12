-- evaluating all possible connections;
DROP TABLE IF EXISTS pg_temp.e;
CREATE TEMP TABLE e (
    int_id INTEGER,
    source_road_id INTEGER,
    source_road_dir TEXT,
    source_road_azi FLOAT,
    source_int_from INTEGER,
    source_int_to INTEGER,
    source_seg_stress INTEGER,
    source_int_stress INTEGER,
    target_road_id INTEGER,
    target_road_dir TEXT,
    target_road_azi FLOAT,
    target_int_from INTEGER,
    target_int_to INTEGER,
    target_seg_stress INTEGER
);

INSERT INTO pg_temp.e
-- backward to forward
SELECT
    i.int_id,
    source.road_id,
    source.{one_way},
    ST_Azimuth(
        ST_LineInterpolatePoint(source.geom,0.1),
        ST_Startpoint(source.geom)
    ),
    source.intersection_from,
    source.intersection_to,
    source.{tf_seg_stress},
    source.{tf_int_stress},
    target.road_id,
    target.{one_way},
    ST_Azimuth(
        ST_Startpoint(target.geom),
        ST_LineInterpolatePoint(target.geom,0.1)
    ),
    target.intersection_from,
    target.intersection_to,
    target.{ft_seg_stress}
FROM
    {schema}.{intersections} i,
    {schema}.{roads} source,
    {schema}.{roads} target
WHERE
    source.road_id != target.road_id
    AND i.int_id = source.intersection_from
    AND i.int_id = target.intersection_from;

INSERT INTO pg_temp.e
-- backward to backward
SELECT
    i.int_id,
    source.road_id,
    source.{one_way},
    ST_Azimuth(
        ST_LineInterpolatePoint(source.geom,0.1),
        ST_Startpoint(source.geom)
    ),
    source.intersection_from,
    source.intersection_to,
    source.{tf_seg_stress},
    source.{tf_int_stress},
    target.road_id,
    target.{one_way},
    ST_Azimuth(
        ST_Endpoint(target.geom),
        ST_LineInterpolatePoint(target.geom,0.9)
    ),
    target.intersection_from,
    target.intersection_to,
    target.{tf_seg_stress}
FROM
    {schema}.{intersections} i,
    {schema}.{roads} source,
    {schema}.{roads} target
WHERE
    source.road_id != target.road_id
    AND i.int_id = source.intersection_from
    AND i.int_id = target.intersection_to;

INSERT INTO pg_temp.e
-- forward to forward
SELECT
    i.int_id,
    source.road_id,
    source.{one_way},
    ST_Azimuth(
        ST_LineInterpolatePoint(source.geom,0.9),
        ST_Endpoint(source.geom)
    ),
    source.intersection_from,
    source.intersection_to,
    source.{ft_seg_stress},
    source.{ft_int_stress},
    target.road_id,
    target.{one_way},
    ST_Azimuth(
        ST_Startpoint(target.geom),
        ST_LineInterpolatePoint(target.geom,0.1)
    ),
    target.intersection_from,
    target.intersection_to,
    target.{ft_seg_stress}
FROM
    {schema}.{intersections} i,
    {schema}.{roads} source,
    {schema}.{roads} target
WHERE
    source.road_id != target.road_id
    AND i.int_id = source.intersection_to
    AND i.int_id = target.intersection_from;

INSERT INTO pg_temp.e
-- forward to backward
SELECT
    i.int_id,
    source.road_id,
    source.{one_way},
    ST_Azimuth(
        ST_LineInterpolatePoint(source.geom,0.9),
        ST_Endpoint(source.geom)
    ),
    source.intersection_from,
    source.intersection_to,
    source.{ft_seg_stress},
    source.{ft_int_stress},
    target.road_id,
    target.{one_way},
    ST_Azimuth(
        ST_Endpoint(target.geom),
        ST_LineInterpolatePoint(target.geom,0.9)
    ),
    target.intersection_from,
    target.intersection_to,
    target.{tf_seg_stress}
FROM
    {schema}.{intersections} i,
    {schema}.{roads} source,
    {schema}.{roads} target
WHERE
    source.road_id != target.road_id
    AND i.int_id = source.intersection_to
    AND i.int_id = target.intersection_to;


-- building network from valid connections;
INSERT INTO {schema}.{edges} (
    int_id,
    source_vert,
    source_road_id,
    target_vert,
    target_road_id,
    int_crossing,
    geom
)
SELECT
    e.int_id,
    source_node.{node_id},
    e.source_road_id,
    target_node.{node_id},
    e.target_road_id,
    TRUE,   -- assume this movement crosses traffic until we prove otherwise
    ST_Makeline(source_node.geom,target_node.geom)
FROM
    pg_temp.e,
    {schema}.{nodes} source_node,
    {schema}.{nodes} target_node
WHERE
    e.source_road_id = source_node.road_id
    AND e.target_road_id = target_node.road_id
    AND (
        e.source_road_dir IS NULL
        OR (e.source_road_dir = {forward} AND e.int_id = e.source_int_to)
        OR (e.source_road_dir = {backward} AND e.int_id = e.source_int_from)
    )
    AND (
        e.target_road_dir IS NULL
        OR (e.target_road_dir = {forward} AND e.int_id = e.target_int_from)
        OR (e.target_road_dir = {backward} AND e.int_id = e.target_int_to)
    );


-- creating indexes;
CREATE INDEX tidx_net_build_int_id ON {schema}.{edges} (int_id);
CREATE INDEX tidx_net_build_source_road_id ON {schema}.{edges} (source_road_id);
CREATE INDEX tidx_net_build_target_road_id ON {schema}.{edges} (target_road_id);
CREATE INDEX {edge_index} ON {schema}.{edges} USING GIST (geom);
ANALYZE {schema}.{edges};


-- reading turns and crossings;
DROP TABLE IF EXISTS pg_temp.t;
SELECT DISTINCT ON (int_id, source_road_id)
    int_id,
    source_road_id,
    target_road_id
INTO TEMP TABLE t
FROM pg_temp.e
ORDER BY
    int_id,
    source_road_id,
    degrees(5*pi() + source_road_azi - target_road_azi)::INT%360 ASC;

UPDATE {schema}.{edges} AS edges
SET int_crossing = FALSE
FROM t
WHERE
    edges.int_id = t.int_id
    AND edges.source_road_id = t.source_road_id
    AND edges.target_road_id = t.target_road_id;


-- assigning stress and costs;
UPDATE {schema}.{edges} AS edges
SET
    link_stress = GREATEST(
        e.source_seg_stress,
        e.target_seg_stress,
        CASE WHEN int_crossing THEN e.source_int_stress ELSE 0 END
    ),
    link_cost = ROUND((ST_Length(source_road.geom) + ST_Length(target_road.geom)) / 2)
FROM
    pg_temp.e,
    {schema}.{roads} source_road,
    {schema}.{roads} target_road
WHERE
    edges.int_id = e.int_id
    AND edges.source_road_id = e.source_road_id
    AND edges.target_road_id = e.target_road_id
    AND edges.source_road_id = source_road.{road_id}
    AND edges.target_road_id = target_road.{road_id};

-- Network edges added;
