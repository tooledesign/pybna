-- evaluating all possible connections;
DROP TABLE IF EXISTS pg_temp.e;
CREATE TEMP TABLE e (
    int_id INTEGER,
    source_road_id INTEGER,
    source_road_dir TEXT,
    source_int_from INTEGER,
    source_int_to INTEGER,
    target_road_id INTEGER,
    target_road_dir TEXT,
    target_int_from INTEGER,
    target_int_to INTEGER
);

INSERT INTO pg_temp.e
SELECT
    i.int_id,
    source.road_id,
    source.one_way,
    source.intersection_from,
    source.intersection_to,
    target.road_id,
    target.one_way,
    target.intersection_from,
    target.intersection_to
FROM
    {schema}.{intersections} i,
    {schema}.{roads} source,
    {schema}.{roads} target
WHERE
    source.road_id != target.road_id
    AND i.int_id = source.intersection_from
    AND i.int_id = target.intersection_from;

INSERT INTO pg_temp.e
SELECT
    i.int_id,
    source.road_id,
    source.one_way,
    source.intersection_from,
    source.intersection_to,
    target.road_id,
    target.one_way,
    target.intersection_from,
    target.intersection_to
FROM
    {schema}.{intersections} i,
    {schema}.{roads} source,
    {schema}.{roads} target
WHERE
    source.road_id != target.road_id
    AND i.int_id = source.intersection_from
    AND i.int_id = target.intersection_to;

INSERT INTO pg_temp.e
SELECT
    i.int_id,
    source.road_id,
    source.one_way,
    source.intersection_from,
    source.intersection_to,
    target.road_id,
    target.one_way,
    target.intersection_from,
    target.intersection_to
FROM
    {schema}.{intersections} i,
    {schema}.{roads} source,
    {schema}.{roads} target
WHERE
    source.road_id != target.road_id
    AND i.int_id = source.intersection_to
    AND i.int_id = target.intersection_from;

INSERT INTO pg_temp.e
SELECT
    i.int_id,
    source.road_id,
    source.one_way,
    source.intersection_from,
    source.intersection_to,
    target.road_id,
    target.one_way,
    target.intersection_from,
    target.intersection_to
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
    {int_id},
    source_vert,
    source_road_id,
    target_vert,
    target_road_id,
    geom
)
SELECT
    e.int_id,
    source_vert.{vert_id},
    e.source_road_id,
    target_vert.{vert_id},
    e.target_road_id,
    ST_Makeline(source_vert.geom,target_vert.geom)
FROM
    pg_temp.e,
    {schema}.{nodes} source_vert,
    {schema}.{nodes} target_vert
WHERE
    e.source_road_id = source_vert.road_id
    AND e.target_road_id = target_vert.road_id
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


-- reading turns and crossings;
DROP TABLE IF EXISTS pg_temp.t;
CREATE TEMP TABLE t (
    road_id INTEGER,
    rt_id INTEGER
);

SELECT
    int_id,
    source_road_id,
    first_value(link_id)
        OVER (ORDER BY )




-- assigning stress;
