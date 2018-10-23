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
    i.{int_id},
    source.{roads_id_col},
    source.{one_way},
    ST_Azimuth(
        ST_LineInterpolatePoint(source.{roads_geom_col},0.1),
        ST_Startpoint(source.{roads_geom_col})
    ),
    source.{road_source},
    source.{road_target},
    source.{tf_seg_stress},
    source.{tf_int_stress},
    target.{roads_id_col},
    target.{one_way},
    ST_Azimuth(
        ST_Startpoint(target.{roads_geom_col}),
        ST_LineInterpolatePoint(target.{roads_geom_col},0.1)
    ),
    target.{road_source},
    target.{road_target},
    target.{ft_seg_stress}
FROM
    {roads_schema}.{intersections} i,
    {roads_schema}.{roads_table} source,
    {roads_schema}.{roads_table} target
WHERE
    source.{roads_id_col} != target.{roads_id_col}
    AND i.{int_id} = source.{road_source}
    AND i.{int_id} = target.{road_source};

INSERT INTO pg_temp.e
-- backward to backward
SELECT
    i.{int_id},
    source.{roads_id_col},
    source.{one_way},
    ST_Azimuth(
        ST_LineInterpolatePoint(source.{roads_geom_col},0.1),
        ST_Startpoint(source.{roads_geom_col})
    ),
    source.{road_source},
    source.{road_target},
    source.{tf_seg_stress},
    source.{tf_int_stress},
    target.{roads_id_col},
    target.{one_way},
    ST_Azimuth(
        ST_Endpoint(target.{roads_geom_col}),
        ST_LineInterpolatePoint(target.{roads_geom_col},0.9)
    ),
    target.{road_source},
    target.{road_target},
    target.{tf_seg_stress}
FROM
    {roads_schema}.{intersections} i,
    {roads_schema}.{roads_table} source,
    {roads_schema}.{roads_table} target
WHERE
    source.{roads_id_col} != target.{roads_id_col}
    AND i.{int_id} = source.{road_source}
    AND i.{int_id} = target.{road_target};

INSERT INTO pg_temp.e
-- forward to forward
SELECT
    i.{int_id},
    source.{roads_id_col},
    source.{one_way},
    ST_Azimuth(
        ST_LineInterpolatePoint(source.{roads_geom_col},0.9),
        ST_Endpoint(source.{roads_geom_col})
    ),
    source.{road_source},
    source.{road_target},
    source.{ft_seg_stress},
    source.{ft_int_stress},
    target.{roads_id_col},
    target.{one_way},
    ST_Azimuth(
        ST_Startpoint(target.{roads_geom_col}),
        ST_LineInterpolatePoint(target.{roads_geom_col},0.1)
    ),
    target.{road_source},
    target.{road_target},
    target.{ft_seg_stress}
FROM
    {roads_schema}.{intersections} i,
    {roads_schema}.{roads_table} source,
    {roads_schema}.{roads_table} target
WHERE
    source.{roads_id_col} != target.{roads_id_col}
    AND i.{int_id} = source.{road_target}
    AND i.{int_id} = target.{road_source};

INSERT INTO pg_temp.e
-- forward to backward
SELECT
    i.{int_id},
    source.{roads_id_col},
    source.{one_way},
    ST_Azimuth(
        ST_LineInterpolatePoint(source.{roads_geom_col},0.9),
        ST_Endpoint(source.{roads_geom_col})
    ),
    source.{road_source},
    source.{road_target},
    source.{ft_seg_stress},
    source.{ft_int_stress},
    target.{roads_id_col},
    target.{one_way},
    ST_Azimuth(
        ST_Endpoint(target.{roads_geom_col}),
        ST_LineInterpolatePoint(target.{roads_geom_col},0.9)
    ),
    target.{road_source},
    target.{road_target},
    target.{tf_seg_stress}
FROM
    {roads_schema}.{intersections} i,
    {roads_schema}.{roads_table} source,
    {roads_schema}.{roads_table} target
WHERE
    source.{roads_id_col} != target.{roads_id_col}
    AND i.{int_id} = source.{road_target}
    AND i.{int_id} = target.{road_target};


-- building network from valid connections;
INSERT INTO {roads_schema}.{edges} (
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
    {roads_schema}.{nodes} source_node,
    {roads_schema}.{nodes} target_node
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
CREATE INDEX tidx_net_build_int_id ON {roads_schema}.{edges} (int_id);
CREATE INDEX tidx_net_build_source_road_id ON {roads_schema}.{edges} (source_road_id);
CREATE INDEX tidx_net_build_target_road_id ON {roads_schema}.{edges} (target_road_id);
CREATE INDEX {edge_index} ON {roads_schema}.{edges} USING GIST (geom);
ANALYZE {roads_schema}.{edges};


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

UPDATE {roads_schema}.{edges} AS edges
SET int_crossing = FALSE
FROM t
WHERE
    edges.int_id = t.int_id
    AND edges.source_road_id = t.source_road_id
    AND edges.target_road_id = t.target_road_id;


-- assigning stress and costs;
UPDATE {roads_schema}.{edges} AS edges
SET
    link_stress = GREATEST(
        e.source_seg_stress,
        e.target_seg_stress,
        CASE WHEN int_crossing THEN e.source_int_stress ELSE 0 END
    ),
    link_cost = ROUND((ST_Length(source_road.geom) + ST_Length(target_road.geom)) / 2)
FROM
    pg_temp.e,
    {roads_schema}.{roads_table} source_road,
    {roads_schema}.{roads_table} target_road
WHERE
    edges.int_id = e.int_id
    AND edges.source_road_id = e.source_road_id
    AND edges.target_road_id = e.target_road_id
    AND edges.source_road_id = source_road.{roads_id_col}
    AND edges.target_road_id = target_road.{roads_id_col};

-- Network edges added;
