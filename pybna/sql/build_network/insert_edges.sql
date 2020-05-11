-- evaluating all possible connections
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
    i.{ints_id_col},
    source.{roads_id_col},
    source.{roads_oneway_col},
    ST_Azimuth(
        ST_LineInterpolatePoint(source.{roads_geom_col},0.1),
        ST_Startpoint(source.{roads_geom_col})
    ),
    source.{roads_source_col},
    source.{roads_target_col},
    source.{roads_stress_seg_bwd},
    source.{roads_stress_cross_bwd},
    target.{roads_id_col},
    target.{roads_oneway_col},
    ST_Azimuth(
        ST_Startpoint(target.{roads_geom_col}),
        ST_LineInterpolatePoint(target.{roads_geom_col},0.1)
    ),
    target.{roads_source_col},
    target.{roads_target_col},
    target.{roads_stress_seg_fwd}
FROM
    {ints_schema}.{ints_table} i,
    {roads_schema}.{roads_table} source,
    {roads_schema}.{roads_table} target
WHERE
    source.{roads_id_col} != target.{roads_id_col}
    AND i.{ints_id_col} = source.{roads_source_col}
    AND i.{ints_id_col} = target.{roads_source_col};

INSERT INTO pg_temp.e
-- backward to backward
SELECT
    i.{ints_id_col},
    source.{roads_id_col},
    source.{roads_oneway_col},
    ST_Azimuth(
        ST_LineInterpolatePoint(source.{roads_geom_col},0.1),
        ST_Startpoint(source.{roads_geom_col})
    ),
    source.{roads_source_col},
    source.{roads_target_col},
    source.{roads_stress_seg_bwd},
    source.{roads_stress_cross_bwd},
    target.{roads_id_col},
    target.{roads_oneway_col},
    ST_Azimuth(
        ST_Endpoint(target.{roads_geom_col}),
        ST_LineInterpolatePoint(target.{roads_geom_col},0.9)
    ),
    target.{roads_source_col},
    target.{roads_target_col},
    target.{roads_stress_seg_bwd}
FROM
    {ints_schema}.{ints_table} i,
    {roads_schema}.{roads_table} source,
    {roads_schema}.{roads_table} target
WHERE
    source.{roads_id_col} != target.{roads_id_col}
    AND i.{ints_id_col} = source.{roads_source_col}
    AND i.{ints_id_col} = target.{roads_target_col};

INSERT INTO pg_temp.e
-- forward to forward
SELECT
    i.{ints_id_col},
    source.{roads_id_col},
    source.{roads_oneway_col},
    ST_Azimuth(
        ST_LineInterpolatePoint(source.{roads_geom_col},0.9),
        ST_Endpoint(source.{roads_geom_col})
    ),
    source.{roads_source_col},
    source.{roads_target_col},
    source.{roads_stress_seg_fwd},
    source.{roads_stress_cross_fwd},
    target.{roads_id_col},
    target.{roads_oneway_col},
    ST_Azimuth(
        ST_Startpoint(target.{roads_geom_col}),
        ST_LineInterpolatePoint(target.{roads_geom_col},0.1)
    ),
    target.{roads_source_col},
    target.{roads_target_col},
    target.{roads_stress_seg_fwd}
FROM
    {ints_schema}.{ints_table} i,
    {roads_schema}.{roads_table} source,
    {roads_schema}.{roads_table} target
WHERE
    source.{roads_id_col} != target.{roads_id_col}
    AND i.{ints_id_col} = source.{roads_target_col}
    AND i.{ints_id_col} = target.{roads_source_col};

INSERT INTO pg_temp.e
-- forward to backward
SELECT
    i.{ints_id_col},
    source.{roads_id_col},
    source.{roads_oneway_col},
    ST_Azimuth(
        ST_LineInterpolatePoint(source.{roads_geom_col},0.9),
        ST_Endpoint(source.{roads_geom_col})
    ),
    source.{roads_source_col},
    source.{roads_target_col},
    source.{roads_stress_seg_fwd},
    source.{roads_stress_cross_fwd},
    target.{roads_id_col},
    target.{roads_oneway_col},
    ST_Azimuth(
        ST_Endpoint(target.{roads_geom_col}),
        ST_LineInterpolatePoint(target.{roads_geom_col},0.9)
    ),
    target.{roads_source_col},
    target.{roads_target_col},
    target.{roads_stress_seg_bwd}
FROM
    {ints_schema}.{ints_table} i,
    {roads_schema}.{roads_table} source,
    {roads_schema}.{roads_table} target
WHERE
    source.{roads_id_col} != target.{roads_id_col}
    AND i.{ints_id_col} = source.{roads_target_col}
    AND i.{ints_id_col} = target.{roads_target_col};



-- building network from valid connections
INSERT INTO {edges_schema}.{edges_table} (
    {ints_id_col},
    {edges_source_col},
    source_road_id,
    {edges_target_col},
    target_road_id,
    int_crossing,
    {edges_geom_col}
)
SELECT
    e.int_id,
    source_node.{nodes_id_col},
    e.source_road_id,
    target_node.{nodes_id_col},
    e.target_road_id,
    TRUE,   -- assume this movement crosses traffic until we prove otherwise
    ST_Makeline(source_node.{nodes_geom_col},target_node.{nodes_geom_col})
FROM
    pg_temp.e,
    {nodes_schema}.{nodes_table} source_node,
    {nodes_schema}.{nodes_table} target_node
WHERE
    e.source_road_id = source_node.road_id
    AND e.target_road_id = target_node.road_id
    AND (
        e.source_road_dir IS NULL
        OR e.source_road_dir NOT IN ({roads_oneway_fwd},{roads_oneway_bwd})
        OR (e.source_road_dir = {roads_oneway_fwd} AND e.int_id = e.source_int_to)
        OR (e.source_road_dir = {roads_oneway_bwd} AND e.int_id = e.source_int_from)
    )
    AND (
        e.target_road_dir IS NULL
        OR e.source_road_dir NOT IN ({roads_oneway_fwd},{roads_oneway_bwd})
        OR (e.target_road_dir = {roads_oneway_fwd} AND e.int_id = e.target_int_from)
        OR (e.target_road_dir = {roads_oneway_bwd} AND e.int_id = e.target_int_to)
    );


-- creating indexes
CREATE INDEX tidx_net_build_int_id ON {edges_schema}.{edges_table} ({ints_id_col});
CREATE INDEX tidx_net_build_source_road_id ON {edges_schema}.{edges_table} (source_road_id);
CREATE INDEX tidx_net_build_target_road_id ON {edges_schema}.{edges_table} (target_road_id);
CREATE INDEX {edges_index} ON {edges_schema}.{edges_table} USING GIST ({edges_geom_col});
ANALYZE {edges_schema}.{edges_table};


-- reading turns and crossings
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

UPDATE {edges_schema}.{edges_table} AS edges
SET int_crossing = FALSE
FROM t
WHERE
    edges.{ints_id_col} = t.int_id
    AND edges.source_road_id = t.source_road_id
    AND edges.target_road_id = t.target_road_id;


-- assigning stress and costs
UPDATE {edges_schema}.{edges_table} AS edges
SET
    {edges_stress_col} =
        CASE
            WHEN e.source_seg_stress < 0 THEN e.source_seg_stress
            WHEN e.target_seg_stress < 0 THEN e.target_seg_stress
            ELSE GREATEST(
                e.source_seg_stress,
                e.target_seg_stress,
                CASE WHEN int_crossing THEN e.source_int_stress ELSE 0 END
            )
            END,
    {edges_cost_col} = ROUND((ST_Length(source_road.{roads_geom_col}) + ST_Length(target_road.{roads_geom_col})) / 2)
FROM
    pg_temp.e,
    {roads_schema}.{roads_table} source_road,
    {roads_schema}.{roads_table} target_road
WHERE
    edges.{ints_id_col} = e.int_id
    AND edges.source_road_id = e.source_road_id
    AND edges.target_road_id = e.target_road_id
    AND edges.source_road_id = source_road.{roads_id_col}
    AND edges.target_road_id = target_road.{roads_id_col};
