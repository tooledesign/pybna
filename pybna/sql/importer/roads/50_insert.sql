DROP TABLE IF EXISTS tmp_combined;
CREATE TEMP TABLE tmp_combined AS (
    SELECT
        osm.id,
        osm.geom,
        osm.osmid,
        tmp_func.functional_class,
        NULL::INTEGER AS path_id,   --path_id
        tmp_oneway.oneway,
        tmp_width.width,
        tmp_speed.speed,
        CASE WHEN tmp_func.functional_class = 'path' THEN 'path' ELSE tmp_bike_infra.ft_bike_infra END,
        tmp_bike_infra.ft_bike_infra_width,
        CASE WHEN tmp_func.functional_class = 'path' THEN 'path' ELSE tmp_bike_infra.tf_bike_infra END,
        tmp_bike_infra.tf_bike_infra_width,
        tmp_ft_lanes.ft_lanes,
        tmp_tf_lanes.tf_lanes,
        NULL::INTEGER AS ft_cross_lanes, -- tmp_cross.ft_cross_lanes,
        NULL::INTEGER AS tf_cross_lanes, -- tmp_cross.tf_cross_lanes,
        NULL::INTEGER AS twltl_cross_lanes, -- tmp_cross.twltl_cross_lanes,
        tmp_park_ft.ft_park,
        tmp_park_tf.tf_park
    FROM
        {osm_ways_schema}.{osm_ways_table} osm
        JOIN tmp_func
            ON osm.id = tmp_func.id
        LEFT JOIN tmp_oneway
            ON osm.id = tmp_oneway.id
        LEFT JOIN tmp_width
            ON osm.id = tmp_width.id
        LEFT JOIN tmp_speed
            ON osm.id = tmp_speed.id
        LEFT JOIN tmp_bike_infra
            ON osm.id = tmp_bike_infra.id
        LEFT JOIN tmp_ft_lanes
            ON osm.id = tmp_ft_lanes.id
        LEFT JOIN tmp_tf_lanes
            ON osm.id = tmp_tf_lanes.id
        -- LEFT JOIN tmp_cross
            -- ON osm.id = tmp_cross.id
        LEFT JOIN tmp_park_ft
            ON osm.id = tmp_park_ft.id
        LEFT JOIN tmp_park_tf
            ON osm.id = tmp_park_tf.id
    WHERE
        NOT 'no' = ANY(         -- checks for bicycle=no tag and excludes
            regexp_split_to_array(
                trim(COALESCE(osm."bicycle",''),'{{}}'),
                ','
            )
        )
        AND CASE
                WHEN (
                    'footway' = ANY(regexp_split_to_array(trim(osm.highway,'{{}}'),','))
                    AND 'crossing' = ANY(regexp_split_to_array(trim(osm.footway,'{{}}'),','))
                    )
                    THEN (
                        ARRAY['yes','designated']
                        &&
                        regexp_split_to_array(
                            trim(COALESCE(osm."bicycle",''),'{{}}'),
                            ','
                        )
                    )
                ELSE TRUE
                END
);

INSERT INTO {roads_schema}.{roads_table}
SELECT
    id,
    geom,
    osmid,
    functional_class,
    path_id,   --path_id
    oneway,
    NULL, -- network source to be filled in later
    NULL, -- network target to be filled in later
    width,
    speed,
    ft_bike_infra,
    ft_bike_infra_width,
    tf_bike_infra,
    tf_bike_infra_width,
    ft_lanes,
    tf_lanes,
    ft_cross_lanes, -- tmp_cross.ft_cross_lanes,
    tf_cross_lanes, -- tmp_cross.tf_cross_lanes,
    twltl_cross_lanes, -- tmp_cross.twltl_cross_lanes,
    ft_park,
    tf_park,
    NULL,   --stress to be calculated later
    NULL,   --stress to be calculated later
    NULL,   --stress to be calculated later
    NULL    --stress to be calculated later
FROM tmp_combined
;

CREATE INDEX {roads_geom_idx} ON {roads_schema}.{roads_table} USING GIST ({roads_geom_col});
ANALYZE {roads_schema}.{roads_table};

DROP TABLE tmp_func;
DROP TABLE tmp_oneway;
DROP TABLE tmp_width;
DROP TABLE tmp_speed;
DROP TABLE tmp_bike_infra;
DROP TABLE tmp_ft_lanes;
DROP TABLE tmp_tf_lanes;
-- DROP TABLE tmp_cross;
DROP TABLE tmp_park_ft;
DROP TABLE tmp_park_tf;
