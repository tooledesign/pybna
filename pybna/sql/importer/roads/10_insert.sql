DROP TABLE IF EXISTS tmp_combined;
CREATE TEMP TABLE tmp_combined AS (
    SELECT
        osm.id,
        osm.geom,
        osm.osmid,
        osm.highway,
        NULL,   --path_id
        tmp_oneway.oneway,
        tmp_width.width,
        tmp_speed.speed,
        tmp_bike_infra.ft_bike_infra,
        tmp_bike_infra.ft_bike_infra_width,
        tmp_bike_infra.tf_bike_infra,
        tmp_bike_infra.tf_bike_infra_width,
        tmp_lanes.ft_lanes,
        tmp_lanes.tf_lanes,
        tmp_cross.ft_cross_lanes,
        tmp_cross.tf_cross_lanes,
        tmp_cross.twltl_cross_lanes,
        tmp_park.ft_park,
        tmp_park.tf_park
    FROM
        {osm_ways_schema}.{osm_ways_table} osm
        LEFT JOIN tmp_oneway
            ON osm.id = tmp_oneway.id
        LEFT JOIN tmp_width
            ON osm.id = tmp_width.id
        LEFT JOIN tmp_speed
            ON osm.id = tmp_speed.id
        LEFT JOIN tmp_bike_infra
            ON osm.id = tmp_bike_infra.id
        LEFT JOIN tmp_lanes
            ON osm.id = tmp_lanes.id
        LEFT JOIN tmp_cross
            ON osm.id = tmp_cross.id
        LEFT JOIN tmp_park
            ON osm.id = tmp_park.id
    -- any where conditions? maybe service roads?
);

INSERT INTO {roads_schema}.{roads_table}
SELECT
    *,
    NULL,   --stress to be calculated later
    NULL,   --stress to be calculated later
    NULL,   --stress to be calculated later
    NULL    --stress to be calculated later
FROM tmp_combined
;

CREATE INDEX {roads_geom_idx} ON {roads_schema}.{roads_table} USING GIST ({roads_geom_col});
ANALYZE {roads_schema}.{roads_table};
