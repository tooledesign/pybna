-- set oneway attribute based on bike infra (to accommodate contraflow, 2-way cycle tracks, etc.)
DROP TABLE IF EXISTS tmp_oneway;
CREATE TEMP TABLE tmp_oneway AS (
    SELECT
        osm.id,
        CASE
            WHEN one_way_car = 'ft'
                AND tf_bike_infra IS NULL
                AND (
                    osm."oneway:bicycle" IS NULL
                    OR osm."oneway:bicycle" != 'no'
                )
                THEN {roads_oneway_fwd}
            WHEN one_way_car = 'tf'
                AND ft_bike_infra IS NULL
                AND (
                    osm."oneway:bicycle" IS NULL
                    OR osm."oneway:bicycle" != 'no'
                )
                THEN {roads_oneway_bwd}
            END AS oneway
    FROM
        {osm_ways_schema}.{osm_ways_table} osm
        LEFT JOIN tmp_bike_infra
            ON osm.id = tmp_bike_infra.id
    WHERE one_way_car IS NOT NULL
);
