-- set oneway attribute based on bike infra (to accommodate contraflow, 2-way cycle tracks, etc.)
DROP TABLE IF EXISTS tmp_speed;
CREATE TEMP TABLE tmp_speed AS (
    SELECT
        osm.id,
        CASE
            WHEN osm.maxspeed LIKE '% mph'
                THEN {mi_multiplier} * substring(osm.maxspeed from '\d+')::FLOAT
            -- osm default is kph
            ELSE {km_multiplier} * substring(osm.maxspeed from '\d+')::FLOAT
            END AS speed
    FROM {osm_ways_schema}.{osm_ways_table} osm
    WHERE osm.maxspeed IS NOT NULL
);
