DROP TABLE IF EXISTS tmp_name;
CREATE TEMP TABLE tmp_name AS (
    SELECT
        osm.id,
        osm.name
    FROM
        {osm_ways_schema}.{osm_ways_table} osm
);