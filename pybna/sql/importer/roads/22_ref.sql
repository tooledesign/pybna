DROP TABLE IF EXISTS tmp_ref;
CREATE TEMP TABLE tmp_ref AS (
    SELECT
        osm.id,
        osm.ref
    FROM
        {osm_ways_schema}.{osm_ways_table} osm
);