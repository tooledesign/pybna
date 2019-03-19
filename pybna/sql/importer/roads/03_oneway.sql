DROP TABLE IF EXISTS tmp_oneway;
CREATE TEMP TABLE tmp_oneway AS (
SELECT
    osm.id,

FROM {osm_ways_schema}.{osm_ways_table} osm
);
