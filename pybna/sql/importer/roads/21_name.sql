DROP TABLE IF EXISTS tmp_name;
CREATE TEMP TABLE tmp_name AS (
    SELECT
        osm.id,
        ('{{' || trim(both '{{' from trim(both '}}' from osm.name)) || '}}')::TEXT[] AS name
    FROM
        {osm_ways_schema}.{osm_ways_table} osm
    WHERE
        osm.name != 'NaN'
);