DROP TABLE IF EXISTS tmp_ref;
CREATE TEMP TABLE tmp_ref AS (
    SELECT
        osm.id,
        ('{{' || trim(both '{{' from trim(both '}}' from osm.ref)) || '}}')::TEXT[] AS ref
    FROM
        {osm_ways_schema}.{osm_ways_table} osm
    WHERE
        osm.ref != 'NaN'
);