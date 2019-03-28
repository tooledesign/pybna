DROP TABLE IF EXISTS tmp_unnest;
CREATE TEMP TABLE tmp_unnest AS (
    SELECT
        osm.id,
        maxspeed.*
    FROM
        {osm_ways_schema}.{osm_ways_table} osm,
        unnest(('{{' || trim(both '{{' from trim(both '}}' from COALESCE(osm.maxspeed,'{NaN}'))) || '}}')::TEXT[]) maxspeed
);

DROP TABLE IF EXISTS tmp_raw_speed;
CREATE TEMP TABLE tmp_raw_speed AS (
    SELECT
        osm.id,
        CASE
            WHEN osm.maxspeed LIKE '% mph'
                THEN {mi_multiplier} * substring(osm.maxspeed from '\d+')::FLOAT
            -- osm default is kph
            ELSE {km_multiplier} * substring(osm.maxspeed from '\d+')::FLOAT
            END AS speed
    FROM tmp_unnest osm
    WHERE osm.maxspeed != 'NaN'
);

DROP TABLE IF EXISTS tmp_speed;
CREATE TEMP TABLE tmp_speed AS (
    SELECT DISTINCT ON (id)
        id,
        speed
    FROM tmp_raw_speed
    ORDER BY
        id,
        speed DESC
);

DROP TABLE tmp_unnest;
DROP TABLE tmp_raw_speed;
