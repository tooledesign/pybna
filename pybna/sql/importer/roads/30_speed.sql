DROP TABLE IF EXISTS tmp_unnest;
CREATE TEMP TABLE tmp_unnest AS (
    SELECT
        osm.id,
        maxspeed.*
    FROM
        {osm_ways_schema}.{osm_ways_table} osm,
        unnest(('{{' || trim(both '{{' from trim(both '}}' from COALESCE(osm.maxspeed,'{{NaN}}'))) || '}}')::TEXT[]) maxspeed
);

DROP TABLE IF EXISTS tmp_speed_mph_to_km;
CREATE TEMP TABLE tmp_speed_mph_to_km AS (
    SELECT *
    FROM (
        VALUES
            (10,20),
            (15,25),
            (20,30),
            (25,40),
            (30,50),
            (35,55),
            (40,65),
            (45,70),
            (50,80),
            (55,90),
            (60,100),
            (65,105),
            (70,110),
            (75,120),
            (80,130),
            (85,140)
    ) AS t (mph, km)
);

DROP TABLE IF EXISTS tmp_speed_km_to_mph;
CREATE TEMP TABLE tmp_speed_km_to_mph AS (
    SELECT *
    FROM (
        VALUES
            (20,10),
            (25,15),
            (30,20),
            (40,25),
            (50,30),
            (55,35),
            (65,40),
            (70,45),
            (80,50),
            (90,55),
            (100,60),
            (105,65),
            (110,70),
            (120,75),
            (130,80),
            (140,85)
    ) AS t (km, mph)
);

DROP TABLE IF EXISTS tmp_raw_speed;
CREATE TEMP TABLE tmp_raw_speed AS (
    SELECT
        osm.id,
        osm.maxspeed,
        (osm.maxspeed LIKE '%mph%')::BOOLEAN AS mph,
        substring(osm.maxspeed from '\d+')::INTEGER AS speed
    FROM tmp_unnest osm
    WHERE osm.maxspeed != 'NaN'
);

DROP TABLE IF EXISTS tmp_speed_conversion;
CREATE TEMP TABLE tmp_speed_conversion AS (
    SELECT
        raw.id,
        CASE
            WHEN raw.mph AND ({km} IS FALSE) THEN raw.speed
            WHEN (raw.mph IS FALSE) AND {km} THEN raw.speed
            WHEN raw.mph AND {km} THEN COALESCE(
                mph_to_km.km,
                {mi_multiplier} * raw.speed
            )
            WHEN (raw.mph IS FALSE) AND ({km} IS FALSE) THEN COALESCE(
                km_to_mph.mph,
                {km_multiplier} * raw.speed
            )
            END AS speed
    FROM
        tmp_raw_speed raw
        LEFT JOIN tmp_speed_km_to_mph km_to_mph
            ON raw.speed = km_to_mph.km
        LEFT JOIN tmp_speed_mph_to_km mph_to_km
            ON raw.speed = mph_to_km.mph
);

DROP TABLE IF EXISTS tmp_speed;
CREATE TEMP TABLE tmp_speed AS (
    SELECT DISTINCT ON (id)
        id,
        speed
    FROM tmp_speed_conversion
    ORDER BY
        id,
        speed DESC
);

DROP TABLE tmp_unnest;
DROP TABLE tmp_speed_mph_to_km;
DROP TABLE tmp_speed_km_to_mph;
DROP TABLE tmp_raw_speed;
DROP TABLE tmp_speed_conversion;
