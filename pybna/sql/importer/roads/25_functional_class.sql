DROP TABLE IF EXISTS tmp_unnest;
CREATE TEMP TABLE tmp_unnest AS (
    SELECT
        osm.id,
        bridge.*,
        highway.*,
        tracktype.*,
        footway.*,
        access.*,
        bicycle.*
    FROM
        {osm_ways_schema}.{osm_ways_table} osm,
        unnest(('{{' || trim(both '{{' from trim(both '}}' from COALESCE(osm.bridge,'{{NaN}}'))) || '}}')::TEXT[]) bridge,
        unnest(('{{' || trim(both '{{' from trim(both '}}' from COALESCE(osm.highway,'{{NaN}}'))) || '}}')::TEXT[]) highway,
        unnest(('{{' || trim(both '{{' from trim(both '}}' from COALESCE(osm.tracktype,'{{NaN}}'))) || '}}')::TEXT[]) tracktype,
        unnest(('{{' || trim(both '{{' from trim(both '}}' from COALESCE(osm.footway,'{{NaN}}'))) || '}}')::TEXT[]) footway,
        unnest(('{{' || trim(both '{{' from trim(both '}}' from COALESCE(osm.access,'{{NaN}}'))) || '}}')::TEXT[]) access,
        unnest(('{{' || trim(both '{{' from trim(both '}}' from COALESCE(osm.bicycle,'{{NaN}}'))) || '}}')::TEXT[]) bicycle
);

DROP TABLE IF EXISTS tmp_raw_func;
CREATE TEMP TABLE tmp_raw_func AS (
    SELECT
        osm.id,
        CASE
            WHEN
                osm.highway IN ('residential','unclassified')
                AND (
                    tmp_bike_infra.ft_bike_infra IN ('track','buffered_lane','lane')
                    OR tmp_bike_infra.tf_bike_infra IN ('track','buffered_lane','lane')
                )
                THEN 'tertiary'
            WHEN osm.highway IN (
                'motorway',
                'tertiary',
                'trunk',
                'tertiary_link',
                'motorway_link',
                'secondary_link',
                'primary_link',
                'trunk_link',
                'unclassified',
                'residential',
                'secondary',
                'primary',
                'living_street'
            )
                THEN osm.highway
            WHEN osm.highway = 'track' AND osm.tracktype = 'grade1'
                THEN 'track'
            WHEN osm.highway IN ('cycleway','path')
                THEN 'path'
            WHEN osm.highway = 'footway' AND osm.footway = 'crossing'
                THEN 'path'
            WHEN
                osm.highway = 'footway'
                AND osm.bicycle IN ('designated','yes','permissive')
                AND osm.bridge = 'yes'
                THEN 'path'
            WHEN
                osm.highway = 'footway'
                AND osm.bicycle IN ('designated','yes','permissive')
                AND COALESCE(osm.access,'yes') NOT IN ('no','private')
                AND tmp_width.width >= 8
                THEN 'path'
            WHEN osm.highway='service' AND osm.bicycle IN ('designated','yes','permissive')
                THEN 'path'
            WHEN
                osm.highway = 'pedestrian'
                AND osm.bicycle IN ('yes','permissive','designated')
                AND COALESCE(osm.access,'yes') NOT IN ('no','private')
                THEN 'living_street'
            END AS functional_class
    FROM
        tmp_unnest osm
        LEFT JOIN tmp_width
            ON osm.id = tmp_width.id
        LEFT JOIN tmp_bike_infra
            ON osm.id = tmp_bike_infra.id
);

DROP TABLE IF EXISTS tmp_order;
CREATE TEMP TABLE tmp_order(o,f) AS (
    VALUES
        (1,'motorway_link'),
        (2,'motorway'),
        (3,'tertiary_link'),
        (4,'tertiary'),
        (5,'trunk_link'),
        (6,'trunk'),
        (7,'primary_link'),
        (8,'primary'),
        (9,'secondary_link'),
        (10,'secondary'),
        (11,'unclassified'),
        (12,'residential'),
        (13,'living_street'),
        (14,'path')
);

DROP TABLE IF EXISTS tmp_func;
CREATE TEMP TABLE tmp_func AS (
    SELECT DISTINCT ON (tmp_raw_func.id)
        tmp_raw_func.id,
        tmp_raw_func.functional_class
    FROM
        tmp_raw_func,
        tmp_order
    WHERE tmp_raw_func.functional_class = tmp_order.f
    ORDER BY
        tmp_raw_func.id,
        tmp_order.o DESC
);

DROP TABLE tmp_unnest;
DROP TABLE tmp_raw_func;
DROP TABLE tmp_order;
