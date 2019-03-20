DROP TABLE IF EXISTS tmp_func;
CREATE TEMP TABLE tmp_func AS (
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
                AND osm.bicycle = 'designated'
                AND COALESCE(osm.access,'yes') NOT IN ('no','private')
                AND tmp_width.width >= 8
                THEN 'path'
            WHEN osm.highway='service' AND osm.bicycle='designated'
                THEN 'path'
            WHEN
                osm.highway = 'pedestrian'
                AND osm.bicycle IN ('yes','permissive','designated')
                AND COALESCE(osm.access,'yes') NOT IN ('no','private')
                THEN 'living_street'
            END AS functional_class
    FROM
        {osm_ways_schema}.{osm_ways_table} osm
        LEFT JOIN tmp_width
            ON osm.id = tmp_width.id
        LEFT JOIN tmp_bike_infra
            ON osm.id = tmp_bike_infra.id
);
