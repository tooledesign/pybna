DROP TABLE IF EXISTS tmp_lanes;
CREATE TEMP TABLE tmp_lanes AS (
    SELECT
        osm.id,
        CASE
            WHEN COALESCE(osm."turn:lanes:forward",'NaN') != 'NaN'
                THEN array_length(
                    regexp_split_to_array(
                        osm."turn:lanes:forward",
                        '\|'
                    ),
                    1       -- only one dimension
                )
            WHEN COALESCE(osm."turn:lanes",'NaN') != 'NaN' AND one_way_car = 'ft'
                THEN array_length(
                    regexp_split_to_array(
                        osm."turn:lanes",
                        '\|'
                    ),
                    1       -- only one dimension
                )
            WHEN COALESCE(osm."lanes:forward",'NaN') != 'NaN'
                THEN substring(osm."lanes:forward" FROM '\d+')::INT
            WHEN COALESCE(osm."lanes",'NaN') != 'NaN' AND one_way_car = 'ft'
                THEN substring(osm."lanes" FROM '\d+')::INT
            WHEN COALESCE(osm."lanes",'NaN') != 'NaN'
                THEN ceil(substring(osm."lanes" FROM '\d+')::FLOAT / 2)
            END AS ft_lanes,
        CASE
            WHEN COALESCE(osm."turn:lanes:backward",'NaN') != 'NaN'
                THEN array_length(
                    regexp_split_to_array(
                        osm."turn:lanes:backward",
                        '\|'
                    ),
                    1       -- only one dimension
                )
            WHEN COALESCE(osm."turn:lanes",'NaN') != 'NaN' AND one_way_car = 'tf'
                THEN array_length(
                    regexp_split_to_array(
                        osm."turn:lanes",
                        '\|'
                    ),
                    1       -- only one dimension
                )
            WHEN COALESCE(osm."lanes:backward",'NaN') != 'NaN'
                THEN substring(osm."lanes:backward" FROM '\d+')::INT
            WHEN COALESCE(osm."lanes",'NaN') != 'NaN' AND one_way_car = 'tf'
                THEN substring(osm."lanes" FROM '\d+')::INT
            WHEN COALESCE(osm."lanes",'NaN') != 'NaN'
                THEN ceil(substring(osm."lanes" FROM '\d+')::FLOAT / 2)
            END AS tf_lanes
    FROM {osm_ways_schema}.{osm_ways_table} osm
    WHERE
        (
            osm."turn:lanes:forward" IS NOT NULL
            AND osm."turn:lanes:forward" != 'NaN'
        )
        OR
        (
            osm."turn:lanes" IS NOT NULL
            AND osm."turn:lanes" != 'NaN'
        )
        OR
        (
            osm."lanes:forward" IS NOT NULL
            AND osm."lanes:forward" != 'NaN'
        )
        OR
        (
            osm."lanes" IS NOT NULL
            AND osm."lanes" != 'NaN'
        )
        OR
        (
            osm."turn:lanes:backward" IS NOT NULL
            AND osm."turn:lanes:backward" != 'NaN'
        )
        OR
        (
            osm."lanes:backward" IS NOT NULL
            AND osm."lanes:backward" != 'NaN'
        )
);
