DROP TABLE IF EXISTS tmp_unnest;
CREATE TEMP TABLE tmp_unnest AS (
    SELECT
        osm.id,
        osm.one_way_car,
        "lanes".*,
        "lanes:forward".*,
        "lanes:backward".*,
        "lanes:both_ways".*,
        "turn:lanes".*,
        "turn:lanes:both_ways".*,
        "turn:lanes:backward".*,
        "turn:lanes:forward".*
    FROM
        {osm_ways_schema}.{osm_ways_table} osm,
        unnest(('{{' || trim(both '{{' from trim(both '}}' from COALESCE(osm."lanes",'{{NaN}}'))) || '}}')::TEXT[]) "lanes",
        unnest(('{{' || trim(both '{{' from trim(both '}}' from COALESCE(osm."lanes:forward",'{{NaN}}'))) || '}}')::TEXT[]) "lanes:forward",
        unnest(('{{' || trim(both '{{' from trim(both '}}' from COALESCE(osm."lanes:backward",'{{NaN}}'))) || '}}')::TEXT[]) "lanes:backward",
        unnest(('{{' || trim(both '{{' from trim(both '}}' from COALESCE(osm."lanes:both_ways",'{{NaN}}'))) || '}}')::TEXT[]) "lanes:both_ways",
        unnest(('{{' || trim(both '{{' from trim(both '}}' from COALESCE(osm."turn:lanes",'{{NaN}}'))) || '}}')::TEXT[]) "turn:lanes",
        unnest(('{{' || trim(both '{{' from trim(both '}}' from COALESCE(osm."turn:lanes:both_ways",'{{NaN}}'))) || '}}')::TEXT[]) "turn:lanes:both_ways",
        unnest(('{{' || trim(both '{{' from trim(both '}}' from COALESCE(osm."turn:lanes:backward",'{{NaN}}'))) || '}}')::TEXT[]) "turn:lanes:backward",
        unnest(('{{' || trim(both '{{' from trim(both '}}' from COALESCE(osm."turn:lanes:forward",'{{NaN}}'))) || '}}')::TEXT[]) "turn:lanes:forward"
);

DROP TABLE IF EXISTS tmp_lanes_raw;
CREATE TEMP TABLE tmp_lanes_raw AS (
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
            WHEN COALESCE(osm."lanes",'NaN') != 'NaN' AND COALESCE(one_way_car,'tf') != 'tf'
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
            WHEN COALESCE(osm."lanes",'NaN') != 'NaN' AND COALESCE(one_way_car,'ft') != 'ft'
                THEN ceil(substring(osm."lanes" FROM '\d+')::FLOAT / 2)
            END AS tf_lanes
    FROM tmp_unnest osm
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

DROP TABLE IF EXISTS tmp_ft_lanes;
CREATE TEMP TABLE tmp_ft_lanes AS (
    SELECT DISTINCT ON (id)
        id,
        ft_lanes
    FROM tmp_lanes_raw
    WHERE ft_lanes > 0
    ORDER BY
        id,
        ft_lanes DESC
);

DROP TABLE IF EXISTS tmp_tf_lanes;
CREATE TEMP TABLE tmp_tf_lanes AS (
    SELECT DISTINCT ON (id)
        id,
        tf_lanes
    FROM tmp_lanes_raw
    WHERE tf_lanes > 0
    ORDER BY
        id,
        tf_lanes DESC
);

DROP TABLE tmp_unnest;
DROP TABLE tmp_lanes_raw;
