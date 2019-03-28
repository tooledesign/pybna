DROP TABLE IF EXISTS tmp_unnest;
CREATE TEMP TABLE tmp_unnest AS (
    SELECT
        osm.id,
        "parking:lane:both".*,
        "parking:lane:left".*,
        "parking:lane:right".*
    FROM
        {osm_ways_schema}.{osm_ways_table} osm,
        unnest(('{{' || trim(both '{{' from trim(both '}}' from COALESCE(osm."parking:lane:both",'{{NaN}}'))) || '}}')::TEXT[]) "parking:lane:both",
        unnest(('{{' || trim(both '{{' from trim(both '}}' from COALESCE(osm."parking:lane:left",'{{NaN}}'))) || '}}')::TEXT[]) "parking:lane:left",
        unnest(('{{' || trim(both '{{' from trim(both '}}' from COALESCE(osm."parking:lane:right",'{{NaN}}'))) || '}}')::TEXT[]) "parking:lane:right"
);

DROP TABLE IF EXISTS tmp_park_raw;
CREATE TEMP TABLE tmp_park_raw AS (
    SELECT
        osm.id,
        CASE
            WHEN osm."parking:lane:both" = 'parallel' THEN TRUE
            WHEN osm."parking:lane:both" = 'paralell' THEN TRUE
            WHEN osm."parking:lane:both" = 'diagonal' THEN TRUE
            WHEN osm."parking:lane:both" = 'perpendicular' THEN TRUE
            WHEN osm."parking:lane:both" = 'no_parking' THEN FALSE
            WHEN osm."parking:lane:both" = 'no_stopping' THEN FALSE
            WHEN osm."parking:lane:right" = 'parallel' THEN TRUE
            WHEN osm."parking:lane:right" = 'paralell' THEN TRUE
            WHEN osm."parking:lane:right" = 'diagonal' THEN TRUE
            WHEN osm."parking:lane:right" = 'perpendicular' THEN TRUE
            WHEN osm."parking:lane:right" = 'no_parking' THEN FALSE
            WHEN osm."parking:lane:right" = 'no_stopping' THEN FALSE
            END AS ft_park,
        CASE
            WHEN osm."parking:lane:both" = 'parallel' THEN TRUE
            WHEN osm."parking:lane:both" = 'paralell' THEN TRUE
            WHEN osm."parking:lane:both" = 'diagonal' THEN TRUE
            WHEN osm."parking:lane:both" = 'perpendicular' THEN TRUE
            WHEN osm."parking:lane:both" = 'no_parking' THEN FALSE
            WHEN osm."parking:lane:both" = 'no_stopping' THEN FALSE
            WHEN osm."parking:lane:left" = 'parallel' THEN TRUE
            WHEN osm."parking:lane:left" = 'paralell' THEN TRUE
            WHEN osm."parking:lane:left" = 'diagonal' THEN TRUE
            WHEN osm."parking:lane:left" = 'perpendicular' THEN TRUE
            WHEN osm."parking:lane:left" = 'no_parking' THEN FALSE
            WHEN osm."parking:lane:left" = 'no_stopping' THEN FALSE
            END AS tf_park
    FROM tmp_unnest osm
    WHERE
        (
            osm."parking:lane:both" IS NOT NULL
            AND osm."parking:lane:both" != 'NaN'
        )
        OR
        (
            osm."parking:lane:right" IS NOT NULL
            AND osm."parking:lane:right" != 'NaN'
        )
        OR
        (
            osm."parking:lane:left" IS NOT NULL
            AND osm."parking:lane:left" != 'NaN'
        )
);

DROP TABLE IF EXISTS tmp_park_ft;
CREATE TEMP TABLE tmp_park_ft AS (
    SELECT DISTINCT ON (id)
        id,
        ft_park
    FROM tmp_park_raw
    ORDER BY
        id,
        ft_park DESC
);

DROP TABLE IF EXISTS tmp_park_tf;
CREATE TEMP TABLE tmp_park_tf AS (
    SELECT DISTINCT ON (id)
        id,
        tf_park
    FROM tmp_park_raw
    ORDER BY
        id,
        tf_park DESC
);

DROP TABLE tmp_unnest;
DROP TABLE tmp_park_raw;
