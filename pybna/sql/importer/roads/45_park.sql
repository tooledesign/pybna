DROP TABLE IF EXISTS tmp_park;
CREATE TEMP TABLE tmp_park AS (
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
    FROM {osm_ways_schema}.{osm_ways_table} osm
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
