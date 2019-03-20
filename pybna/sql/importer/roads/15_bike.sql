-- ft_bike_infra
DROP TABLE IF EXISTS tmp_bike_infra;
CREATE TEMP TABLE tmp_bike_infra AS (
    SELECT
        osm.id,
        CASE        --ft_bike_infra
            -- :both
            WHEN osm."cycleway:both" = 'shared_lane' THEN 'sharrow'
            WHEN osm."cycleway:both" = 'buffered_lane' THEN 'buffered_lane'
            WHEN osm."cycleway:both" = 'lane' AND osm."cycleway:buffer" IN ('yes','both','right','left') THEN 'buffered_lane'
            WHEN osm."cycleway:both" = 'lane' AND osm."cycleway:both:buffer" IN ('yes','both','right','left') THEN 'buffered_lane'
            WHEN osm."cycleway:both" = 'lane' THEN 'lane'
            WHEN osm."cycleway:both" = 'track' THEN 'track'
            WHEN (osm."cycleway:right" = 'track' AND osm."oneway:bicycle" = 'no') THEN 'track'
            WHEN (osm."cycleway:left" = 'track' AND osm."oneway:bicycle" = 'no') THEN 'track'
            WHEN (osm.cycleway = 'track' AND osm."oneway:bicycle" = 'no') THEN 'track'

            -- one-way=ft
            WHEN one_way_car = 'ft' THEN
                CASE
                    WHEN osm."cycleway:left" = 'shared_lane' THEN 'sharrow'
                    WHEN osm."cycleway:left" = 'lane' AND osm."cycleway:buffer" IN ('yes','both','right','left') THEN 'buffered_lane'
                    WHEN osm."cycleway:left" = 'lane' AND osm."cycleway:left:buffer" IN ('yes','both','right','left') THEN 'buffered_lane'
                    WHEN osm."cycleway:left" = 'lane' THEN 'lane'
                    WHEN osm."cycleway:left" = 'track' THEN 'track'

                    -- stuff from two-way that also applies to one-way=ft
                    WHEN osm.cycleway = 'shared_lane' THEN 'sharrow'
                    WHEN osm."cycleway:right" = 'shared_lane' THEN 'sharrow'
                    WHEN osm.cycleway = 'buffered_lane' THEN 'buffered_lane'
                    WHEN osm."cycleway:right" = 'buffered_lane' THEN 'buffered_lane'
                    WHEN osm.cycleway = 'lane' AND osm."cycleway:buffer" IN ('yes','both','right','left') THEN 'buffered_lane'
                    WHEN osm."cycleway:right" = 'lane' AND osm."cycleway:buffer" IN ('yes','both','right','left') THEN 'buffered_lane'
                    WHEN osm."cycleway:right" = 'lane' AND osm."cycleway:right:buffer" IN ('yes','both','right','left') THEN 'buffered_lane'
                    WHEN osm.cycleway = 'lane' THEN 'lane'
                    WHEN osm."cycleway:right" = 'lane' THEN 'lane'
                    WHEN osm."cycleway" = 'track' THEN 'track'
                    WHEN osm."cycleway:right" = 'track' THEN 'track'
                END

            -- one-way=tf
            WHEN one_way_car = 'tf' THEN
                CASE
                    WHEN osm.cycleway = 'opposite_lane' AND osm."cycleway:buffer" IN ('yes','both','right','left') THEN 'buffered_lane'
                    WHEN osm."cycleway:right" = 'opposite_lane' AND osm."cycleway:buffer" IN ('yes','both','right','left') THEN 'buffered_lane'
                    WHEN osm."cycleway:right" = 'opposite_lane' AND osm."cycleway:right:buffer" IN ('yes','both','right','left') THEN 'buffered_lane'
                    WHEN osm.cycleway = 'opposite_lane' THEN 'lane'
                    WHEN osm."cycleway:right" = 'opposite_lane' THEN 'lane'
                    WHEN osm."cycleway" = 'opposite_track' THEN 'track'
                    WHEN (one_way_car = 'tf' AND osm."cycleway:left" = 'opposite_track') THEN 'track'
                    WHEN (one_way_car = 'tf' AND osm."cycleway:right" = 'opposite_track') THEN 'track'
                END

            -- two-way
            WHEN one_way_car IS NULL THEN
                CASE
                    WHEN osm.cycleway = 'shared_lane' THEN 'sharrow'
                    WHEN osm."cycleway:right" = 'shared_lane' THEN 'sharrow'
                    WHEN osm.cycleway = 'buffered_lane' THEN 'buffered_lane'
                    WHEN osm."cycleway:right" = 'buffered_lane' THEN 'buffered_lane'
                    WHEN osm.cycleway = 'lane' AND osm."cycleway:buffer" IN ('yes','both','right','left') THEN 'buffered_lane'
                    WHEN osm."cycleway:right" = 'lane' AND osm."cycleway:buffer" IN ('yes','both','right','left') THEN 'buffered_lane'
                    WHEN osm."cycleway:right" = 'lane' AND osm."cycleway:right:buffer" IN ('yes','both','right','left') THEN 'buffered_lane'
                    WHEN osm.cycleway = 'lane' THEN 'lane'
                    WHEN osm."cycleway:right" = 'lane' THEN 'lane'
                    WHEN osm."cycleway" = 'track' THEN 'track'
                    WHEN osm."cycleway:right" = 'track' THEN 'track'
                END
        END AS ft_bike_infra,
        CASE        -- tf_bike_infra
            -- :both
            WHEN osm."cycleway:both" = 'shared_lane' THEN 'sharrow'
            WHEN osm."cycleway:both" = 'buffered_lane' THEN 'buffered_lane'
            WHEN osm."cycleway:both" = 'lane' AND osm."cycleway:buffer" IN ('yes','both','right','left') THEN 'buffered_lane'
            WHEN osm."cycleway:both" = 'lane' AND osm."cycleway:both:buffer" IN ('yes','both','right','left') THEN 'buffered_lane'
            WHEN osm."cycleway:both" = 'lane' THEN 'lane'
            WHEN osm."cycleway:both" = 'track' THEN 'track'
            WHEN (osm."cycleway:right" = 'track' AND osm."oneway:bicycle" = 'no') THEN 'track'
            WHEN (osm."cycleway:left" = 'track' AND osm."oneway:bicycle" = 'no') THEN 'track'
            WHEN (osm.cycleway = 'track' AND osm."oneway:bicycle" = 'no') THEN 'track'

            -- one-way=tf
            WHEN one_way_car = 'tf' THEN
                CASE
                    WHEN osm."cycleway:right" = 'shared_lane' THEN 'sharrow'
                    WHEN osm."cycleway:right" = 'lane' AND osm."cycleway:buffer" IN ('yes','both','right','left') THEN 'buffered_lane'
                    WHEN osm."cycleway:right" = 'lane' AND osm."cycleway:right:buffer" IN ('yes','both','right','left') THEN 'buffered_lane'
                    WHEN osm."cycleway:right" = 'lane' THEN 'lane'
                    WHEN osm."cycleway:right" = 'track' THEN 'track'

                    -- stuff from two-way that also applies to one-way=tf
                    WHEN osm.cycleway = 'shared_lane' THEN 'sharrow'
                    WHEN osm."cycleway:left" = 'shared_lane' THEN 'sharrow'
                    WHEN osm.cycleway = 'buffered_lane' THEN 'buffered_lane'
                    WHEN osm."cycleway:left" = 'buffered_lane' THEN 'buffered_lane'
                    WHEN osm.cycleway = 'lane' AND osm."cycleway:buffer" IN ('yes','both','right','left') THEN 'buffered_lane'
                    WHEN osm."cycleway:left" = 'lane' AND osm."cycleway:buffer" IN ('yes','both','right','left') THEN 'buffered_lane'
                    WHEN osm."cycleway:left" = 'lane' AND osm."cycleway:left:buffer" IN ('yes','both','right','left') THEN 'buffered_lane'
                    WHEN osm.cycleway = 'lane' THEN 'lane'
                    WHEN osm."cycleway:left" = 'lane' THEN 'lane'
                    WHEN osm."cycleway" = 'track' THEN 'track'
                    WHEN osm."cycleway:left" = 'track' THEN 'track'
                END

            -- one-way=ft
            WHEN one_way_car = 'ft' THEN
                CASE
                    WHEN osm.cycleway = 'opposite_lane' AND osm."cycleway:buffer" IN ('yes','both','right','left') THEN 'buffered_lane'
                    WHEN osm."cycleway:right" = 'opposite_lane' AND osm."cycleway:buffer" IN ('yes','both','right','left') THEN 'buffered_lane'
                    WHEN osm."cycleway:right" = 'opposite_lane' AND osm."cycleway:right:buffer" IN ('yes','both','right','left') THEN 'buffered_lane'
                    WHEN osm.cycleway = 'opposite_lane' THEN 'lane'
                    WHEN osm."cycleway:right" = 'opposite_lane' THEN 'lane'
                    WHEN osm."cycleway" = 'opposite_track' THEN 'track'
                    WHEN (one_way_car = 'tf' AND osm."cycleway:left" = 'opposite_track') THEN 'track'
                    WHEN (one_way_car = 'tf' AND osm."cycleway:right" = 'opposite_track') THEN 'track'
                END

            -- two-way
            WHEN one_way_car IS NULL THEN
                CASE
                    WHEN osm.cycleway = 'shared_lane' THEN 'sharrow'
                    WHEN osm."cycleway:left" = 'shared_lane' THEN 'sharrow'
                    WHEN osm.cycleway = 'buffered_lane' THEN 'buffered_lane'
                    WHEN osm."cycleway:left" = 'buffered_lane' THEN 'buffered_lane'
                    WHEN osm.cycleway = 'lane' AND osm."cycleway:buffer" IN ('yes','both','right','left') THEN 'buffered_lane'
                    WHEN osm."cycleway:left" = 'lane' AND osm."cycleway:buffer" IN ('yes','both','right','left') THEN 'buffered_lane'
                    WHEN osm."cycleway:left" = 'lane' AND osm."cycleway:left:buffer" IN ('yes','both','right','left') THEN 'buffered_lane'
                    WHEN osm.cycleway = 'lane' THEN 'lane'
                    WHEN osm."cycleway:left" = 'lane' THEN 'lane'
                    WHEN osm."cycleway" = 'track' THEN 'track'
                    WHEN osm."cycleway:left" = 'track' THEN 'track'
                END
        END AS tf_bike_infra,
        CASE        --ft_bike_infra_width
            -- feet
            WHEN osm."cycleway:right:width" LIKE '% ft'
                THEN substring("cycleway:right:width" from '\d+\.?\d?\d?')::FLOAT
            WHEN one_way_car = 'ft' AND osm."cycleway:left:width" LIKE '% ft'
                THEN substring("cycleway:left:width" from '\d+\.?\d?\d?')::FLOAT
            WHEN osm."cycleway:both:width" LIKE '% ft'
                THEN substring("cycleway:both:width" from '\d+\.?\d?\d?')::FLOAT
            WHEN osm."cycleway:width" LIKE '% ft'
                THEN substring("cycleway:width" from '\d+\.?\d?\d?')::FLOAT

            -- meters
            WHEN osm."cycleway:right:width" LIKE '% m'
                THEN 3.28084 * substring("cycleway:right:width" from '\d+\.?\d?\d?')::FLOAT
            WHEN one_way_car = 'ft' AND osm."cycleway:left:width" LIKE '% m'
                THEN 3.28084 * substring("cycleway:left:width" from '\d+\.?\d?\d?')::FLOAT
            WHEN osm."cycleway:both:width" LIKE '% m'
                THEN 3.28084 * substring("cycleway:both:width" from '\d+\.?\d?\d?')::FLOAT
            WHEN osm."cycleway:width" LIKE '% m'
                THEN 3.28084 * substring("cycleway:width" from '\d+\.?\d?\d?')::FLOAT

            -- no units (default=meters)
            WHEN COALESCE(osm."cycleway:right:width",'NaN') != 'NaN'
                THEN 3.28084 * substring("cycleway:right:width" from '\d+\.?\d?\d?')::FLOAT
            WHEN one_way_car = 'ft' AND osm."cycleway:left:width" IS NOT NULL
                THEN 3.28084 * substring("cycleway:left:width" from '\d+\.?\d?\d?')::FLOAT
            WHEN COALESCE(osm."cycleway:both:width",'NaN') != 'NaN'
                THEN 3.28084 * substring("cycleway:both:width" from '\d+\.?\d?\d?')::FLOAT
            WHEN COALESCE(osm."cycleway:width",'NaN') != 'NaN'
                THEN 3.28084 * substring("cycleway:width" from '\d+\.?\d?\d?')::FLOAT
        END AS ft_bike_infra_width,
        CASE        -- tf_bike_infra_width
            -- feet
            WHEN osm."cycleway:left:width" LIKE '% ft'
                THEN substring("cycleway:left:width" from '\d+\.?\d?\d?')::FLOAT
            WHEN one_way_car = 'tf' AND osm."cycleway:right:width" LIKE '% ft'
                THEN substring("cycleway:right:width" from '\d+\.?\d?\d?')::FLOAT
            WHEN osm."cycleway:both:width" LIKE '% ft'
                THEN substring("cycleway:both:width" from '\d+\.?\d?\d?')::FLOAT
            WHEN osm."cycleway:width" LIKE '% ft'
                THEN substring("cycleway:width" from '\d+\.?\d?\d?')::FLOAT

            -- meters
            WHEN osm."cycleway:left:width" LIKE '% m'
                THEN 3.28084 * substring("cycleway:left:width" from '\d+\.?\d?\d?')::FLOAT
            WHEN one_way_car = 'tf' AND osm."cycleway:right:width" LIKE '% m'
                THEN 3.28084 * substring("cycleway:right:width" from '\d+\.?\d?\d?')::FLOAT
            WHEN osm."cycleway:both:width" LIKE '% m'
                THEN 3.28084 * substring("cycleway:both:width" from '\d+\.?\d?\d?')::FLOAT
            WHEN osm."cycleway:width" LIKE '% m'
                THEN 3.28084 * substring("cycleway:width" from '\d+\.?\d?\d?')::FLOAT

            -- no units (default=meters)
            WHEN COALESCE(osm."cycleway:left:width",'NaN') != 'NaN'
                THEN 3.28084 * substring("cycleway:left:width" from '\d+\.?\d?\d?')::FLOAT
            WHEN one_way_car = 'tf' AND osm."cycleway:right:width" IS NOT NULL
                THEN 3.28084 * substring("cycleway:right:width" from '\d+\.?\d?\d?')::FLOAT
            WHEN COALESCE(osm."cycleway:both:width",'NaN') != 'NaN'
                THEN 3.28084 * substring("cycleway:both:width" from '\d+\.?\d?\d?')::FLOAT
            WHEN COALESCE(osm."cycleway:width",'NaN') != 'NaN'
                THEN 3.28084 * substring("cycleway:width" from '\d+\.?\d?\d?')::FLOAT
        END AS tf_bike_infra_width
    FROM {osm_ways_schema}.{osm_ways_table} osm
);
