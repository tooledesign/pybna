DROP TABLE IF EXISTS tmp_unnest;
CREATE TEMP TABLE tmp_unnest AS (
    SELECT
        osm.id,
        osm.one_way_car,
        "cycleway".*,
        "cycleway:left".*,
        "cycleway:right".*,
        "oneway:bicycle".*,
        "cycleway:both".*,
        "cycleway:buffer".*,
        "cycleway:left:buffer".*,
        "cycleway:right:buffer".*,
        "cycleway:both:buffer".*,
        "cycleway:width".*,
        "cycleway:left:width".*,
        "cycleway:right:width".*,
        "cycleway:both:width".*
    FROM
        {osm_ways_schema}.{osm_ways_table} osm,
        unnest(('{{' || trim(both '{{' from trim(both '}}' from COALESCE(osm."cycleway",'{{NaN}}'))) || '}}')::TEXT[]) "cycleway",
        unnest(('{{' || trim(both '{{' from trim(both '}}' from COALESCE(osm."cycleway:left",'{{NaN}}'))) || '}}')::TEXT[]) "cycleway:left",
        unnest(('{{' || trim(both '{{' from trim(both '}}' from COALESCE(osm."cycleway:right",'{{NaN}}'))) || '}}')::TEXT[]) "cycleway:right",
        unnest(('{{' || trim(both '{{' from trim(both '}}' from COALESCE(osm."oneway:bicycle",'{{NaN}}'))) || '}}')::TEXT[]) "oneway:bicycle",
        unnest(('{{' || trim(both '{{' from trim(both '}}' from COALESCE(osm."cycleway:both",'{{NaN}}'))) || '}}')::TEXT[]) "cycleway:both",
        unnest(('{{' || trim(both '{{' from trim(both '}}' from COALESCE(osm."cycleway:buffer",'{{NaN}}'))) || '}}')::TEXT[]) "cycleway:buffer",
        unnest(('{{' || trim(both '{{' from trim(both '}}' from COALESCE(osm."cycleway:left:buffer",'{{NaN}}'))) || '}}')::TEXT[]) "cycleway:left:buffer",
        unnest(('{{' || trim(both '{{' from trim(both '}}' from COALESCE(osm."cycleway:right:buffer",'{{NaN}}'))) || '}}')::TEXT[]) "cycleway:right:buffer",
        unnest(('{{' || trim(both '{{' from trim(both '}}' from COALESCE(osm."cycleway:both:buffer",'{{NaN}}'))) || '}}')::TEXT[]) "cycleway:both:buffer",
        unnest(('{{' || trim(both '{{' from trim(both '}}' from COALESCE(osm."cycleway:width",'{{NaN}}'))) || '}}')::TEXT[]) "cycleway:width",
        unnest(('{{' || trim(both '{{' from trim(both '}}' from COALESCE(osm."cycleway:left:width",'{{NaN}}'))) || '}}')::TEXT[]) "cycleway:left:width",
        unnest(('{{' || trim(both '{{' from trim(both '}}' from COALESCE(osm."cycleway:right:width",'{{NaN}}'))) || '}}')::TEXT[]) "cycleway:right:width",
        unnest(('{{' || trim(both '{{' from trim(both '}}' from COALESCE(osm."cycleway:both:width",'{{NaN}}'))) || '}}')::TEXT[]) "cycleway:both:width"
);

-- ft_bike_infra
DROP TABLE IF EXISTS tmp_bike_infra_raw;
CREATE TEMP TABLE tmp_bike_infra_raw AS (
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
                THEN {ft_multiplier} * substring("cycleway:right:width" from '\d+\.?\d?\d?')::FLOAT
            WHEN one_way_car = 'ft' AND osm."cycleway:left:width" LIKE '% ft'
                THEN {ft_multiplier} * substring("cycleway:left:width" from '\d+\.?\d?\d?')::FLOAT
            WHEN osm."cycleway:both:width" LIKE '% ft'
                THEN {ft_multiplier} * substring("cycleway:both:width" from '\d+\.?\d?\d?')::FLOAT
            WHEN osm."cycleway:width" LIKE '% ft'
                THEN {ft_multiplier} * substring("cycleway:width" from '\d+\.?\d?\d?')::FLOAT

            -- meters
            WHEN osm."cycleway:right:width" LIKE '% m'
                THEN {m_multiplier} * substring("cycleway:right:width" from '\d+\.?\d?\d?')::FLOAT
            WHEN one_way_car = 'ft' AND osm."cycleway:left:width" LIKE '% m'
                THEN {m_multiplier} * substring("cycleway:left:width" from '\d+\.?\d?\d?')::FLOAT
            WHEN osm."cycleway:both:width" LIKE '% m'
                THEN {m_multiplier} * substring("cycleway:both:width" from '\d+\.?\d?\d?')::FLOAT
            WHEN osm."cycleway:width" LIKE '% m'
                THEN {m_multiplier} * substring("cycleway:width" from '\d+\.?\d?\d?')::FLOAT

            -- no units (default=meters)
            WHEN COALESCE(osm."cycleway:right:width",'NaN') != 'NaN'
                THEN {m_multiplier} * substring("cycleway:right:width" from '\d+\.?\d?\d?')::FLOAT
            WHEN one_way_car = 'ft' AND osm."cycleway:left:width" IS NOT NULL
                THEN {m_multiplier} * substring("cycleway:left:width" from '\d+\.?\d?\d?')::FLOAT
            WHEN COALESCE(osm."cycleway:both:width",'NaN') != 'NaN'
                THEN {m_multiplier} * substring("cycleway:both:width" from '\d+\.?\d?\d?')::FLOAT
            WHEN COALESCE(osm."cycleway:width",'NaN') != 'NaN'
                THEN {m_multiplier} * substring("cycleway:width" from '\d+\.?\d?\d?')::FLOAT
        END AS ft_bike_infra_width,
        CASE        -- tf_bike_infra_width
            -- feet
            WHEN osm."cycleway:left:width" LIKE '% ft'
                THEN {ft_multiplier} * substring("cycleway:left:width" from '\d+\.?\d?\d?')::FLOAT
            WHEN one_way_car = 'tf' AND osm."cycleway:right:width" LIKE '% ft'
                THEN {ft_multiplier} * substring("cycleway:right:width" from '\d+\.?\d?\d?')::FLOAT
            WHEN osm."cycleway:both:width" LIKE '% ft'
                THEN {ft_multiplier} * substring("cycleway:both:width" from '\d+\.?\d?\d?')::FLOAT
            WHEN osm."cycleway:width" LIKE '% ft'
                THEN {ft_multiplier} * substring("cycleway:width" from '\d+\.?\d?\d?')::FLOAT

            -- meters
            WHEN osm."cycleway:left:width" LIKE '% m'
                THEN {m_multiplier} * substring("cycleway:left:width" from '\d+\.?\d?\d?')::FLOAT
            WHEN one_way_car = 'tf' AND osm."cycleway:right:width" LIKE '% m'
                THEN {m_multiplier} * substring("cycleway:right:width" from '\d+\.?\d?\d?')::FLOAT
            WHEN osm."cycleway:both:width" LIKE '% m'
                THEN {m_multiplier} * substring("cycleway:both:width" from '\d+\.?\d?\d?')::FLOAT
            WHEN osm."cycleway:width" LIKE '% m'
                THEN {m_multiplier} * substring("cycleway:width" from '\d+\.?\d?\d?')::FLOAT

            -- no units (default=meters)
            WHEN COALESCE(osm."cycleway:left:width",'NaN') != 'NaN'
                THEN {m_multiplier} * substring("cycleway:left:width" from '\d+\.?\d?\d?')::FLOAT
            WHEN one_way_car = 'tf' AND osm."cycleway:right:width" IS NOT NULL
                THEN {m_multiplier} * substring("cycleway:right:width" from '\d+\.?\d?\d?')::FLOAT
            WHEN COALESCE(osm."cycleway:both:width",'NaN') != 'NaN'
                THEN {m_multiplier} * substring("cycleway:both:width" from '\d+\.?\d?\d?')::FLOAT
            WHEN COALESCE(osm."cycleway:width",'NaN') != 'NaN'
                THEN {m_multiplier} * substring("cycleway:width" from '\d+\.?\d?\d?')::FLOAT
        END AS tf_bike_infra_width
    FROM tmp_unnest osm
);

DROP TABLE IF EXISTS tmp_order;
CREATE TEMP TABLE tmp_order(o,f) AS (
    VALUES
        (1,'sharrow'),
        (2,'lane'),
        (3,'buffered_lane'),
        (4,'track')
);

DROP TABLE IF EXISTS tmp_ft_bike;
CREATE TEMP TABLE tmp_ft_bike AS (
    SELECT DISTINCT ON (id)
        r.id,
        r.ft_bike_infra
    FROM
        tmp_bike_infra_raw r
        LEFT JOIN tmp_order
            ON r.ft_bike_infra = tmp_order.f
    ORDER BY
        id,
        COALESCE(r.ft_bike_infra,'NaN') = 'NaN' DESC,
        tmp_order.o ASC
);

DROP TABLE IF EXISTS tmp_ft_width;
CREATE TEMP TABLE tmp_ft_width AS (
    SELECT DISTINCT ON (id)
        id,
        ft_bike_infra_width
    FROM tmp_bike_infra_raw
    ORDER BY
        id,
        ft_bike_infra_width ASC
);

DROP TABLE IF EXISTS tmp_tf_bike;
CREATE TEMP TABLE tmp_tf_bike AS (
    SELECT DISTINCT ON (id)
        r.id,
        r.tf_bike_infra
    FROM
        tmp_bike_infra_raw r
        LEFT JOIN tmp_order
            ON r.tf_bike_infra = tmp_order.f
    ORDER BY
        id,
        COALESCE(r.tf_bike_infra,'NaN') = 'NaN' DESC,
        tmp_order.o ASC
);

DROP TABLE IF EXISTS tmp_tf_width;
CREATE TEMP TABLE tmp_tf_width AS (
    SELECT DISTINCT ON (id)
        id,
        tf_bike_infra_width
    FROM tmp_bike_infra_raw
    ORDER BY
        id,
        tf_bike_infra_width ASC
);

DROP TABLE IF EXISTS tmp_bike_infra;
CREATE TEMP TABLE tmp_bike_infra AS (
    SELECT
        osm.id,
        tmp_ft_bike.ft_bike_infra,
        tmp_ft_width.ft_bike_infra_width,
        tmp_tf_bike.tf_bike_infra,
        tmp_tf_width.tf_bike_infra_width
    FROM
        {osm_ways_schema}.{osm_ways_table} osm
        LEFT JOIN tmp_ft_bike
            ON osm.id = tmp_ft_bike.id
        LEFT JOIN tmp_ft_width
            ON osm.id = tmp_ft_width.id
        LEFT JOIN tmp_tf_bike
            ON osm.id = tmp_tf_bike.id
        LEFT JOIN tmp_tf_width
            ON osm.id = tmp_tf_width.id
    WHERE
        tmp_ft_bike IS NOT NULL
        OR tmp_tf_bike IS NOT NULL
);

DROP TABLE tmp_unnest;
DROP TABLE tmp_bike_infra_raw;
DROP TABLE tmp_ft_bike;
DROP TABLE tmp_ft_width;
DROP TABLE tmp_tf_bike;
DROP TABLE tmp_tf_width;
