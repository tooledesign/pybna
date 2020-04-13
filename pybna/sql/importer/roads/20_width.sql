-- set oneway attribute based on bike infra (to accommodate contraflow, 2-way cycle tracks, etc.)
DROP TABLE IF EXISTS tmp_width;
CREATE TEMP TABLE tmp_width AS (
    SELECT
        osm.id,
        CASE
            WHEN osm.width LIKE '% ft' THEN {ft_multiplier} * substring(osm.width from '\d+\.?\d?\d?')::FLOAT
            WHEN osm.width LIKE '% m' THEN {m_multiplier} * substring(osm.width from '\d+\.?\d?\d?')::FLOAT
            -- anything left is sorted based on whether it's greater than 50 or not
            -- if it's less than 50 we assume meters (OSM default). if it's more than
            -- 50 we assume feet since anything above 50 meters would be absurd.
            WHEN substring(osm.width from '\d+\.?\d?\d?')::FLOAT < 50
                THEN {m_multiplier} * substring(osm.width from '\d+\.?\d?\d?')::FLOAT
            WHEN substring(osm.width from '\d+\.?\d?\d?')::FLOAT >= 50
                THEN {ft_multiplier} * substring(osm.width from '\d+\.?\d?\d?')::FLOAT
            END AS width
    FROM {osm_ways_schema}.{osm_ways_table} osm
    WHERE
        osm.width IS NOT NULL
        AND osm.width != 'NaN'
);
