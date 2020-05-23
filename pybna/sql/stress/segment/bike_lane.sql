-----------------------------------------------
-- bike lanes with all avaialble data
-- (uses tables in DB, based on Furth 2017 with
-- some adjustments based on direction from prime)
-----------------------------------------------

-- #reading given and assumed road characteristics
DROP TABLE IF EXISTS pg_temp.tmp_attrs;
CREATE TEMP TABLE pg_temp.tmp_attrs AS (
    SELECT
        {id_column}::INTEGER AS id,
        COALESCE({lanes},{assumed_lanes})::INTEGER AS lanes,
        {oneway}::BOOLEAN AS oneway,
        COALESCE({parking},{assumed_parking})::BOOLEAN AS parking,
        COALESCE({parking_width},{assumed_parking_width})::INTEGER AS parking_width,
        COALESCE({low_parking},{assumed_low_parking})::BOOLEAN AS low_parking,
        COALESCE({bike_lane_width},{assumed_bike_lane_width})::INTEGER AS bike_lane_width,
        COALESCE({speed},{assumed_speed})::INTEGER AS speed
    FROM
        {in_schema}.{in_table}
    WHERE
        {bike_lane}
        AND {filter}
);

CREATE INDEX tidx_tmp_attrs_id ON pg_temp.tmp_attrs (id); ANALYZE pg_temp.tmp_attrs;

-- #comparing against LTS tables;
DROP TABLE IF EXISTS pg_temp.tmp_stress;
CREATE TEMP TABLE pg_temp.tmp_stress AS (
    SELECT DISTINCT ON (tmp_attrs.id)
        tmp_attrs.id,
        lts.stress
    FROM
        pg_temp.tmp_attrs,
        {bike_lane_lts_schema}.{bike_lane_lts_table} lts
    WHERE
        tmp_attrs.lanes <= lts.lanes
        AND (lts.oneway IS NULL OR tmp_attrs.oneway = lts.oneway)
        AND tmp_attrs.parking = lts.parking
        AND tmp_attrs.low_parking >= lts.low_parking
        AND (tmp_attrs.parking_width * tmp_attrs.parking::INTEGER) + tmp_attrs.bike_lane_width >= lts.reach
        AND tmp_attrs.speed <= lts.speed
    ORDER BY
        tmp_attrs.id,
        lts.stress ASC
);

CREATE INDEX tidx_tmp_stress_id ON pg_temp.tmp_stress (id); ANALYZE pg_temp.tmp_stress;

-- insert
INSERT INTO {out_schema}.{out_table} (
    {id_column},
    {geom},
    lanes,
    parking,
    low_parking,
    parking_width,
    bike_lane_width,
    speed,
    stress
)
SELECT
    tmp_attrs.id,
    {in_table}.geom,
    tmp_attrs.lanes,
    tmp_attrs.parking,
    tmp_attrs.low_parking,
    tmp_attrs.parking_width,
    tmp_attrs.bike_lane_width,
    tmp_attrs.speed,
    tmp_stress.stress
FROM
    pg_temp.tmp_attrs,
    {in_schema}.{in_table},
    pg_temp.tmp_stress
WHERE
    tmp_attrs.id = {in_table}.{id_column}
    AND tmp_attrs.id = tmp_stress.id
;
