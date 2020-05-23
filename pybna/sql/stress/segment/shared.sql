-----------------------------------------------
-- mixed traffic with all available data,
-- also bike lanes of substandard width
-- (uses tables in DB, based on Furth 2017 with
-- some adjustments)
-----------------------------------------------

-- #reading given and assumed road characteristics;
DROP TABLE IF EXISTS pg_temp.tmp_attrs;

-- two way
CREATE TEMP TABLE tmp_attrs AS (
    SELECT
        {id_column}::INTEGER AS id,
        COALESCE({lanes},{assumed_lanes})::INTEGER AS lanes,
        CASE
            WHEN COALESCE({lanes},{assumed_lanes})::INTEGER > 0 THEN TRUE
            ELSE COALESCE({centerline},{assumed_centerline})::BOOLEAN
            END AS marked_centerline,
        COALESCE({speed},{assumed_speed})::INTEGER AS speed,
        COALESCE({width},{assumed_width})::INTEGER AS width,
        COALESCE({parking},{assumed_parking})::BOOLEAN AS parking,
        COALESCE({aadt},{assumed_aadt})::INTEGER AS effective_aadt
    FROM
        {in_schema}.{in_table}
    WHERE
        ({shared})
        AND {filter}
        AND {twoway}
);

-- one way
INSERT INTO pg_temp.tmp_attrs
SELECT
    {id_column}::INTEGER,
    COALESCE({lanes},{assumed_lanes})::INTEGER,
    CASE
        WHEN COALESCE({lanes},{assumed_lanes})::INTEGER > 0 THEN TRUE
        ELSE COALESCE({centerline},{assumed_centerline})::BOOLEAN
        END AS marked_centerline,
    COALESCE({speed},{assumed_speed})::INTEGER,
    COALESCE({width},{assumed_width})::INTEGER,
    COALESCE({parking},{assumed_parking})::BOOLEAN,
    (COALESCE({aadt},{assumed_aadt})*1.67)::INTEGER
FROM
    {in_schema}.{in_table}
WHERE
    ({shared})
    AND {filter}
    AND {oneway}
;

CREATE INDEX tidx_tmp_attrs_id ON pg_temp.tmp_attrs (id); ANALYZE pg_temp.tmp_attrs;


-- #comparing against LTS tables;
DROP TABLE IF EXISTS pg_temp.tmp_stress;
CREATE TEMP TABLE pg_temp.tmp_stress AS (
    SELECT DISTINCT ON (tmp_attrs.id)
        tmp_attrs.id,
        lts.stress
    FROM
        pg_temp.tmp_attrs,
        {shared_lts_schema}.{shared_lts_table} lts
    WHERE
        tmp_attrs.lanes <= lts.lanes
        AND tmp_attrs.marked_centerline = lts.marked_centerline
        AND tmp_attrs.speed <= lts.speed
        AND tmp_attrs.width <= lts.width
        AND tmp_attrs.effective_aadt <= lts.effective_aadt
    ORDER BY
        tmp_attrs.id,
        (tmp_attrs.lanes = lts.lanes) DESC, --ensures that we give priority to an exact match
        (tmp_attrs.speed = lts.speed) DESC, --ensures that we give priority to an exact match
        lts.width ASC,
        (tmp_attrs.parking = lts.parking) DESC, --ensures that we give priority to an exact match
        lts.stress ASC
);

CREATE INDEX tidx_tmp_stress_id ON pg_temp.tmp_stress (id); ANALYZE pg_temp.tmp_stress;

INSERT INTO {out_schema}.{out_table} (
    {id_column},
    {geom},
    lanes,
    marked_centerline,
    speed,
    effective_aadt,
    stress
)
SELECT
    tmp_attrs.id,
    {in_table}.geom,
    tmp_attrs.lanes,
    tmp_attrs.marked_centerline,
    tmp_attrs.speed,
    tmp_attrs.effective_aadt,
    tmp_stress.stress
FROM
    pg_temp.tmp_attrs,
    {in_schema}.{in_table},
    pg_temp.tmp_stress
WHERE
    tmp_attrs.id = {in_table}.{id_column}
    AND tmp_attrs.id = tmp_stress.id
;
