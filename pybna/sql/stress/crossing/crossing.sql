-----------------------------------------------
-- bike lanes with all avaialble data
-- (uses tables in DB, based on Furth 2017 with
-- some adjustments based on direction from prime)
-----------------------------------------------

-- #reading given and assumed road characteristics;
{directional_attribute_aggregation}

DROP TABLE IF EXISTS pg_temp.tmp_intcount;
SELECT
    this.id,
    COUNT(that.id) AS legs
INTO TEMP TABLE tmp_intcount
FROM
    pg_temp.tmp_combineddirs this,
    pg_temp.tmp_combineddirs that
WHERE
    ST_DWithin(this.geom,that.geom,{intersection_tolerance})
    AND (
        ST_DWithin(this.{point},that.forward_pt,{intersection_tolerance})
        OR ST_DWithin(this.{point},that.backward_pt,{intersection_tolerance})
    )
GROUP BY this.id
;

DROP TABLE IF EXISTS pg_temp.tmp_allconnections;
CREATE TEMP TABLE pg_temp.tmp_allconnections AS (
    -- forward
    SELECT
        this.id AS this_id,
        that.id AS that_id,
        tdg_CompareAzimuths(this.{line},that.forward_ln) AS angle
    FROM
        pg_temp.tmp_combineddirs this,
        pg_temp.tmp_combineddirs that
    WHERE
        this.id != that.id
        AND ST_DWithin(this.{point},that.forward_pt,{intersection_tolerance})
    UNION ALL
    -- backward
    SELECT
        this.id AS this_id,
        that.id AS that_id,
        tdg_CompareAzimuths(this.{line},that.backward_ln) AS angle
    FROM
        pg_temp.tmp_combineddirs this,
        pg_temp.tmp_combineddirs that
    WHERE
        this.id != that.id
        AND ST_DWithin(this.{point},that.backward_pt,{intersection_tolerance})
);

{control_assignment}

{island_assignment}

DROP TABLE IF EXISTS pg_temp.tmp_cross_streets_assumed;
SELECT
    this.this_id AS id,
    tmp_control.control,
    MAX(that.lanes) AS lanes,
    MAX(that.speed) AS speed,
    tmp_island.island
INTO TEMP TABLE tmp_cross_streets_assumed
FROM
    tmp_allconnections this,
    tmp_intcount,
    tmp_combineddirs that,
    tmp_control,
    tmp_island
WHERE
    this.angle > {angle}
    AND this.this_id = tmp_intcount.id
    AND this.that_id = that.id
    AND this.this_id = tmp_control.id
    AND this.that_id = tmp_island.id
GROUP BY
    this.this_id,
    tmp_control.control,
    tmp_island.island
;

DROP TABLE IF EXISTS pg_temp.tmp_cross_streets;
SELECT
    actual.{id_column} AS id,
    COALESCE({cross_control},assumed.control) AS control,
    COALESCE({cross_lanes},assumed.lanes) AS lanes,
    COALESCE({cross_speed},assumed.speed) AS speed,
    COALESCE({cross_island},assumed.island) AS island
INTO TEMP TABLE tmp_cross_streets
FROM
    {in_schema}.{in_table} actual,
    tmp_cross_streets_assumed assumed
WHERE
    actual.{id_column} = assumed.id
;

DROP TABLE IF EXISTS pg_temp.tmp_stress;
SELECT DISTINCT ON (attrs.id)
    attrs.id,
    attrs.control,
    attrs.lanes,
    attrs.speed,
    attrs.island,
    lts.stress
INTO TEMP TABLE tmp_stress
FROM
    tmp_cross_streets AS attrs,
    {cross_lts_schema}.{cross_lts_table} lts
WHERE
    COALESCE(attrs.control,'none') = COALESCE(lts.control,'none')
    AND attrs.lanes <= lts.lanes
    AND attrs.speed <= lts.speed
    AND COALESCE(attrs.island,FALSE) = COALESCE(lts.island,FALSE)
ORDER BY
    attrs.id,
    lts.stress ASC
;

INSERT INTO {out_schema}.{out_table} (
    {id_column},
    {geom},
    legs,
    control,
    lanes,
    speed,
    island,
    stress
)
SELECT
    tmp_stress.id,
    {in_table}.geom,
    tmp_intcount.legs,
    tmp_stress.control,
    tmp_stress.lanes,
    tmp_stress.speed,
    tmp_stress.island,
    CASE WHEN tmp_intcount.legs = 2 THEN 1 ELSE tmp_stress.stress END
FROM
    {in_schema}.{in_table}
    LEFT JOIN tmp_stress
        ON tmp_stress.id = {in_table}.{id_column}
    LEFT JOIN tmp_intcount
        ON tmp_intcount.id = {in_table}.{id_column}
;

DROP TABLE IF EXISTS pg_temp.tmp_priority;
CREATE TEMP TABLE tmp_priority (id INTEGER);

{priority_assignment}

UPDATE {out_schema}.{out_table}
SET
    priority = TRUE,
    stress = 1
FROM
    tmp_priority,
    tmp_intcount
WHERE
    {out_table}.{id_column} = tmp_priority.id
    AND tmp_priority.id = tmp_intcount.id
;
