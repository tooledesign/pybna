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
        bna_CompareAzimuths(this.{line},that.forward_ln) AS angle
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
        bna_CompareAzimuths(this.{line},that.backward_ln) AS angle
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
