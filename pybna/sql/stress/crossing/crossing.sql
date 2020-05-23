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
