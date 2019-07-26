DROP TABLE IF EXISTS pg_temp.tmp_control;
SELECT DISTINCT ON (tmp_combineddirs.id)
    tmp_combineddirs.id,
    control.{control_column} AS control
INTO TEMP TABLE tmp_control
FROM
    tmp_combineddirs,
    {control_schema}.{control_table} AS control
WHERE
    ST_DWithin(tmp_combineddirs.{point},control.{control_geom},{intersection_tolerance})
ORDER BY
    tmp_combineddirs.id,
    ST_Distance(tmp_combineddirs.{point},control.{control_geom}) ASC
;
