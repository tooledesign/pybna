DROP TABLE IF EXISTS pg_temp.tmp_island;
SELECT DISTINCT ON (tmp_combineddirs.id)
    tmp_combineddirs.id,
    island.{island_column} AS island
INTO TEMP TABLE tmp_island
FROM
    tmp_combineddirs,
    {island_schema}.{island_table} AS island
WHERE
    ST_DWithin(tmp_combineddirs.{point},island.{island_geom},{intersection_tolerance})
ORDER BY
    tmp_combineddirs.id,
    ST_Distance(tmp_combineddirs.{point},island.{island_geom}) ASC
;
