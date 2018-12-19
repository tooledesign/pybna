DROP TABLE IF EXISTS tmp_tile;
SELECT
    {tiles_id_col} AS id,
    {tiles_geom_col} AS geom
INTO TEMP TABLE tmp_tile
FROM {tiles_schema}.{tiles_table}
WHERE {tiles_id_col} = {tile_id}
;

-- filtering to tile;
DROP TABLE IF EXISTS tmp_tileunits;
SELECT
    units.{units_id_col} AS id,
    units.node_ids
INTO TEMP TABLE tmp_tileunits
FROM
    {units_schema}.{units_table} units,
    tmp_tile
WHERE
    units.node_ids IS NOT NULL
    AND ST_Intersects(tmp_tile.geom,units.{units_geom_col})
    AND ST_Intersects(tmp_tile.geom,ST_Centroid(units.{units_geom_col}))
;
