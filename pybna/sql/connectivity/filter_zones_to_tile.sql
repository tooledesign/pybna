DROP TABLE IF EXISTS tmp_tile;
SELECT
    {tiles_id_col} AS id,
    {tiles_geom_col} AS geom
INTO TEMP TABLE tmp_tile
FROM {tiles_schema}.{tiles_table}
WHERE {tiles_id_col} = {tile_id}
;

-- filtering zones to tile;
DROP TABLE IF EXISTS tmp_tilezones;
SELECT
    zones.{zones_id_col} AS id,
    zones.node_ids
INTO TEMP TABLE tmp_tilezones
FROM
    {zones_schema}.{zones_table} zones,
    tmp_tile
WHERE
    zones.node_ids IS NOT NULL
    AND ST_Intersects(tmp_tile.geom,zones.{zones_geom_col})
    AND ST_Intersects(tmp_tile.geom,ST_Centroid(zones.{zones_geom_col}))
;
