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
    ozones.{zones_id_col} AS o_id,
    dzones.{zones_id_col} AS d_id,
    ozones.node_ids AS o_nodes,
    dzones.node_ids AS d_nodes
INTO TEMP TABLE tmp_tilezones
FROM
    {zones_schema}.{zones_table} ozones,
    {zones_schema}.{zones_table} dzones,
    tmp_tile
WHERE
    ST_Intersects(tmp_tile.geom,ozones.{zones_geom_col})
    AND ST_Intersects(tmp_tile.geom,ST_Centroid(ozones.{zones_geom_col}))
    AND ST_DWithin(ozones.{zones_geom_col},dzones.{zones_geom_col},{connectivity_max_distance})
;

-- analyzing shortest high stress routes;
DROP TABLE IF EXISTS tmp_allverts;
SELECT
    tmp_allverts.o_id,
    route.agg_cost
INTO TEMP TABLE tmp_allverts
FROM
    tmp_tilezones
    pgr_drivingdistance(
        {hs_link_query},
        tmp_tilezones.o_nodes,
        {connectivity_max_distance},
        equicost:=TRUE,
        directed:=TRUE
    ) route
;
