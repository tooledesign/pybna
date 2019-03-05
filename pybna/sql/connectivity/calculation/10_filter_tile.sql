DROP TABLE IF EXISTS tmp_tile;
CREATE TEMP TABLE tmp_tile AS (
    SELECT
        {tiles_id_col} AS id,
        {tiles_geom_col} AS geom
    FROM {tiles_schema}.{tiles_table}
    WHERE {tiles_id_col} = {tile_id}
);
