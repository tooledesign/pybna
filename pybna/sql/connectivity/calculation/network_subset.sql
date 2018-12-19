DROP TABLE IF EXISTS {net_table};
SELECT
    link.{edges_id_col} AS id,
    link.{edges_source_col} AS source,
    link.{edges_target_col} AS target,
    link.{edges_cost_col} AS cost
INTO TEMP TABLE {net_table}
FROM
    {edges_schema}.{edges_table} link,
    {tiles_schema}.{tiles_table} tile
WHERE
    tile.{tiles_id_col}={tile_id}
    AND ST_DWithin(tile.{tiles_geom_col},link.{edges_geom_col},{connectivity_max_distance})
    AND {edges_stress_col} <= {max_stress}
    AND {network_filter}
;
