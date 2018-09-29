SELECT
    link.{link_id_col},
    source_vert AS source,
    target_vert AS target,
    {link_cost_col} AS cost
FROM
    {link_table} link,
    {tiles_table} tile
WHERE
    tile.{tile_id_col}={tile_id}
    AND ST_DWithin(tile.{tile_geom_col},link.geom,{max_trip_distance})
    AND {link_stress_col} <= {max_stress}
    AND {filter}
