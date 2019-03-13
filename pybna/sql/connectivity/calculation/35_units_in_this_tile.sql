SELECT
    units.{blocks_id_col} AS id,
    array_agg(tmp_unit_nodes.node_id) AS node_ids
FROM
    {blocks_schema}.{blocks_table} units,
    tmp_unit_nodes,
    tmp_tile
WHERE
    units.{blocks_id_col} = tmp_unit_nodes.id
    AND ST_Intersects(tmp_tile.geom,units.{blocks_geom_col})
    AND ST_Intersects(tmp_tile.geom,ST_Centroid(units.{blocks_geom_col}))
GROUP BY units.{blocks_id_col}
