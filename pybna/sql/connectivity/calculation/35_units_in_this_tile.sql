SELECT
    units.{units_id_col} AS id,
    array_agg(tmp_unit_nodes.node_id) AS node_ids
FROM
    {units_schema}.{units_table} units,
    tmp_unit_nodes,
    tmp_tile
WHERE
    units.{units_id_col} = tmp_unit_nodes.id
    AND ST_Intersects(tmp_tile.geom,units.{units_geom_col})
    AND ST_Intersects(tmp_tile.geom,ST_Centroid(units.{units_geom_col}))
GROUP BY units.{units_id_col}
