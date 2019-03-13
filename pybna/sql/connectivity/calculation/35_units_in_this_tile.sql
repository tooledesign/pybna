SELECT
    blocks.{blocks_id_col} AS id,
    array_agg(tmp_unit_nodes.node_id) AS node_ids
FROM
    {blocks_schema}.{blocks_table} blocks,
    tmp_unit_nodes,
    tmp_tile
WHERE
    blocks.{blocks_id_col} = tmp_unit_nodes.id
    AND ST_Intersects(tmp_tile.geom,blocks.{blocks_geom_col})
    AND ST_Intersects(tmp_tile.geom,ST_Centroid(blocks.{blocks_geom_col}))
GROUP BY blocks.{blocks_id_col}
