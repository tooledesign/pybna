SELECT DISTINCT blocks.{blocks_id_col}
FROM
    {blocks_schema}.{blocks_table} blocks,
    {roads_schema}.{roads_table} roads
WHERE
    roads.{roads_scenario_col} = {scenario_id}
    AND ST_DWithin(blocks.{blocks_geom_col},roads.{roads_geom_col},{connectivity_max_distance})
    AND EXISTS (
        SELECT 1
        FROM {boundary_schema}.{boundary_table} bound
        WHERE ST_Intersects(blocks.geom,bound.geom)
    )
;
