SELECT
    link.{edges_id_col} AS id,
    link.{edges_source_col} AS source,
    link.{edges_target_col} AS target,
    link.{edges_cost_col} AS cost
FROM
    {edges_schema}.{edges_table} link,
    {blocks_schema}.{blocks_table} block
WHERE
    block.{blocks_id_col}={block_id}
    AND ST_DWithin(block.{blocks_geom_col},link.{edges_geom_col},{connectivity_max_distance})
    AND {edges_stress_col} <= {max_stress}
    AND {filter}
