DROP TABLE IF EXISTS {net_table};
SELECT
    link.{edges_id_col} AS id,
    link.{edges_source_col} AS source,
    link.{edges_target_col} AS target,
    link.{edges_cost_col} AS cost
INTO TEMP TABLE {net_table}
FROM
    {edges_schema}.{edges_table} link
    JOIN {blocks_schema}.{blocks_table} block
        ON TRUE
    LEFT JOIN tmp_flip_stress
        ON link.{edges_id_col} = tmp_flip_stress.id
WHERE
    block.{blocks_id_col}={block_id}
    AND ST_DWithin(block.{blocks_geom_col},link.{edges_geom_col},{connectivity_max_distance})
    AND COALESCE(tmp_flip_stress.stress,{edges_stress_col}) <= {max_stress}
    AND COALESCE(tmp_flip_stress.stress,{edges_stress_col}) > 0
    AND {network_filter}
;
