--
-- This script is used to save a copy of the scenario edges
--
CREATE TABLE {output_schema}.{output_table} AS (
    SELECT
        link.{edges_id_col},
        link.{edges_geom_col},
        link.{edges_source_col},
        link.{edges_target_col},
        link.{edges_cost_col},
        COALESCE(tmp_flip_stress.stress,link.{edges_stress_col}) AS {edges_stress_col}
    FROM
        {edges_schema}.{edges_table} link
        LEFT JOIN tmp_flip_stress
            ON link.{edges_id_col} = tmp_flip_stress.id
);
