--
-- Creates a table of road_ids that flip to low stress
--
DROP TABLE IF EXISTS tmp_flip_stress;
CREATE TEMP TABLE tmp_flip_stress AS (
    SELECT {edges_id_col} AS id, {connectivity_max_stress} AS stress
    FROM {edges_schema}.{edges_table}
    WHERE
        source_road_id = ANY({low_stress_road_ids})
        OR target_road_id = ANY({low_stress_road_ids})
);
