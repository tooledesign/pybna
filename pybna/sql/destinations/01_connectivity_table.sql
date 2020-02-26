DROP TABLE IF EXISTS pg_temp.tmp_connectivity;
CREATE TEMP TABLE pg_temp.tmp_connectivity AS (
    SELECT
        {connectivity_source_col} AS source,
        {connectivity_target_col} AS target,
        high_stress,
        low_stress
    FROM {connectivity_schema}.{connectivity_table}
    {scenario_where}
);

CREATE INDEX tidx_conn ON pg_temp.tmp_connectivity (source,target) WHERE low_stress;
ANALYZE pg_temp.tmp_connectivity;
