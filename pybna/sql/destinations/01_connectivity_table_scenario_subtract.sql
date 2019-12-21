DROP TABLE IF EXISTS pg_temp.tmp_connectivity;
CREATE TEMP TABLE pg_temp.tmp_connectivity AS (
    SELECT DISTINCT ON (source,target)
        {connectivity_source_col} AS source,
        {connectivity_target_col} AS target,
        high_stress,
        low_stress
    FROM {connectivity_schema}.{connectivity_table}
    WHERE (scenario = {scenario_id} AND subtract) OR scenario IS NULL
    ORDER BY
        source,
        target,
        (scenario = {scenario_id}) ASC
);

CREATE INDEX tidx_conn ON pg_temp.tmp_connectivity (source,target) WHERE low_stress;
ANALYZE pg_temp.tmp_connectivity;
