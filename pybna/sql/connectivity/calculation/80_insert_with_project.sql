INSERT INTO {connectivity_schema}.{connectivity_table} (
    {connectivity_source_col},
    {connectivity_target_col},
    high_stress,
    low_stress,
    scenario
)
SELECT
    source,
    target,
    hs,
    ls,
    {scenario_id}
FROM tmp_connectivity
;

DROP TABLE tmp_connectivity;
