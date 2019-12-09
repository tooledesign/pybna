INSERT INTO {connectivity_schema}.{connectivity_table} (
    source,target,hs,ls,scenario
)
SELECT
    source,
    target,
    hs,
    ls,
    {project_id}
FROM tmp_connectivity
;

DROP TABLE tmp_connectivity;
