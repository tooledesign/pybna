SELECT DISTINCT ({roads_id_col})
FROM {projects_schema}.{projects_table}
WHERE
    {projects_col} IS NOT NULL
    AND {projects_col} != {project_id}
;
