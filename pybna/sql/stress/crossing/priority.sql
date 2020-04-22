DROP TABLE IF EXISTS pg_temp.tmp_priority;
CREATE TEMP TABLE tmp_priority (id INTEGER);

{priority_assignment}

UPDATE {out_schema}.{out_table}
SET
    priority = TRUE,
    stress = 1
FROM
    tmp_priority,
    tmp_intcount
WHERE
    {out_table}.{id_column} = tmp_priority.id
    AND tmp_priority.id = tmp_intcount.id
;
