DROP TABLE IF EXISTS pg_temp.{this_priority_table};
SELECT {id_column} AS id
INTO TEMP TABLE {this_priority_table}
FROM {in_schema}.{in_table}
WHERE {this_where_test}
;

DROP TABLE IF EXISTS pg_temp.{that_priority_table};
SELECT {id_column} AS id
INTO TEMP TABLE {that_priority_table}
FROM {in_schema}.{in_table}
WHERE {that_where_test}
;

INSERT INTO tmp_priority
SELECT DISTINCT tmp_allconnections.this_id AS id
FROM
    tmp_allconnections,
    {this_priority_table},
    {that_priority_table}
WHERE
    tmp_allconnections.this_id = {this_priority_table}.id
    AND tmp_allconnections.that_id = {that_priority_table}.id
;
