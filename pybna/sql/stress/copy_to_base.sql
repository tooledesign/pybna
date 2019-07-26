UPDATE {in_schema}.{in_table} SET {stress} = NULL;

UPDATE {in_schema}.{in_table} base
SET {stress} = out.stress
FROM {out_schema}.{out_table} out
WHERE base.{id_column} = out.{id_column}
;
