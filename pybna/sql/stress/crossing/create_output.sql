CREATE TABLE {out_schema}.{out_table} AS (
    SELECT
        {id_column},
        {geom},
        NULL::INTEGER AS legs,
        NULL::BOOLEAN AS priority,
        NULL::TEXT AS control,
        NULL::INTEGER AS lanes,
        NULL::INTEGER AS speed,
        NULL::BOOLEAN AS island,
        NULL::INTEGER AS stress
    FROM {in_schema}.{in_table}
    WHERE FALSE
);
