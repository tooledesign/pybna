CREATE TABLE {ints_schema}.{ints_table} (
    {ints_id_col} SERIAL PRIMARY KEY,
    {ints_geom_col} geometry(point,{srid}),
    control TEXT,
    island BOOLEAN
);
