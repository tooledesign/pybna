CREATE TABLE {connectivity_schema}.{connectivity_table} (
    id SERIAL PRIMARY KEY,
    {connectivity_source_col} {blocks_id_type},
    {connectivity_target_col} {blocks_id_type},
    high_stress BOOLEAN,
    low_stress BOOLEAN
);
