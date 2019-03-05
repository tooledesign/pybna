SELECT
    {blocks_id_col} AS {connectivity_source_col},
    {blocks_id_col} AS {connectivity_target_col},
    FALSE::BOOLEAN AS high_stress,
    FALSE::BOOLEAN AS low_stress
INTO {connectivity_schema}.{connectivity_table}
FROM {blocks_schema}.{blocks_table}
WHERE FALSE;

DELETE FROM {connectivity_schema}.{connectivity_table};

ALTER TABLE {connectivity_schema}.{connectivity_table}
    ADD PRIMARY KEY ({connectivity_source_col},{connectivity_target_col})
;
