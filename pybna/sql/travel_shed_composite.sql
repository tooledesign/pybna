SELECT DISTINCT
    blocks.{blocks_geom_col} AS geom,
    array_agg(conn.{connectivity_source_col}) AS source_blockids,
    conn.{connectivity_target_col} AS target_blockid,
    MAX(conn.high_stress::INTEGER)::BOOLEAN AS high_stress,
    MAX(conn.low_stress::INTEGER)::BOOLEAN AS low_stress
INTO {schema}.{table}
FROM
    {blocks_schema}.{blocks_table} blocks,
    {connectivity_schema}.{connectivity_table} conn,
    unnest({block_ids}) AS block_ids(block)
WHERE
    conn.{connectivity_source_col} = block_ids.block
    AND blocks.{blocks_id_col} = conn.{connectivity_target_col}
GROUP BY
    conn.{connectivity_target_col},
    blocks.{blocks_geom_col};

ALTER TABLE {schema}.{table} ADD COLUMN id SERIAL PRIMARY KEY;
CREATE INDEX {sidx} ON {schema}.{table} USING GIST (geom);
ANALYZE {schema}.{table};
