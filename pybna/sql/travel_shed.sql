SELECT
    blocks.{blocks_geom_col} AS geom,
    conn.{connectivity_source_col} AS source_blockid,
    conn.{connectivity_target_col} AS target_blockid,
    conn.high_stress AS high_stress,
    conn.low_stress AS low_stress
INTO {schema}.{table}
FROM
    {blocks_schema}.{blocks_table} blocks,
    {connectivity_schema}.{connectivity_table} conn,
    unnest({block_ids}) AS block_ids(block)
WHERE
    conn.{connectivity_source_col} = block_ids.block
    AND blocks.{blocks_id_col} = conn.{connectivity_target_col};

ALTER TABLE {schema}.{table} ADD COLUMN id SERIAL PRIMARY KEY;
CREATE INDEX {sidx} ON {schema}.{table} USING GIST (geom);
CREATE INDEX {idx} ON {schema}.{table} (source_blockid);
