SELECT
    blocks.{geom} AS geom,
    conn.{source_blockid} AS source_blockid,
    conn.{target_blockid} AS target_blockid,
    conn.high_stress AS high_stress,
    conn.low_stress AS low_stress
INTO {schema}.{table}
FROM
    {blocks_schema}.{blocks} blocks,
    {connectivity} conn,
    unnest({block_ids}) AS block_ids(block)
WHERE
    conn.{source_blockid} = block_ids.block
    AND blocks.{block_id_col} = conn.{target_blockid};

ALTER TABLE {schema}.{table} ADD COLUMN id SERIAL PRIMARY KEY;
CREATE INDEX {sidx} ON {schema}.{table} USING GIST (geom);
CREATE INDEX {idx} ON {schema}.{table} (source_blockid);
