SELECT DISTINCT
    blocks.{geom} AS geom,
    array_agg(conn.{source_blockid}) AS source_blockids,
    conn.{target_blockid} AS target_blockid,
    MAX(conn.high_stress::INTEGER)::BOOLEAN AS high_stress,
    MAX(conn.low_stress::INTEGER)::BOOLEAN AS low_stress
INTO {schema}.{table}
FROM
    {blocks_schema}.{blocks} blocks,
    {connectivity} conn,
    unnest({block_ids}) AS block_ids(block)
WHERE
    conn.{source_blockid} = block_ids.block
    AND blocks.{block_id_col} = conn.{target_blockid}
GROUP BY
    conn.{target_blockid},
    blocks.{geom};

ALTER TABLE {schema}.{table} ADD COLUMN id SERIAL PRIMARY KEY;
CREATE INDEX {sidx} ON {schema}.{table} USING GIST (geom);
ANALYZE {schema}.{table};
