SELECT
    blocks.{blocks_geom_col} AS geom,
    conn.source AS source_blockid,
    conn.target AS target_blockid,
    conn.high_stress AS high_stress,
    conn.low_stress AS low_stress
INTO {schema}.{table}
FROM
    {blocks_schema}.{blocks_table} blocks,
    pg_temp.tmp_connectivity conn,
    unnest({block_ids}) AS block_ids(block)
WHERE
    conn.source = block_ids.block
    AND blocks.{blocks_id_col} = conn.target;

ALTER TABLE {schema}.{table} ADD COLUMN id SERIAL PRIMARY KEY;
CREATE INDEX {sidx} ON {schema}.{table} USING GIST (geom);
CREATE INDEX {idx} ON {schema}.{table} (source_blockid);
