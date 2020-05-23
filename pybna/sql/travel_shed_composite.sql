SELECT DISTINCT
    blocks.{blocks_geom_col} AS geom,
    array_agg(conn.source) AS source_blockids,
    conn.target AS target_blockid,
    MAX(conn.high_stress::INTEGER)::BOOLEAN AS high_stress,
    MAX(conn.low_stress::INTEGER)::BOOLEAN AS low_stress
INTO {schema}.{table}
FROM
    {blocks_schema}.{blocks_table} blocks,
    pg_temp.tmp_connectivity conn,
    unnest({block_ids}) AS block_ids(block)
WHERE
    conn.source = block_ids.block
    AND blocks.{blocks_id_col} = conn.target
GROUP BY
    conn.target,
    blocks.{blocks_geom_col};

ALTER TABLE {schema}.{table} ADD COLUMN id SERIAL PRIMARY KEY;
CREATE INDEX {sidx} ON {schema}.{table} USING GIST (geom);
ANALYZE {schema}.{table};
