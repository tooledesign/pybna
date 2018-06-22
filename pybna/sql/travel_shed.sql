INSERT INTO {schema}.{table} AS shed (
    geom, source_blockid, target_blockid, high_stress, low_stress
)
SELECT
    blocks.{geom},
    conn.{source_blockid},
    conn.{target_blockid},
    conn.high_stress,
    conn.low_stress
FROM
    {blocks_schema}.{blocks} blocks,
    {connectivity} conn
WHERE
    blocks.{block_id_col} = conn.{target_blockid}
    AND conn.{source_blockid} = {block_id};

CREATE INDEX {sidx} ON {schema}.{table} USING GIST (geom);
CREATE INDEX {idx} ON {schema}.{table} (source_blockid);
