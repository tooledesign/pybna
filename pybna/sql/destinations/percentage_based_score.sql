--
-- calculates access to a destination type from each census block
--
SELECT
    connections.{source_block} AS block_id,
    SUM(target_block.{val}) AS total
INTO TEMP TABLE pg_temp.{tmp_table}
FROM
    {block_connections} connections,
    {schema}.{table} target_block
WHERE
    {connection_true}
    AND connections.{target_block} = target_block.{block_id_col}
GROUP BY connections.{source_block};

CREATE INDEX {index} ON pg_temp.{tmp_table} (block_id); ANALYZE pg_temp.{tmp_table};
