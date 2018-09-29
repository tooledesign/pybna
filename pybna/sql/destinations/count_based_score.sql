--
-- calculates access to a destination type using counts and score thresholds
--
DROP TABLE IF EXISTS pg_temp.d;

SELECT
    {destination_id} AS id,
    unnest({block_id_col}) AS block_id
INTO TEMP TABLE pg_temp.d
FROM {schema}.{table};
CREATE INDEX tidx_d_id ON pg_temp.d (id);
ANALYZE pg_temp.d;

SELECT
    connections.{source_block} AS block_id,
    COUNT(DISTINCT d.id) AS total
INTO TEMP TABLE pg_temp.{tmp_table}
FROM
    {block_connections} connections,
    pg_temp.d
WHERE
    {connection_true}
    AND connections.{target_block} = d.block_id
GROUP BY connections.{source_block};

CREATE INDEX {index} ON pg_temp.{tmp_table} (block_id); ANALYZE pg_temp.{tmp_table};
