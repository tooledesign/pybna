--
-- calculates access to a destination type from each census block
--
DROP TABLE IF EXISTS pg_temp.tmp_dests;
CREATE TEMP TABLE pg_temp.tmp_dests AS (
    SELECT
        {destinations_id_col} AS id,
        {val} AS val
    FROM {destinations_schema}.{destinations_table} destinations
    WHERE {destinations_filter}
);


CREATE TEMP TABLE pg_temp.{tmp_table} AS (
    SELECT
        connections.source AS block_id,
        SUM(target_block.val) AS total
    FROM
        pg_temp.tmp_connectivity connections,
        pg_temp.tmp_dests target_block
    WHERE
        {connection_true}
        AND connections.target = target_block.id
    GROUP BY connections.source
);

CREATE INDEX {index} ON pg_temp.{tmp_table} (block_id); ANALYZE pg_temp.{tmp_table};
