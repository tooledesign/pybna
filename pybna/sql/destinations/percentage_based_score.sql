--
-- calculates access to a destination type from each census block
--
DROP TABLE IF EXISTS pg_temp.tmp_dests;
CREATE TEMP TABLE pg_temp.tmp_dests AS (
    SELECT
        {destination_id} AS id,
        {val} AS val,
        {geom} AS geom
    FROM {destinations_schema}.{destinations_table} destinations
    WHERE {filter}
);
CREATE INDEX tsidx_tmp_dests ON pg_temp.tmp_dests USING GIST (geom);
ANALYZE pg_temp.tmp_dests;


CREATE TEMP TABLE pg_temp.{tmp_table} AS (
    SELECT
        connections.{source_block} AS block_id,
        SUM(target_block.val) AS total
    FROM
        {block_connections} connections,
        pg_temp.tmp_dests target_block
    WHERE
        {connection_true}
        AND connections.{target_block} = target_block.id
    GROUP BY connections.{source_block}
);

CREATE INDEX {index} ON pg_temp.{tmp_table} (block_id); ANALYZE pg_temp.{tmp_table};
