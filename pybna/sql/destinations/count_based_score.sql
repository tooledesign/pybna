--
-- calculates access to a destination type using counts and score thresholds
--
DROP TABLE IF EXISTS pg_temp.tmp_dests;
CREATE TEMP TABLE pg_temp.tmp_dests AS (
    SELECT
        {destination_id} AS id,
        {geom} AS geom
    FROM {destinations_schema}.{destinations_table} destinations
    WHERE {filter}
);
CREATE INDEX tsidx_tmp_dests ON pg_temp.tmp_dests USING GIST (geom);
ANALYZE pg_temp.tmp_dests;


DROP TABLE IF EXISTS pg_temp.tmp_dest_blocks;
CREATE TEMP TABLE pg_temp.tmp_dest_blocks AS (
    SELECT
        tmp_dests.id AS dest_id,
        blocks.{blocks_id_col} AS block_id
    FROM
        pg_temp.tmp_dests,
        {blocks_schema}.{blocks_table} blocks
);
CREATE INDEX tidx_tmp_dest_blocks ON pg_temp.tmp_dest_blocks (block_id);
ANALYZE pg_temp.tmp_dest_blocks;


CREATE TEMP TABLE pg_temp.{tmp_table} AS (
    SELECT
        connections.{source_block} AS block_id,
        COUNT(DISTINCT tmp_dest_blocks.dest_id) AS total
    FROM
        {block_connections} connections,
        pg_temp.tmp_dest_blocks
    WHERE
        {connection_true}
        AND connections.{target_block} = tmp_dest_blocks.block_id
    GROUP BY connections.{source_block}
);

CREATE INDEX {index} ON pg_temp.{tmp_table} (block_id); ANALYZE pg_temp.{tmp_table};
