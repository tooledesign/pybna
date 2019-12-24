--
-- calculates access to a destination type using counts and score thresholds
--
DROP TABLE IF EXISTS pg_temp.tmp_dests;
CREATE TEMP TABLE pg_temp.tmp_dests AS (
    SELECT
        {destinations_id_col} AS id,
        {destinations_geom_col} AS geom
    FROM {destinations_schema}.{destinations_table} destinations
    WHERE {destinations_filter}
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
    WHERE ST_Intersects(tmp_dests.geom,blocks.{blocks_geom_col})
);
CREATE INDEX tidx_tmp_dest_blocks ON pg_temp.tmp_dest_blocks (block_id);
ANALYZE pg_temp.tmp_dest_blocks;


DROP TABLE IF EXISTS pg_temp.{tbl};
CREATE TEMP TABLE pg_temp.{tbl} AS (
    SELECT
        connections.source AS block_id,
        COUNT(DISTINCT tmp_dest_blocks.dest_id) AS total
    FROM
        pg_temp.tmp_connectivity connections,
        pg_temp.tmp_dest_blocks
    WHERE
        {connection_true}
        AND connections.target = tmp_dest_blocks.block_id
    GROUP BY connections.source
);
