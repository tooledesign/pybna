--
-- calculates access to a destination type from each census block
--
DROP TABLE IF EXISTS pg_temp.tmp_dests;
CREATE TEMP TABLE pg_temp.tmp_dests AS (
    SELECT
        {destinations_id_col} AS id,
        {val} AS val,
        {destinations_geom_col} AS geom
    FROM {destinations_schema}.{destinations_table} destinations
    WHERE {destinations_filter}
);
CREATE INDEX tsidx_tmp_dests ON pg_temp.tmp_dests USING GIST (geom);
ANALYZE pg_temp.tmp_dests;


CREATE TEMP TABLE pg_temp.{tmp_table} AS (
    SELECT
        connections.{connectivity_source_col} AS block_id,
        SUM(target_block.val) AS total
    FROM
        {connectivity_schema}.{connectivity_table} connections,
        pg_temp.tmp_dests target_block
    WHERE
        {connection_true}
        AND connections.{connectivity_target_col} = target_block.id
    GROUP BY connections.{connectivity_source_col}
);

CREATE INDEX {index} ON pg_temp.{tmp_table} (block_id); ANALYZE pg_temp.{tmp_table};
