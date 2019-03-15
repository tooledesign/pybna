DROP TABLE IF EXISTS tmp_this_block;
CREATE TEMP TABLE tmp_this_block AS (
    SELECT
        {blocks_id_col} AS id,
        {blocks_geom_col} AS geom
    FROM {blocks_schema}.{blocks_table}
    WHERE {blocks_id_col} = {block_id}
);
