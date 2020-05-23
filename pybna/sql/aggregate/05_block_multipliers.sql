DROP TABLE IF EXISTS tmp_total;
CREATE TEMP TABLE tmp_total AS (
    SELECT SUM(blocks.{blocks_population_col}) AS tot
    FROM
        {scores_schema}.{scores_table} scores,
        {blocks_schema}.{blocks_table} blocks
    WHERE scores.{blocks_id_col} = blocks.{blocks_id_col}
);

DROP TABLE IF EXISTS tmp_ratios;
CREATE TEMP TABLE tmp_ratios AS (
    SELECT
        scores.{blocks_id_col} AS id,
        COALESCE(blocks.{blocks_population_col},0)::FLOAT / tmp_total.tot AS ratio
    FROM
        {scores_schema}.{scores_table} scores,
        {blocks_schema}.{blocks_table} blocks,
        tmp_total
    WHERE scores.{blocks_id_col} = blocks.{blocks_id_col}
);

DROP TABLE IF EXISTS tmp_total;
