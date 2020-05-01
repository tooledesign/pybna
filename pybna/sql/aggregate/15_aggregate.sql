DROP TABLE IF EXISTS tmp_agg;
CREATE TEMP TABLE tmp_agg AS (
    SELECT SUM(
        COALESCE(scores.{scores_column},0) * tmp_ratios.ratio
    ) AS agg_score
    FROM
        {scores_schema}.{scores_table} scores,
        tmp_ratios
    WHERE scores.{blocks_id_col} = tmp_ratios.id
);

UPDATE {agg_schema}.{agg_table} agg
SET {scores_column} = tmp_agg.agg_score
FROM tmp_agg
WHERE agg.scenario = {scenario_name}
;
