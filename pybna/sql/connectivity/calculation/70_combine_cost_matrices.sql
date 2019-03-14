-- combine hs and ls results
DROP TABLE IF EXISTS tmp_combined;
SELECT
    COALESCE(hs.id,ls.id) AS id,
    hs.agg_cost AS hs_cost,
    ls.agg_cost AS ls_cost
INTO TEMP TABLE tmp_combined
FROM
    tmp_hs_cost_to_blocks hs
    FULL OUTER JOIN
    tmp_ls_cost_to_blocks ls
        ON hs.id = ls.id
;

DROP TABLE tmp_hs_cost_to_blocks;
DROP TABLE tmp_ls_cost_to_blocks;

-- build connectivity table
DROP TABLE IF EXISTS tmp_connectivity;
SELECT
    oblocks.id AS source,
    dblocks.id AS target,
    (hs_cost IS NOT NULL)::BOOLEAN AS hs,
    (
        hs_cost IS NULL
        OR ls_cost <= {connectivity_detour_agnostic_threshold}
        OR ls_cost <= ({connectivity_max_detour} * hs_cost)
    )::BOOLEAN AS ls
INTO TEMP TABLE tmp_connectivity
FROM
    tmp_blocks oblocks,
    tmp_blocks dblocks,
    tmp_combined
WHERE
    oblocks.id = {block_id}
    AND tmp_combined.id = dblocks.id
;

UPDATE tmp_connectivity
SET
    hs = TRUE,
    ls = TRUE
WHERE source = target
;

DROP TABLE tmp_blocks;
DROP TABLE tmp_combined;

INSERT INTO {connectivity_schema}.{connectivity_table}
SELECT * FROM tmp_connectivity
;

DROP TABLE tmp_connectivity;
