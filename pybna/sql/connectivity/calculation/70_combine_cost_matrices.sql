-- combine hs and ls results
DROP TABLE IF EXISTS tmp_combined;
SELECT
    COALESCE(hs.id::{blocks_id_type},ls.id::{blocks_id_type}) AS id,
    hs.agg_cost AS hs_cost,
    ls.agg_cost AS ls_cost
INTO TEMP TABLE tmp_combined
FROM
    tmp_hs_cost_to_blocks hs
    FULL OUTER JOIN
    tmp_ls_cost_to_blocks ls
        ON hs.id::{blocks_id_type} = ls.id::{blocks_id_type}
;

DROP TABLE tmp_hs_cost_to_blocks;
DROP TABLE tmp_ls_cost_to_blocks;

-- build connectivity table
DROP TABLE IF EXISTS tmp_connectivity;
SELECT
    oblocks.id::{blocks_id_type} AS source,
    dblocks.id::{blocks_id_type} AS target,
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
    oblocks.id::{blocks_id_type} = {block_id}::{blocks_id_type}
    AND tmp_combined.id::{blocks_id_type} = dblocks.id::{blocks_id_type}
;

UPDATE tmp_connectivity
SET
    hs = TRUE,
    ls = TRUE
WHERE source = target
;

DROP TABLE tmp_blocks;
DROP TABLE tmp_combined;
