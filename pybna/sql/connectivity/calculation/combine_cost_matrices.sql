-- combine hs and ls results
DROP TABLE IF EXISTS tmp_combined;
SELECT
    COALESCE(hs.id,ls.id) AS id,
    hs.agg_cost AS hs_cost,
    ls.agg_cost AS ls_cost
INTO TEMP TABLE tmp_combined
FROM
    tmp_hs_cost_to_units hs
    FULL OUTER JOIN
    tmp_ls_cost_to_units ls
        ON hs.id = ls.id
;

DROP TABLE tmp_hs_cost_to_units;
DROP TABLE tmp_ls_cost_to_units;

-- build connectivity table
DROP TABLE IF EXISTS tmp_connectivity;
SELECT
    ounits.block_id AS source,
    dunits.block_id AS target,
    (hs_cost IS NOT NULL)::BOOLEAN AS hs,
    (
        hs_cost IS NULL
        OR ls_cost <= {connectivity_detour_agnostic_threshold}
        OR ls_cost <= ({connectivity_max_detour} * hs_cost)
    )::BOOLEAN AS ls
INTO TEMP TABLE tmp_connectivity
FROM
    tmp_unit_blocks ounits,
    tmp_unit_blocks dunits,
    tmp_combined
WHERE
    ounits.id = {unit_id}
    AND tmp_combined.id = dunits.id
;

UPDATE tmp_connectivity
SET
    hs = TRUE,
    ls = TRUE
WHERE source = target
;

DROP TABLE tmp_unit_blocks;
DROP TABLE tmp_combined;

INSERT INTO {connectivity_schema}.{connectivity_table}
SELECT * FROM tmp_connectivity
;

DROP TABLE tmp_connectivity;
