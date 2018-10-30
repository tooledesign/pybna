-- combine hs and ls results
DROP TABLE IF EXISTS tmp_combined;
SELECT
    COALESCE(hs.id,ls.id) AS id,
    hs.agg_cost AS hs_cost,
    ls.agg_cost AS ls_cost
INTO TEMP TABLE tmp_combined
FROM
    tmp_hs_cost_to_zones hs
    FULL OUTER JOIN
    tmp_ls_cost_to_zones ls
        ON hs.id = ls.id
;

DROP TABLE tmp_hs_cost_to_zones;
DROP TABLE tmp_ls_cost_to_zones;

-- flatten blocks
DROP TABLE IF EXISTS tmp_zone_blocks;
SELECT
    zones.{zones_id_col} AS id,
    unnest(zones.block_ids) AS block_id
INTO TEMP TABLE tmp_zone_blocks
FROM
    {zones_schema}.{zones_table} zones,
    tmp_tile
WHERE ST_DWithin(zones.{zones_geom_col},tmp_tile.geom,{connectivity_max_distance})
;

CREATE INDEX idx_tmp_zone_blocks_node_id ON tmp_zone_blocks (block_id);
ANALYZE tmp_zone_blocks;

-- build connectivity table
DROP TABLE IF EXISTS tmp_connectivity;
SELECT
    ozones.block_id AS source,
    dzones.block_id AS target,
    (hs_cost IS NOT NULL)::BOOLEAN AS hs,
    (hs_cost IS NULL OR ls_cost <= ({connectivity_max_detour} * hs_cost))::BOOLEAN AS ls
INTO TEMP TABLE tmp_connectivity
FROM
    tmp_zone_blocks ozones,
    tmp_zone_blocks dzones,
    tmp_combined
WHERE
    ozones.id = {zone_id}
    AND tmp_combined.id = dzones.id
;

DROP TABLE tmp_zone_blocks;
DROP TABLE tmp_combined;

INSERT INTO {connectivity_schema}.{connectivity_table}
SELECT * FROM tmp_connectivity
;

DROP TABLE tmp_connectivity;
