UDPATE {zones_schema}.{zones_table}
SET block_ids = NULL
;

-- define zone-block combinations by assigning each block to the zone
-- it overlaps most with
DROP TABLE IF EXISTS tmp_zones_blocks;
SELECT DISTINCT ON (block_id)
    zones.{zones_id_col} AS zone_id,
    blocks.{blocks_id_col} AS block_id
FROM
    {zones_schema}.{zones_table} zones,
    {blocks_schema}.{blocks_table} blocks
WHERE ST_Intersects(zones.{zones_geom_col},blocks.{blocks_geom_col})
ORDER BY
    block_id,
    ST_Area(ST_Intersection(zones.{zones_geom_col},blocks.{blocks_geom_col})) DESC
;

DROP TABLE IF EXISTS tmp_block_agg;
SELECT
    zone_id,
    array_agg(block_id) AS block_ids
INTO tmp_block_agg
FROM tmp_zones_blocks
GROUP BY zone_id
;

-- aggregate blocks within zones
UPDATE {zones_schema}.{zones_table} zones
SET block_ids = tmp_block_agg.block_ids
FROM tmp_block_agg
WHERE tmp_block_agg.zone_id = zones.{zones_id_col}
;
