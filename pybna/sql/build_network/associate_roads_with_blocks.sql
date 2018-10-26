-- buffering blocks;
DROP TABLE IF EXISTS tmp_blocks;
SELECT
    blocks.{blocks_id_col} AS id,
    ST_Buffer(blocks.{blocks_geom_col},{blocks_roads_tolerance}) AS geom
INTO TEMP TABLE tmp_blocks
FROM
    {blocks_schema}.{blocks_table} blocks
;

CREATE INDEX tsidx_b ON tmp_blocks USING GIST (geom);
ANALYZE tmp_blocks;

-- finding matching roads;
DROP TABLE IF EXISTS tmp_blkrds1;
SELECT
    blocks.id AS block_id,
    blocks.geom AS block_geom,
    roads.{roads_id_col} AS road_id,
    roads.{roads_geom_col} AS road_geom
INTO TEMP TABLE tmp_blkrds1
FROM
    tmp_blocks blocks,
    {roads_schema}.{roads_table} roads
WHERE ST_Intersects(blocks.geom,roads.geom)
;

DROP TABLE IF EXISTS tmp_blkrds2;
SELECT
    block_id,
    array_agg(road_id) AS road_ids
INTO TEMP TABLE tmp_blkrds2
FROM tmp_blkrds1
WHERE
    ST_Contains(block_geom,road_geom)
    OR ST_Length(ST_Intersection(block_geom,road_geom)) > {blocks_min_road_length}
GROUP BY block_id
;

-- resetting road ids on blocks;
UPDATE {blocks_schema}.{blocks_table}
SET road_ids = NULL
;

UPDATE {blocks_schema}.{blocks_table} blocks
SET road_ids = tmp_blkrds2.road_ids
FROM tmp_blkrds2
WHERE blocks.{blocks_id_col} = tmp_blkrds2.block_id
;
