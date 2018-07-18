-- filtering blocks to tile;
SELECT
    blocks.{block_id_col} AS id,
    blocks.{block_geom_col} AS geom,
    unnest(blocks.road_ids) AS road_id
INTO TEMP TABLE tmp_allblocks
FROM
    {blocks_table} blocks,
    {tiles_table} tile
WHERE
    tile.{tile_id_col}={tile_id}
    AND ST_DWithin(tile.{tile_geom_col},blocks.{block_geom_col},{max_trip_distance});

CREATE INDEX tsidx_tmp_allblocks ON tmp_allblocks USING GIST (geom);
ANALYZE tmp_allblocks;


-- analyzing shortest high stress routes;
SELECT *
INTO TEMP TABLE tmp_allverts
FROM pgr_johnson({hs_link_query},directed:=TRUE)
WHERE agg_cost < {max_trip_distance};

-- indexing high stress routes;
CREATE INDEX tidx_tmp_allverts ON tmp_allverts (start_vid, end_vid);
ANALYZE tmp_allverts;


-- applying high stress routes to blocks;
SELECT DISTINCT ON (source_id, target_id)
    source.id AS source_id,
    target.id AS target_id,
    tmp_allverts.agg_cost
INTO TEMP TABLE tmp_histress
FROM
    tmp_allblocks source,
    tmp_allblocks target,
    {vert_table} source_vert,
    {vert_table} target_vert,
    {tiles_table} tile,
    pg_temp.tmp_allverts
WHERE
    tile.{tile_id_col}={tile_id}
    AND ST_Intersects(source.geom,tile.{tile_geom_col})
    AND source.road_id = source_vert.{road_id}
    AND target.road_id = target_vert.{road_id}
    AND (
        tmp_allverts.start_vid = source_vert.{vert_id_col}
        AND tmp_allverts.end_vid = target_vert.{vert_id_col}
    )
ORDER BY
    source_id,
    target_id,
    agg_cost ASC;

DROP TABLE pg_temp.tmp_allverts;
CREATE INDEX tidx_histress ON tmp_histress (source_id,target_id);
ANALYZE tmp_histress;


-- analyzing shortest low stress routes;
SELECT *
INTO TEMP TABLE tmp_allverts
FROM pgr_johnson({ls_link_query},directed:=TRUE)
WHERE agg_cost < {max_trip_distance};

-- indexing low stress routes;
CREATE INDEX tidx_tmp_allverts ON tmp_allverts (start_vid, end_vid);
ANALYZE tmp_allverts;


-- applying low stress routes to blocks;
SELECT DISTINCT ON (source_id, target_id)
    source.id AS source_id,
    target.id AS target_id,
    tmp_allverts.agg_cost
INTO TEMP TABLE tmp_lostress
FROM
    tmp_allblocks source,
    tmp_allblocks target,
    {vert_table} source_vert,
    {vert_table} target_vert,
    {tiles_table} tile,
    pg_temp.tmp_allverts
WHERE
    tile.{tile_id_col}={tile_id}
    AND ST_Intersects(source.geom,tile.{tile_geom_col})
    AND source.road_id = source_vert.{road_id}
    AND target.road_id = target_vert.{road_id}
    AND (
        tmp_allverts.start_vid = source_vert.{vert_id_col}
        AND tmp_allverts.end_vid = target_vert.{vert_id_col}
    )
    AND source.id != target.id
ORDER BY
    source_id,
    target_id,
    agg_cost ASC;

DROP TABLE pg_temp.tmp_allverts;

INSERT INTO tmp_lostress
SELECT
    source.id AS source_id,
    target.id AS target_id,
    0
FROM
    tmp_allblocks source,
    tmp_allblocks target,
    {tiles_table} tile
WHERE
    tile.{tile_id_col}={tile_id}
    AND ST_Intersects(source.geom,tile.{tile_geom_col})
    AND source.id = target.id;

CREATE INDEX tidx_lostress ON tmp_lostress (source_id,target_id);
ANALYZE tmp_lostress;


-- saving connections in database;
INSERT INTO {connectivity_table} ({conn_source_col},{conn_target_col},high_stress,low_stress)
SELECT
    COALESCE(hs.source_id,ls.source_id),
    COALESCE(hs.target_id,ls.target_id),
    (hs.source_id IS NOT NULL)::BOOLEAN,
    (ls.source_id IS NOT NULL AND ls.agg_cost <= (hs.agg_cost::FLOAT * {max_detour}))::BOOLEAN
FROM
    tmp_histress hs FULL OUTER JOIN tmp_lostress ls
        ON hs.source_id = ls.source_id
        AND hs.target_id = ls.target_id;


DROP TABLE pg_temp.tmp_allblocks;
DROP TABLE pg_temp.tmp_histress;
DROP TABLE pg_temp.tmp_lostress;
