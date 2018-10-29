-- set up table structure
DROP TABLE IF EXISTS {zones_schema}.{zones_table};
SELECT
    array_agg(blocks.{blocks_id_col}) AS block_ids,
    ST_Multi(ST_Union(blocks.{blocks_geom_col}))::geometry(multipolygon,{srid}) AS {zones_geom_col},
    array_agg(nodes.{nodes_id_col}) AS node_ids
INTO {zones_schema}.{zones_table}
FROM
    tmp_prelim_zones z,
    {blocks_schema}.{blocks_table} blocks,
    {nodes_schema}.{nodes_table} nodes
WHERE FALSE
;

DELETE FROM {zones_schema}.{zones_table};

-- define all possible zone-block combinations
DROP TABLE IF EXISTS tmp_zones_blocks;
SELECT DISTINCT ON (block_id)
    src_table.{in_uid} AS src_id,
    blocks.{blocks_id_col} AS block_id,
    blocks.{blocks_geom_col} AS geom
FROM
    {in_schema}.{in_table} src_table,
    {blocks_schema}.{blocks_table} blocks
WHERE ST_Intersects(src_table.{in_geom},blocks.{blocks_geom_col})
ORDER BY
    block_id,
    ST_Area(ST_Intersection(src_table.{in_geom},blocks.{blocks_geom_col})) DESC
;

-- aggregate blocks within zones
INSERT INTO {zones_schema}.{zones_table}
SELECT
    array_agg(tmp_zones_blocks.block_id) AS block_ids,
    ST_Union(tmp_zones_blocks.geom) AS {zones_geom_col},
    NULL
FROM tmp_zones_blocks
GROUP BY tmp_zones_blocks.src_id
;

CREATE INDEX {zones_index} ON {zones_schema}.{zones_table} USING GIST ({zones_geom_col});
ALTER TABLE {zones_schema}.{zones_table} ADD COLUMN {zones_id_col} SERIAL PRIMARY KEY;
