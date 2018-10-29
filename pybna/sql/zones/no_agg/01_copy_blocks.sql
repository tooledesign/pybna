-- set up table structure
DROP TABLE IF EXISTS {zones_schema}.{zones_table};
SELECT
    array_agg(blocks.{blocks_id_col}) AS block_ids,
    ST_Multi(ST_Union(blocks.{blocks_geom_col}))::geometry(multipolygon,{srid}) AS {zones_geom_col},
    array_agg(nodes.{nodes_id_col}) AS node_ids
INTO {zones_schema}.{zones_table}
FROM
    {blocks_schema}.{blocks_table} blocks,
    {nodes_schema}.{nodes_table} nodes
WHERE FALSE
;

DELETE FROM {zones_schema}.{zones_table};

-- unnest road_ids
DROP TABLE IF EXISTS tmp_blocks_roads;
SELECT
    {blocks_id_col} AS block_id,
    {blocks_geom_col} AS geom,
    unnest(road_ids) AS road_id
INTO TEMP TABLE tmp_blocks_roads
FROM {blocks_schema}.{blocks_table}
;

-- aggregate with nodes
DROP TABLE IF EXISTS tmp_blocks_nodes;
SELECT
    ARRAY[tmp_blocks_roads.block_id],
    ST_Multi(tmp_blocks_roads.geom),
    array_agg(nodes.{nodes_id_col}) AS node_ids
INTO TEMP TABLE tmp_blocks_nodes
FROM
    tmp_blocks_roads,
    {nodes_schema}.{nodes_table} nodes
WHERE tmp_blocks_roads.road_id = nodes.road_id
GROUP BY
    tmp_blocks_roads.block_id,
    tmp_blocks_roads.geom
;

-- add to table
INSERT INTO {zones_schema}.{zones_table}
SELECT *
FROM tmp_blocks_nodes
;

CREATE INDEX {zones_index} ON {zones_schema}.{zones_table} USING GIST ({zones_geom_col});
ALTER TABLE {zones_schema}.{zones_table} ADD COLUMN {zones_id_col} SERIAL PRIMARY KEY;
