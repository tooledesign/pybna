-- set up table structure
DROP TABLE IF EXISTS {zones_schema}.{zones_table};
SELECT
    array_agg(blocks.{blocks_id_col}) AS block_ids,
    ST_Union(blocks.{blocks_geom_col}) AS {zones_geom_col},
    array_agg(nodes.{nodes_id_col}) AS node_ids
INTO {zones_schema}.{zones_table}
FROM
    tmp_prelim_zones z,
    {blocks_schema}.{blocks_table} blocks,
    {nodes_schema}.{nodes_table} nodes
WHERE FALSE
;

DELETE FROM {zones_schema}.{zones_table};

-- aggregate blocks within zones
INSERT INTO {zones_schema}.{zones_table}
SELECT
    array_agg(blocks.{blocks_id_col}) AS block_ids,
    ST_Union(blocks.{blocks_geom_col}) AS {zones_geom_col},
    NULL
FROM
    tmp_prelim_zones z,
    {blocks_schema}.{blocks_table} blocks
WHERE
    ST_Intersects(blocks.{blocks_geom_col},z.geom)
    AND ST_Area(ST_Intersection(z.geom,blocks.{blocks_geom_col})) > (ST_Area(blocks.{blocks_geom_col}) * 0.9)
GROUP BY z.id
;

INSERT INTO {zones_schema}.{zones_table}
SELECT
    ARRAY[{blocks_id_col}],
    ST_Multi({blocks_geom_col}),
    NULL
FROM {blocks_schema}.{blocks_table} blocks
WHERE NOT EXISTS (
    SELECT 1
    FROM {zones_schema}.{zones_table} z
    WHERE blocks.{blocks_id_col} = ANY(z.block_ids)
);

CREATE INDEX {zones_index} ON {zones_schema}.{zones_table} USING GIST ({zones_geom_col});
ALTER TABLE {zones_schema}.{zones_table} ADD COLUMN {zones_id_col} SERIAL PRIMARY KEY;
