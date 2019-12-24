SELECT
    blocks.{blocks_id_col}
    {columns}
INTO {scores_schema}.{scores_table}
FROM
    {blocks_schema}.{blocks_table} blocks
    {tables}
WHERE EXISTS (
    SELECT 1
    FROM {boundary_schema}.{boundary_table} bound
    WHERE st_intersects(blocks.{blocks_geom_col},bound.{boundary_geom_col})
);
ALTER TABLE {scores_schema}.{scores_table} ADD PRIMARY KEY ({blocks_id_col});
