--
-- adds geometries to the block scoring output table
--
ALTER TABLE {scores_schema}.{scores_table}
ADD COLUMN {blocks_geom_col} {type};

UPDATE {scores_schema}.{scores_table} AS t
SET {blocks_geom_col} = b.{blocks_geom_col}
FROM {blocks_schema}.{blocks_table} AS b
WHERE t.{blocks_id_col} = b.{blocks_id_col};

CREATE INDEX {sidx_name} ON {scores_schema}.{scores_table} USING GIST({blocks_geom_col});
