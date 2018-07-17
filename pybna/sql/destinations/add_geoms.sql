--
-- adds geometries to the block scoring output table
--
ALTER TABLE {schema}.{table}
ADD COLUMN {block_geom} {type};

UPDATE {schema}.{table} AS t
SET {block_geom} = b.{block_geom}
FROM {blocks_schema}.{blocks_table} AS b
WHERE t.{block_id_col} = b.{block_id_col};

CREATE INDEX {sidx_name} ON {schema}.{table} USING GIST({block_geom});
