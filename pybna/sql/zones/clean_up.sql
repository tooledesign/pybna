ALTER TABLE {zones_schema}.{zones_table} ADD COLUMN {zones_id_col} SERIAL PRIMARY KEY;
CREATE INDEX {zones_index} ON {zones_schema}.{zones_table} USING GIST ({zones_geom_col});
ANALYZE {zones_schema}.{zones_table};
