ALTER TABLE tmp_zones ADD COLUMN id SERIAL PRIMARY KEY;
CREATE INDEX tsidx_tmp_zones ON tmp_zones USING GIST (geom);
ANALYZE tmp_zones;
