-- filter roads
DROP TABLE IF EXISTS tmp_lines_filtered;
SELECT {in_geom} AS geom
INTO TEMP TABLE tmp_lines_filtered
FROM {in_schema}.{in_table}
WHERE {lines_filter}
;

-- polygonize to create preliminary zones
DROP TABLE IF EXISTS tmp_prelim_zones;
SELECT ST_MakeValid((ST_Dump(ST_Polygonize(geom))).geom) AS geom
INTO TEMP TABLE tmp_prelim_zones
FROM tmp_lines_filtered
;
ALTER TABLE tmp_prelim_zones ADD COLUMN id SERIAL PRIMARY KEY;
