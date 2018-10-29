-- filter roads
DROP TABLE IF EXISTS tmp_roads_filtered;
SELECT {roads_geom_col} AS geom
INTO TEMP TABLE tmp_roads_filtered
FROM {roads_schema}.{roads_table}
WHERE
    {roads_stress_seg_fwd} > {connectivity_max_stress}
    OR {roads_stress_cross_fwd} > {connectivity_max_stress}
    OR {roads_stress_seg_bwd} > {connectivity_max_stress}
    OR {roads_stress_cross_bwd} > {connectivity_max_stress}
    OR ({roads_filter})
;

-- polygonize to create preliminary zones
DROP TABLE IF EXISTS tmp_prelim_zones;
SELECT (ST_Dump(ST_Polygonize(geom))).geom AS geom
INTO TEMP TABLE tmp_prelim_zones
FROM tmp_roads_filtered
;
ALTER TABLE tmp_prelim_zones ADD COLUMN id SERIAL PRIMARY KEY;
