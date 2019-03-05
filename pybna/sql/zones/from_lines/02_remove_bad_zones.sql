-- filter out potentially bad zones
DROP TABLE IF EXISTS tmp_drop_zones;
SELECT z.id
INTO TEMP TABLE tmp_drop_zones
FROM
    tmp_prelim_zones z,
    {blocks_schema}.{blocks_table} blocks
WHERE
    ST_Intersects(z.geom,blocks.{blocks_geom_col})
    AND ST_Area(ST_Intersection(z.geom,blocks.{blocks_geom_col})) > (ST_Area(z.geom) * 0.03)
    AND ST_Area(ST_Intersection(z.geom,blocks.{blocks_geom_col})) > (ST_Area(blocks.{blocks_geom_col}) * 0.1)
    AND blocks.{blocks_population_col} <= 5
GROUP BY z.id
HAVING SUM(ST_Area(blocks.{blocks_geom_col})) >= (ST_Area(z.geom) * 0.5)
;

DELETE FROM tmp_prelim_zones
USING tmp_drop_zones
WHERE tmp_drop_zones.id = tmp_prelim_zones.id
;
