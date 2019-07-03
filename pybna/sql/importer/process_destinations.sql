CREATE TABLE {schema}.{final_table} AS (
    SELECT *, 'ways' AS osm_type FROM {schema}.{ways_table}
    UNION
    SELECT *, 'nodes' FROM {schema}.{nodes_table}
);

ALTER TABLE {schema}.{final_table} ADD COLUMN {pkey} SERIAL PRIMARY KEY;
ALTER TABLE {schema}.{final_table} ADD COLUMN geom_pt geometry(point,{srid});
ALTER TABLE {schema}.{final_table} ADD COLUMN geom_poly geometry(multipolygon,{srid});

UPDATE {schema}.{final_table}
SET geom_poly = ST_Transform(
        ST_SetSRID(
            ST_Multi(ST_MakePolygon(ST_GeomFromEWKT(geom))),
            4326
        ),
        {srid}
    )
WHERE osm_type = 'ways'
;

UPDATE {schema}.{final_table}
SET geom_pt = ST_Transform(
        ST_SetSRID(
            ST_GeomFromEWKT(geom),
            4326
        ),
        {srid}
    )
WHERE osm_type = 'nodes'
;

UPDATE {schema}.{final_table}
SET geom_pt = ST_Centroid(geom_poly)
WHERE osm_type = 'ways'
;

ALTER TABLE {schema}.{final_table} DROP COLUMN osm_type;
ALTER TABLE {schema}.{final_table} DROP COLUMN geom;

CREATE INDEX {sidx} ON {schema}.{final_table} USING GIST (geom_pt);
ANALYZE {schema}.{final_table};
