-- dump all candidate intersection points
DROP TABLE IF EXISTS tmp_allpoints;
CREATE TEMP TABLE tmp_allpoints AS (
    SELECT DISTINCT ST_StartPoint(geom) AS geom FROM {roads_schema}.{roads_table}
    UNION
    SELECT DISTINCT ST_EndPoint(geom) FROM {roads_schema}.{roads_table}
);

-- collapse clusters
INSERT INTO {ints_schema}.{ints_table} ({ints_geom_col})
SELECT  ST_Centroid(ST_CollectionExtract(unnest(
            ST_ClusterWithin(geom,{ints_cluster_distance})
        ),1))
FROM tmp_allpoints
;

DROP TABLE tmp_allpoints;

-- indexes
CREATE INDEX {ints_geom_idx} ON {ints_schema}.{ints_table} USING GIST ({ints_geom_col});
ANALYZE {ints_schema}.{ints_table};

-- assign to/from
DROP TABLE IF EXISTS tmp_tos;
CREATE TEMP TABLE tmp_tos AS (
    SELECT DISTINCT ON (r.{roads_id_col})
        r.{roads_id_col} AS rid,
        i.{ints_id_col} AS iid
    FROM
        {roads_schema}.{roads_table} r,
        {ints_schema}.{ints_table} i
    WHERE ST_DWithin(r.{roads_geom_col},i.{ints_geom_col},{ints_cluster_distance})
    ORDER BY
        r.{roads_id_col},
        ST_Distance(ST_EndPoint(r.{roads_geom_col}),i.{ints_geom_col})
);

UPDATE {roads_schema}.{roads_table}
SET {roads_target_col} = tmp_tos.iid
FROM tmp_tos
WHERE {roads_table}.{roads_id_col} = tmp_tos.rid
;

DROP TABLE tmp_tos;

DROP TABLE IF EXISTS tmp_froms;
CREATE TEMP TABLE tmp_froms AS (
    SELECT DISTINCT ON (r.{roads_id_col})
        r.{roads_id_col} AS rid,
        i.{ints_id_col} AS iid
    FROM
        {roads_schema}.{roads_table} r,
        {ints_schema}.{ints_table} i
    WHERE ST_DWithin(r.{roads_geom_col},i.{ints_geom_col},{ints_cluster_distance})
    ORDER BY
        r.{roads_id_col},
        ST_Distance(ST_StartPoint(r.{roads_geom_col}),i.{ints_geom_col})
);

UPDATE {roads_schema}.{roads_table}
SET {roads_source_col} = tmp_froms.iid
FROM tmp_froms
WHERE {roads_table}.{roads_id_col} = tmp_froms.rid
;

DROP TABLE tmp_froms;
