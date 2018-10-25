DROP TABLE IF EXISTS tmp_roads_filtered;
SELECT
    {roads_id_col} AS road_id,
    {roads_geom_col} AS geom,
    {roads_source_col} AS source,
    {roads_target_col} AS target,
    GREATEST(COALESCE({roads_stress_seg_fwd},99),COALESCE({roads_stress_cross_fwd},99)) AS target_stress,
    GREATEST(COALESCE({roads_stress_seg_bwd},99),COALESCE({roads_stress_cross_bwd},99)) AS source_stress
INTO TEMP TABLE tmp_roads_filtered
FROM {roads_schema}.{roads_table}
WHERE {roads_filter}
;

DROP TABLE IF EXISTS tmp_ints_filtered;
SELECT {ints_id_col} AS int_id
INTO TEMP TABLE tmp_ints_filtered
FROM {ints_schema}.{ints_table}
WHERE {ints_filter}
;

CREATE INDEX tidx_tmp_roads_filtered_src ON tmp_roads_filtered (source);
CREATE INDEX tidx_tmp_roads_filtered_tgt ON tmp_roads_filtered (target);
ANALYZE tmp_roads_filtered;
CREATE INDEX tidx_tmp_ints_filtered ON tmp_ints_filtered (int_id);
ANALYZE tmp_ints_filtered;

DROP TABLE IF EXISTS tmp_ints_all_stress;
SELECT
    i.int_id,
    r.source_stress AS stress
INTO TEMP TABLE tmp_ints_all_stress
FROM
    tmp_ints_filtered i,
    tmp_roads_filtered r
WHERE i.{ints_id_col} = r.source
UNION
SELECT
    i.int_id,
    r.target_stress
FROM
    tmp_ints_filtered i,
    tmp_roads_filtered r
WHERE i.{ints_id_col} = r.target
;

DROP TABLE IF EXISTS tmp_ints_low_stress;
SELECT int_id
INTO TEMP TABLE tmp_ints_low_stress
FROM tmp_ints_all_stress
GROUP BY int_id
HAVING MAX(stress) <= {connectivity_max_stress}
;

CREATE INDEX tidx_ils ON tmp_ints_low_stress(int_id);
ANALYZE tmp_ints_low_stress;

DROP TABLE IF EXISTS tmp_roads_filtered;
DROP TABLE IF EXISTS tmp_ints_filtered;
