-- guarantees that we don't have duplicate nodes
-- (duplicates can cause problems in pg_routing)
DROP TABLE IF EXISTS tmp_unique;
SELECT DISTINCT
    {zones_id_col},
    unnest(node_ids) AS node_id
INTO TEMP TABLE tmp_unique
FROM {zones_schema}.{zones_table}
WHERE node_ids IS NOT NULL
;

DROP TABLE IF EXISTS tmp_reagg;
SELECT
    {zones_id_col},
    array_agg(node_id) AS node_ids
INTO TEMP TABLE tmp_reagg
FROM tmp_unique
GROUP BY {zones_id_col}
;

UPDATE {zones_schema}.{zones_table} zones
SET node_ids = tmp_reagg.node_ids
FROM tmp_reagg
WHERE tmp_reagg.{zones_id_col} = zones.{zones_id_col}
;
