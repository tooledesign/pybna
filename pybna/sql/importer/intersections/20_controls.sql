-- temporary osm index
CREATE INDEX tidx_osmid_roads ON {roads_schema}.{roads_table} (osmid);
ANALYZE {roads_schema}.{roads_table};
CREATE INDEX tidx_osmid_import ON {osm_ways_schema}.{osm_ways_table} (osmid);
ANALYZE {osm_ways_schema}.{osm_ways_table};

-- signals
DROP TABLE IF EXISTS tmp_signals;
CREATE TEMP TABLE tmp_signals AS (
    SELECT DISTINCT i.{ints_id_col} AS id
    FROM
        {ints_schema}.{ints_table} i,
        {osm_nodes_schema}.{osm_nodes_table} osm
    WHERE
        ST_DWithin(i.{ints_geom_col},osm.geom,{ints_cluster_distance})
        AND osm.highway = 'traffic_signals'
    UNION
    SELECT DISTINCT r.{roads_target_col} AS id
    FROM
        {roads_schema}.{roads_table} r,
        {osm_ways_schema}.{osm_ways_table} osm
    WHERE
        r.osmid = osm.osmid
        AND osm."traffic_signals:direction" = 'forward'
    UNION
    SELECT DISTINCT r.{roads_source_col} AS id
    FROM
        {roads_schema}.{roads_table} r,
        {osm_ways_schema}.{osm_ways_table} osm
    WHERE
        r.osmid = osm.osmid
        AND osm."traffic_signals:direction" = 'backward'
);


-- hawks
DROP TABLE IF EXISTS tmp_hawks;
CREATE TEMP TABLE tmp_hawks AS (
    SELECT DISTINCT i.{ints_id_col} AS id
    FROM
        {ints_schema}.{ints_table} i,
        {osm_nodes_schema}.{osm_nodes_table} osm
    WHERE
        ST_DWithin(i.{ints_geom_col},osm.geom,{ints_cluster_distance})
        AND osm.highway = 'crossing'
        AND osm.crossing IN ('traffic_signals','pelican','toucan')
);


-- rrfbs
DROP TABLE IF EXISTS tmp_rrfbs;
CREATE TEMP TABLE tmp_rrfbs AS (
    SELECT DISTINCT i.{ints_id_col} AS id
    FROM
        {ints_schema}.{ints_table} i,
        {osm_nodes_schema}.{osm_nodes_table} osm
    WHERE
        ST_DWithin(i.{ints_geom_col},osm.geom,{ints_cluster_distance})
        AND osm.highway = 'crossing'
        AND osm.flashing_lights = 'yes'
);


-- stops
DROP TABLE IF EXISTS tmp_stops;
CREATE TEMP TABLE tmp_stops AS (
    SELECT DISTINCT i.{ints_id_col} AS id
    FROM
        {ints_schema}.{ints_table} i,
        {osm_nodes_schema}.{osm_nodes_table} osm
    WHERE
        ST_DWithin(i.{ints_geom_col},osm.geom,{ints_cluster_distance})
        AND osm.highway = 'stop'
        AND osm.stop = 'all'
);


DROP INDEX IF EXISTS {roads_schema}.tidx_osmid_roads;
DROP INDEX IF EXISTS {osm_ways_schema}.tidx_osmid_import;


-- combine them all
DROP TABLE IF EXISTS tmp_controls_all;
CREATE TEMP TABLE tmp_controls_all AS (
    SELECT
        id,
        1 AS sort_order,
        'signals' AS control
    FROM tmp_signals
    UNION
    SELECT
        id,
        2,
        'hawk'
    FROM tmp_hawks
    UNION
    SELECT
        id,
        3,
        'stops'
    FROM tmp_stops
    UNION
    SELECT
        id,
        4,
        'rrfb'
    FROM tmp_rrfbs
);

DROP TABLE IF EXISTS tmp_controls;
CREATE TEMP TABLE tmp_controls AS (
    SELECT DISTINCT ON (id)
        id,
        control
    FROM tmp_controls_all
    ORDER BY
        id,
        sort_order ASC
);

DROP TABLE IF EXISTS tmp_signals;
DROP TABLE IF EXISTS tmp_hawks;
DROP TABLE IF EXISTS tmp_stops;
DROP TABLE IF EXISTS tmp_rrfbs;
DROP TABLE IF EXISTS tmp_controls_all;
