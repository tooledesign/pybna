DROP TABLE IF EXISTS tmp_islands;
CREATE TEMP TABLE tmp_islands AS (
    SELECT DISTINCT
        i.{ints_id_col} AS id,
        TRUE AS island
    FROM
        {ints_schema}.{ints_table} i,
        {osm_nodes_schema}.{osm_nodes_table} osm
    WHERE
        ST_DWithin(i.{ints_geom_col},osm.geom,{ints_cluster_distance})
        AND osm.highway = 'crossing'
        AND osm.crossing = 'island'
);
