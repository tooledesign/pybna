INSERT INTO {roads_schema}.{nodes} (road_id, geom)
SELECT  ways.{roads_id_col},
        ST_LineInterpolatePoint(ways.{roads_geom_col},0.5)
FROM    {roads_schema}.{roads_table} ways;

-- index
CREATE INDEX {node_index} ON {roads_schema}.{nodes} USING gist (geom);
CREATE INDEX tidx_build_net_nodes_roadid ON {roads_schema}.{nodes} (road_id);
ANALYZE {roads_schema}.{nodes};
