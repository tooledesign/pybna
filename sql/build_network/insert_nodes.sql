INSERT INTO {schema}.{nodes} (road_id, geom)
SELECT  ways.{road_id},
        ST_LineInterpolatePoint(ways.{roads_geom},0.5)
FROM    {schema}.{roads} ways;

-- index
CREATE INDEX {node_index} ON {schema}.{nodes} USING gist (geom);
CREATE INDEX tidx_build_net_nodes_roadid ON {schema}.{nodes} (road_id);
ANALYZE {schema}.{nodes};
