INSERT INTO {nodes_schema}.{nodes_table} (road_id, {nodes_geom_col})
SELECT  ways.{roads_id_col},
        ST_LineInterpolatePoint(ways.{roads_geom_col},0.5)
FROM    {roads_schema}.{roads_table} ways;

-- index
CREATE INDEX {nodes_index} ON {nodes_schema}.{nodes_table} USING gist ({nodes_geom_col});
CREATE INDEX tidx_build_net_nodes_roadid ON {nodes_schema}.{nodes_table} (road_id);
ANALYZE {nodes_schema}.{nodes_table};
