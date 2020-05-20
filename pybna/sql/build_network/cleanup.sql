DROP TABLE IF EXISTS pg_temp.e;
DROP TABLE IF EXISTS pg_temp.t;

DROP INDEX IF EXISTS {edges_schema}.tidx_net_build_int_id;
DROP INDEX IF EXISTS {edges_schema}.tidx_net_build_source_road_id;
DROP INDEX IF EXISTS {edges_schema}.tidx_net_build_target_road_id;
DROP INDEX IF EXISTS {nodes_schema}.tidx_build_net_nodes_roadid;
