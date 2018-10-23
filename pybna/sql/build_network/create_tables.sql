DROP TABLE IF EXISTS {roads_schema}.{nodes};
DROP TABLE IF EXISTS {roads_schema}.{edges};

CREATE TABLE {roads_schema}.{nodes} (
    {node_id} SERIAL PRIMARY KEY,
    road_id INTEGER,
    vert_cost INTEGER,
    geom geometry(point,{srid})
);

CREATE TABLE {roads_schema}.{edges} (
    {edge_id} SERIAL PRIMARY KEY,
    {int_id} INTEGER,
    turn_angle INTEGER,
    int_crossing BOOLEAN,
    int_stress INTEGER,
    source_vert INTEGER,
    source_road_id INTEGER,
    source_road_dir VARCHAR(2),
    source_road_azi INTEGER,
    source_road_length INTEGER,
    source_stress INTEGER,
    target_vert INTEGER,
    target_road_id INTEGER,
    target_road_dir VARCHAR(2),
    target_road_azi INTEGER,
    target_road_length INTEGER,
    target_stress INTEGER,
    link_cost INTEGER,
    link_stress INTEGER,
    geom geometry(linestring,{srid})
);
