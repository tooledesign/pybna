DROP TABLE IF EXISTS {nodes_schema}.{nodes_table};
DROP TABLE IF EXISTS {edges_schema}.{edges_table};

CREATE TABLE {nodes_schema}.{nodes_table} (
    {nodes_id_col} SERIAL PRIMARY KEY,
    road_id INTEGER,
    vert_cost INTEGER,
    {nodes_geom_col} geometry(point,{srid})
);

CREATE TABLE {edges_schema}.{edges_table} (
    {edges_id_col} SERIAL PRIMARY KEY,
    {ints_id_col} INTEGER,
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
    {edges_geom_col} geometry(linestring,{srid})
);
