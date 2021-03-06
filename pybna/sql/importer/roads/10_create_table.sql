CREATE TABLE {roads_schema}.{roads_table} (
    {roads_id_col} INTEGER PRIMARY KEY,
    {roads_geom_col} geometry(linestring,{srid}),
    osmid BIGINT[],
    functional_class TEXT,
    path_id INTEGER,
    {roads_oneway_col} TEXT,
    {roads_source_col} INTEGER,
    {roads_target_col} INTEGER,
    width FLOAT,
    speed_limit INTEGER,
    ft_bike_infra TEXT,
    ft_bike_infra_width FLOAT,
    tf_bike_infra TEXT,
    tf_bike_infra_width FLOAT,
    ft_lanes INTEGER,
    tf_lanes INTEGER,
    ft_cross_lanes INTEGER,
    tf_cross_lanes INTEGER,
    twltl_cross_lanes INTEGER,
    ft_park BOOLEAN,
    tf_park BOOLEAN,
    {roads_stress_seg_fwd} INTEGER,
    {roads_stress_cross_fwd} INTEGER,
    {roads_stress_seg_bwd} INTEGER,
    {roads_stress_cross_bwd} INTEGER,
    xwalk INTEGER
);
