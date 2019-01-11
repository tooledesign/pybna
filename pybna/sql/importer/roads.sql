-- add any missing columns to raw OSM
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "access" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "bridge" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "bicycle" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "button_operated" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "crossing" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "cycleway" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "cycleway:left" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "cycleway:right" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "cycleway:both" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "cycleway:buffer" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "cycleway:left:buffer" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "cycleway:right:buffer" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "cycleway:both:buffer" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "cycleway:width" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "cycleway:left:width" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "cycleway:right:width" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "cycleway:both:width" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "flashing_lights" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "foot" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "footway" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "highway" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "junction" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "landuse" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "lanes" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "lanes:forward" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "lanes:backward" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "lanes:both_ways" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "leisure" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "maxspeed" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "motorcar" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "name" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "oneway" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "oneway:bicycle" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "operator" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "parking" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "parking:lane" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "parking:lane:right" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "parking:lane:left" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "parking:lane:both" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "parking:lane:width" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "parking:lane:right:width" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "parking:lane:left:width" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "parking:lane:both:width" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "public_transport" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "railway" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "segregated" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "service" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "shop" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "stop" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "surface" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "tracktype" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "traffic_sign" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "traffic_signals:direction" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "tunnel" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "turn:lanes" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "turn:lanes:both_ways" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "turn:lanes:backward" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "turn:lanes:forward" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "width" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "width:lanes" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "width:lanes:forward" TEXT;
ALTER TABLE {ways_schema}.{ways_table} ADD COLUMN IF NOT EXISTS "width:lanes:backward" TEXT;

-- create
CREATE TABLE {roads_schema}.{roads_table} (
    {roads_id_col} INTEGER PRIMARY KEY,
    {roads_geom_col} geometry(linestring,{srid}),
    functional_class TEXT,
    path_id INTEGER,
    speed_limit INTEGER,
    {roads_oneway_col} TEXT,
    width_ft INTEGER,
    ft_bike_infra TEXT,
    ft_bike_infra_width FLOAT,
    tf_bike_infra TEXT,
    tf_bike_infra_width FLOAT,
    ft_lanes INTEGER,
    tf_lanes INTEGER,
    ft_cross_lanes INTEGER,
    tf_cross_lanes INTEGER,
    twltl_cross_lanes INTEGER,
    ft_park INTEGER,
    tf_park INTEGER,
    {roads_stress_seg_fwd} INTEGER,
    {roads_stress_cross_fwd} INTEGER,
    {roads_stress_seg_bwd} INTEGER,
    {roads_stress_cross_bwd} INTEGER,
    xwalk INTEGER
);
