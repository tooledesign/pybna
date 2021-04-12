-- add any missing columns to raw OSM
ALTER TABLE {osm_nodes_schema}.{osm_nodes_table} ADD COLUMN IF NOT EXISTS "access" TEXT;
ALTER TABLE {osm_nodes_schema}.{osm_nodes_table} ADD COLUMN IF NOT EXISTS "amenity" TEXT;
ALTER TABLE {osm_nodes_schema}.{osm_nodes_table} ADD COLUMN IF NOT EXISTS "bicycle" TEXT;
ALTER TABLE {osm_nodes_schema}.{osm_nodes_table} ADD COLUMN IF NOT EXISTS "bridge" TEXT;
ALTER TABLE {osm_nodes_schema}.{osm_nodes_table} ADD COLUMN IF NOT EXISTS "button_operated" TEXT;
ALTER TABLE {osm_nodes_schema}.{osm_nodes_table} ADD COLUMN IF NOT EXISTS "crossing" TEXT;
ALTER TABLE {osm_nodes_schema}.{osm_nodes_table} ADD COLUMN IF NOT EXISTS "flashing_lights" TEXT;
ALTER TABLE {osm_nodes_schema}.{osm_nodes_table} ADD COLUMN IF NOT EXISTS "foot" TEXT;
ALTER TABLE {osm_nodes_schema}.{osm_nodes_table} ADD COLUMN IF NOT EXISTS "highway" TEXT;
ALTER TABLE {osm_nodes_schema}.{osm_nodes_table} ADD COLUMN IF NOT EXISTS "junction" TEXT;
ALTER TABLE {osm_nodes_schema}.{osm_nodes_table} ADD COLUMN IF NOT EXISTS "leisure" TEXT;
ALTER TABLE {osm_nodes_schema}.{osm_nodes_table} ADD COLUMN IF NOT EXISTS "motorcar" TEXT;
ALTER TABLE {osm_nodes_schema}.{osm_nodes_table} ADD COLUMN IF NOT EXISTS "name" TEXT;
ALTER TABLE {osm_nodes_schema}.{osm_nodes_table} ADD COLUMN IF NOT EXISTS "oneway" TEXT;
ALTER TABLE {osm_nodes_schema}.{osm_nodes_table} ADD COLUMN IF NOT EXISTS "oneway:bicycle" TEXT;
ALTER TABLE {osm_nodes_schema}.{osm_nodes_table} ADD COLUMN IF NOT EXISTS "operator" TEXT;
ALTER TABLE {osm_nodes_schema}.{osm_nodes_table} ADD COLUMN IF NOT EXISTS "public_transport" TEXT;
ALTER TABLE {osm_nodes_schema}.{osm_nodes_table} ADD COLUMN IF NOT EXISTS "railway" TEXT;
ALTER TABLE {osm_nodes_schema}.{osm_nodes_table} ADD COLUMN IF NOT EXISTS "segregated" TEXT;
ALTER TABLE {osm_nodes_schema}.{osm_nodes_table} ADD COLUMN IF NOT EXISTS "shop" TEXT;
ALTER TABLE {osm_nodes_schema}.{osm_nodes_table} ADD COLUMN IF NOT EXISTS "stop" TEXT;
ALTER TABLE {osm_nodes_schema}.{osm_nodes_table} ADD COLUMN IF NOT EXISTS "surface" TEXT;
ALTER TABLE {osm_nodes_schema}.{osm_nodes_table} ADD COLUMN IF NOT EXISTS "traffic_sign" TEXT;
ALTER TABLE {osm_nodes_schema}.{osm_nodes_table} ADD COLUMN IF NOT EXISTS "traffic_signals" TEXT;
ALTER TABLE {osm_nodes_schema}.{osm_nodes_table} ADD COLUMN IF NOT EXISTS "tunnel" TEXT;
ALTER TABLE {osm_nodes_schema}.{osm_nodes_table} ADD COLUMN IF NOT EXISTS "width" TEXT;
