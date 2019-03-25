ALTER TABLE {osm_ways_schema}.{osm_ways_table} ALTER COLUMN osmid TYPE BIGINT[]
USING ('{{' || trim(both '{{' from trim(both '}}' from osmid)) || '}}')::BIGINT[];

-- add any missing columns to raw OSM
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "access" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "bridge" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "bicycle" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "button_operated" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "crossing" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "cycleway" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "cycleway:left" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "cycleway:right" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "cycleway:both" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "cycleway:buffer" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "cycleway:left:buffer" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "cycleway:right:buffer" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "cycleway:both:buffer" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "cycleway:width" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "cycleway:left:width" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "cycleway:right:width" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "cycleway:both:width" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "flashing_lights" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "foot" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "footway" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "highway" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "junction" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "landuse" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "lanes" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "lanes:forward" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "lanes:backward" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "lanes:both_ways" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "leisure" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "maxspeed" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "motorcar" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "name" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "oneway" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "oneway:bicycle" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "operator" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "parking" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "parking:lane" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "parking:lane:right" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "parking:lane:left" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "parking:lane:both" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "parking:lane:width" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "parking:lane:right:width" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "parking:lane:left:width" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "parking:lane:both:width" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "public_transport" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "railway" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "segregated" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "service" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "shop" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "stop" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "surface" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "tracktype" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "traffic_sign" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "traffic_signals:direction" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "tunnel" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "turn:lanes" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "turn:lanes:both_ways" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "turn:lanes:backward" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "turn:lanes:forward" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "width" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "width:lanes" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "width:lanes:forward" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "width:lanes:backward" TEXT;
ALTER TABLE {osm_ways_schema}.{osm_ways_table} ADD COLUMN IF NOT EXISTS "one_way_car" TEXT;

-- set one_way_car
UPDATE {osm_ways_schema}.{osm_ways_table}
SET one_way_car = 'ft'
WHERE trim(oneway) IN ('1','yes','true')
;

UPDATE {osm_ways_schema}.{osm_ways_table}
SET one_way_car = 'tf'
WHERE trim(oneway) = '-1'
;
