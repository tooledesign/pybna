----------------------------------------
-- INPUTS
-- location: neighborhood
-- {srid} psql var must be set before running this script,
--      e.g. psql -v nb_output_srid=2249 -f build_network.sql
----------------------------------------
DROP TABLE IF EXISTS {schema}.{nodes};
DROP TABLE IF EXISTS {schema}.{edges};

-- create new tables
CREATE TABLE {schema}.{nodes} (
    {node_id} SERIAL PRIMARY KEY,
    {road_id} INTEGER,
    vert_cost INTEGER,
    geom geometry(point,{srid})
);

CREATE TABLE {schema}.{edges} (
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

-- create vertices
INSERT INTO {schema}.{nodes} ({road_id}, geom)
SELECT  ways.{road_id},
        ST_LineInterpolatePoint(ways.{roads_geom},0.5)
FROM    {schema}.{roads} ways;

-- index
CREATE INDEX sidx_nodes_geom ON {schema}.{nodes} USING gist (geom);
CREATE INDEX idx_nodes_roadid ON {schema}.{nodes} ({road_id});
ANALYZE {schema}.{nodes};

---------------
-- add links --
---------------
-- two-way to two-way
INSERT INTO {schema}.{edges} ({int_id}, source_vert, target_vert, geom)
SELECT  ints.{int_id},
        vert1.{node_id},
        vert2.{node_id},
        ST_Makeline(vert1.geom,vert2.geom)
FROM    {schema}.{intersections} ints,
        {schema}.{nodes} vert1,
        {schema}.{roads} roads1,
        {schema}.{nodes} vert2,
        {schema}.{roads} roads2
WHERE   vert1.{road_id} = roads1.{road_id}
AND     vert2.{road_id} = roads2.{road_id}
AND     ints.{int_id} IN (roads1.{road_source}, roads1.{road_target})
AND     ints.{int_id} IN (roads2.{road_source}, roads2.{road_target})
AND     roads1.{one_way} IS NULL
AND     roads2.{one_way} IS NULL
AND     roads1.{road_id} != roads2.{road_id};

-- two-way to from-to
INSERT INTO {schema}.{edges} ({int_id}, source_vert, target_vert, geom)
SELECT  ints.{int_id},
        vert1.{node_id},
        vert2.{node_id},
        ST_Makeline(vert1.geom,vert2.geom)
FROM    {schema}.{intersections} ints,
        {schema}.{nodes} vert1,
        {schema}.{roads} roads1,
        {schema}.{nodes} vert2,
        {schema}.{roads} roads2
WHERE   vert1.{road_id} = roads1.{road_id}
AND     vert2.{road_id} = roads2.{road_id}
AND     ints.{int_id} IN (roads1.{road_source}, roads1.{road_target})
AND     ints.{int_id} = roads2.{road_source}
AND     roads1.{one_way} IS NULL
AND     roads2.{one_way} = {forward}
AND     roads1.{road_id} != roads2.{road_id};

-- two-way to to-from
INSERT INTO {schema}.{edges} ({int_id}, source_vert, target_vert, geom)
SELECT  ints.{int_id},
        vert1.{node_id},
        vert2.{node_id},
        ST_Makeline(vert1.geom,vert2.geom)
FROM    {schema}.{intersections} ints,
        {schema}.{nodes} vert1,
        {schema}.{roads} roads1,
        {schema}.{nodes} vert2,
        {schema}.{roads} roads2
WHERE   vert1.{road_id} = roads1.{road_id}
AND     vert2.{road_id} = roads2.{road_id}
AND     ints.{int_id} IN (roads1.{road_source}, roads1.{road_target})
AND     ints.{int_id} = roads2.{road_target}
AND     roads1.{one_way} IS NULL
AND     roads2.{one_way} = {backward}
AND     roads1.{road_id} != roads2.{road_id};

-- from-to to two-way
INSERT INTO {schema}.{edges} ({int_id}, source_vert, target_vert, geom)
SELECT  ints.{int_id},
        vert1.{node_id},
        vert2.{node_id},
        ST_Makeline(vert1.geom,vert2.geom)
FROM    {schema}.{intersections} ints,
        {schema}.{nodes} vert1,
        {schema}.{roads} roads1,
        {schema}.{nodes} vert2,
        {schema}.{roads} roads2
WHERE   vert1.{road_id} = roads1.{road_id}
AND     vert2.{road_id} = roads2.{road_id}
AND     ints.{int_id} = roads1.{road_target}
AND     ints.{int_id} IN (roads2.{road_source}, roads2.{road_target})
AND     roads1.{one_way} = {forward}
AND     roads2.{one_way} IS NULL
AND     roads1.{road_id} != roads2.{road_id};

-- from-to to from-to
INSERT INTO {schema}.{edges} ({int_id}, source_vert, target_vert, geom)
SELECT  ints.{int_id},
        vert1.{node_id},
        vert2.{node_id},
        ST_Makeline(vert1.geom,vert2.geom)
FROM    {schema}.{intersections} ints,
        {schema}.{nodes} vert1,
        {schema}.{roads} roads1,
        {schema}.{nodes} vert2,
        {schema}.{roads} roads2
WHERE   vert1.{road_id} = roads1.{road_id}
AND     vert2.{road_id} = roads2.{road_id}
AND     ints.{int_id} = roads1.{road_target}
AND     ints.{int_id} = roads2.{road_source}
AND     roads1.{one_way} = {forward}
AND     roads2.{one_way} = {forward}
AND     roads1.{road_id} != roads2.{road_id};

-- from-to to to-from
INSERT INTO {schema}.{edges} ({int_id}, source_vert, target_vert, geom)
SELECT  ints.{int_id},
        vert1.{node_id},
        vert2.{node_id},
        ST_Makeline(vert1.geom,vert2.geom)
FROM    {schema}.{intersections} ints,
        {schema}.{nodes} vert1,
        {schema}.{roads} roads1,
        {schema}.{nodes} vert2,
        {schema}.{roads} roads2
WHERE   vert1.{road_id} = roads1.{road_id}
AND     vert2.{road_id} = roads2.{road_id}
AND     ints.{int_id} = roads1.{road_target}
AND     ints.{int_id} = roads2.{road_target}
AND     roads1.{one_way} = {forward}
AND     roads2.{one_way} = {backward}
AND     roads1.{road_id} != roads2.{road_id};

-- to-from to two-way
INSERT INTO {schema}.{edges} ({int_id}, source_vert, target_vert, geom)
SELECT  ints.{int_id},
        vert1.{node_id},
        vert2.{node_id},
        ST_Makeline(vert1.geom,vert2.geom)
FROM    {schema}.{intersections} ints,
        {schema}.{nodes} vert1,
        {schema}.{roads} roads1,
        {schema}.{nodes} vert2,
        {schema}.{roads} roads2
WHERE   vert1.{road_id} = roads1.{road_id}
AND     vert2.{road_id} = roads2.{road_id}
AND     ints.{int_id} = roads1.{road_source}
AND     ints.{int_id} IN (roads2.{road_source}, roads2.{road_target})
AND     roads1.{one_way} = {backward}
AND     roads2.{one_way} IS NULL
AND     roads1.{road_id} != roads2.{road_id};

-- to-from to to-from
INSERT INTO {schema}.{edges} ({int_id}, source_vert, target_vert, geom)
SELECT  ints.{int_id},
        vert1.{node_id},
        vert2.{node_id},
        ST_Makeline(vert1.geom,vert2.geom)
FROM    {schema}.{intersections} ints,
        {schema}.{nodes} vert1,
        {schema}.{roads} roads1,
        {schema}.{nodes} vert2,
        {schema}.{roads} roads2
WHERE   vert1.{road_id} = roads1.{road_id}
AND     vert2.{road_id} = roads2.{road_id}
AND     ints.{int_id} = roads1.{road_source}
AND     ints.{int_id} = roads2.{road_target}
AND     roads1.{one_way} = {backward}
AND     roads2.{one_way} = {backward}
AND     roads1.{road_id} != roads2.{road_id};

-- to-from to from-to
INSERT INTO {schema}.{edges} ({int_id}, source_vert, target_vert, geom)
SELECT  ints.{int_id},
        vert1.{node_id},
        vert2.{node_id},
        ST_Makeline(vert1.geom,vert2.geom)
FROM    {schema}.{intersections} ints,
        {schema}.{nodes} vert1,
        {schema}.{roads} roads1,
        {schema}.{nodes} vert2,
        {schema}.{roads} roads2
WHERE   vert1.{road_id} = roads1.{road_id}
AND     vert2.{road_id} = roads2.{road_id}
AND     ints.{int_id} = roads1.{road_source}
AND     ints.{int_id} = roads2.{road_source}
AND     roads1.{one_way} = {backward}
AND     roads2.{one_way} = {forward}
AND     roads1.{road_id} != roads2.{road_id};

-- index
CREATE INDEX idx_nodes_road_id ON {schema}.{nodes} ({road_id});
CREATE INDEX idx_edges_int_id ON {schema}.{edges} ({int_id});
CREATE INDEX idx_edges_src_trgt ON {schema}.{edges} (source_vert,target_vert);
CREATE INDEX idx_edges_src_rdid ON {schema}.{edges} (source_road_id);
CREATE INDEX idx_edges_tgt_rdid ON {schema}.{edges} (target_road_id);
ANALYZE {schema}.{edges};

--set source and target roads
UPDATE  {schema}.{edges}
SET     source_road_id = s_vert.{road_id},
        target_road_id = t_vert.{road_id}
FROM    {schema}.{nodes} s_vert,
        {schema}.{nodes} t_vert
WHERE   {schema}.{edges}.source_vert = s_vert.{node_id}
AND     {schema}.{edges}.target_vert = t_vert.{node_id};

--source_road_dir
UPDATE  {schema}.{edges}
SET     source_road_dir = CASE  WHEN {schema}.{edges}.{int_id} = road.{road_target}
                                    THEN {forward}
                                ELSE {backward}
                                END
FROM    {schema}.{roads} road
WHERE   {schema}.{edges}.source_road_id = road.{road_id};

--target_road_dir
UPDATE  {schema}.{edges}
SET     target_road_dir = CASE  WHEN {schema}.{edges}.{int_id} = road.{road_target}
                                    THEN {forward}
                                ELSE {backward}
                                END
FROM    {schema}.{roads} road
WHERE   {schema}.{edges}.target_road_id = road.{road_id};

--set azimuths and turn angles
UPDATE  {schema}.{edges}
SET     source_road_azi = CASE  WHEN source_road_dir = {backward}
                                THEN degrees(ST_Azimuth(ST_LineInterpolatePoint(roads1.{roads_geom},0.5),ST_StartPoint(roads1.{roads_geom})))
                                ELSE degrees(ST_Azimuth(ST_LineInterpolatePoint(roads1.{roads_geom},0.5),ST_EndPoint(roads1.{roads_geom})))
                                END,
        target_road_azi = CASE  WHEN target_road_dir = {backward}
                                THEN degrees(ST_Azimuth(ST_StartPoint(roads2.{roads_geom}),ST_LineInterpolatePoint(roads2.{roads_geom},0.5)))
                                ELSE degrees(ST_Azimuth(ST_EndPoint(roads2.{roads_geom}),ST_LineInterpolatePoint(roads2.{roads_geom},0.5)))
                                END
FROM    {schema}.{roads} roads1,
        {schema}.{roads} roads2
WHERE   source_road_id = roads1.{road_id}
AND     target_road_id = roads2.{road_id};

UPDATE {schema}.{edges}
SET     turn_angle = (target_road_azi - source_road_azi + 360) % 360;

-------------------
-- set turn info --
-------------------
-- assume crossing is true unless proven otherwise
UPDATE {schema}.{edges} SET int_crossing = TRUE;

-- set right turns
UPDATE  {schema}.{edges}
SET     int_crossing = FALSE
WHERE   {edge_id} = (
            SELECT      r.{edge_id}
            FROM        {schema}.{edges} r
            WHERE       {schema}.{edges}.source_road_id = r.source_road_id
            AND         {schema}.{edges}.{int_id} = r.{int_id}
            ORDER BY    (sin(radians(r.turn_angle))>0)::INT DESC,
                        CASE    WHEN sin(radians(r.turn_angle))>0
                                THEN cos(radians(r.turn_angle))
                                ELSE -cos(radians(r.turn_angle))
                                END ASC
            LIMIT       1
);

--set lengths
UPDATE  {schema}.{edges}
SET     source_road_length = ST_Length(roads1.{roads_geom}),
        target_road_length = ST_Length(roads2.{roads_geom})
FROM    {schema}.{roads} roads1,
        {schema}.{roads} roads2
WHERE   source_road_id = roads1.{road_id}
AND     target_road_id = roads2.{road_id};

---------------------
-- set link stress --
---------------------
--source_stress
UPDATE  {schema}.{edges}
SET     source_stress = CASE WHEN {schema}.{edges}.{int_id} = road.{road_target} THEN road.{ft_seg_stress}
                        ELSE road.{tf_seg_stress}
                        END
FROM    {schema}.{roads} road
WHERE   {schema}.{edges}.source_road_id = road.{road_id};

--int_stress
UPDATE  {schema}.{edges}
SET     int_stress = roads.{ft_int_stress}
FROM    {schema}.{roads} roads
WHERE   {schema}.{edges}.source_road_id = roads.{road_id}
AND     source_road_dir = {forward};

UPDATE  {schema}.{edges}
SET     int_stress = roads.{tf_int_stress}
FROM    {schema}.{roads} roads
WHERE   {schema}.{edges}.source_road_id = roads.{road_id}
AND     source_road_dir = {backward};

UPDATE  {schema}.{edges}
SET     int_stress = 1
WHERE   NOT int_crossing;;

--target_stress
UPDATE  {schema}.{edges}
SET     target_stress = CASE    WHEN {schema}.{edges}.{int_id} = road.{road_target}
                                    THEN road.{tf_seg_stress}
                                ELSE road.{ft_seg_stress}
                                END
FROM    {schema}.{roads} road
WHERE   {schema}.{edges}.target_road_id = road.{road_id};

--link_stress
UPDATE  {schema}.{edges}
SET     link_stress = GREATEST(source_stress,int_stress,target_stress);

--------------
-- set cost --
--------------
UPDATE  {schema}.{edges}
SET     link_cost = ROUND((source_road_length + target_road_length) / 2);
