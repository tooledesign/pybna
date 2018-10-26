-- filter roads
DROP TABLE IF EXISTS scratch.tmp_roads_filtered;
SELECT geom
INTO scratch.tmp_roads_filtered
FROM scratch.bna_test_streets
WHERE
    GREATEST(COALESCE(stress,99),COALESCE(stress,99)) > 2
    OR GREATEST(COALESCE(stress,99),COALESCE(stress,99)) > 2
    OR ("f_class" IN ('1','2','3','4','6','7'))
;

-- polygonize to create preliminary zones
DROP TABLE IF EXISTS scratch.tmp_prelim_zones;
SELECT (ST_Dump(ST_Polygonize(geom))).geom
INTO scratch.tmp_prelim_zones
FROM scratch.tmp_roads_filtered
;
ALTER TABLE scratch.tmp_prelim_zones ADD COLUMN id SERIAL PRIMARY KEY;

-- filter out potentially bad zones
DROP TABLE IF EXISTS tmp_drop_zones;
SELECT z.id
INTO TEMP TABLE tmp_drop_zones
FROM
    scratch.tmp_prelim_zones z,
    scratch.bna_test_blocks blocks
WHERE
    ST_Intersects(z.geom,blocks."geom")
    AND ST_Area(ST_Intersection(z.geom,blocks."geom")) > (ST_Area(z.geom) * 0.03)
    AND ST_Area(ST_Intersection(z.geom,blocks."geom")) > (ST_Area(blocks."geom") * 0.1)
    AND blocks."POP10" <= 5
GROUP BY z.id
HAVING SUM(ST_Area(blocks."geom")) >= (ST_Area(z.geom) * 0.5)
;

DELETE FROM scratch.tmp_prelim_zones
USING tmp_drop_zones
WHERE tmp_drop_zones.id = tmp_prelim_zones.id
;

-- aggregate blocks within zones
DROP TABLE IF EXISTS scratch.zones;
SELECT
    array_agg(blocks."BLOCKID10") AS block_ids,
    ST_Union(blocks."geom") AS geom
INTO scratch.zones
FROM
    scratch.tmp_prelim_zones z,
    scratch.bna_test_blocks blocks
WHERE
    ST_Intersects(blocks."geom",z.geom)
    AND ST_Area(ST_Intersection(z.geom,blocks."geom")) > (ST_Area(blocks."geom") * 0.9)
GROUP BY z.id
;

INSERT INTO scratch.zones
SELECT
    ARRAY["BLOCKID10"],
    ST_Multi("geom")
FROM scratch.bna_test_blocks blocks
WHERE NOT EXISTS (
    SELECT 1
    FROM scratch.zones z
    WHERE blocks."BLOCKID10" = ANY(z.block_ids)
);

CREATE INDEX sidx_scratch_zones ON scratch.zones USING GIST (geom);
ALTER TABLE scratch.zones ADD COLUMN id SERIAL PRIMARY KEY;

-- add nodes to zones starting with all nodes for single-block zones,
-- then for multi-block zones take the one closest to the center and then
-- four additional nodes distributed around the zone
DROP TABLE IF EXISTS tmp_blockzones;
SELECT
    id AS zone_id,
    unnest(block_ids) AS block_id,
    ST_Centroid(geom) AS geom
INTO TEMP TABLE tmp_blockzones
FROM scratch.zones
WHERE array_length(block_ids,1) > 1
;
CREATE INDEX tidx_tmp_blockzones_block_id ON tmp_blockzones (block_id);
CREATE INDEX tsidx_tmp_blockzones ON tmp_blockzones USING GIST (geom);
ANALYZE tmp_blockzones;

DROP TABLE IF EXISTS tmp_blockunnest;
SELECT
    "BLOCKID10" AS block_id,
    unnest(road_ids) AS road_id
INTO TEMP TABLE tmp_blockunnest
FROM scratch.bna_test_blocks
;

DROP TABLE IF EXISTS tmp_block_nodes_unnest;
SELECT
    tmp_blockunnest.block_id,
    nodes.node_id AS node_id,
    nodes."geom" AS geom
INTO TEMP TABLE tmp_block_nodes_unnest
FROM
    tmp_blockunnest,
    scratch.bna_test_net_vert nodes
WHERE tmp_blockunnest.road_id = nodes.road_id
;
CREATE INDEX tidx_block_nodes_unnest_block_id ON tmp_block_nodes_unnest (block_id);
CREATE INDEX tidx_block_nodes_unnest_node_id ON tmp_block_nodes_unnest (node_id);
CREATE INDEX tsidx_block_nodes_unnest ON tmp_block_nodes_unnest USING GIST (geom);
ANALYZE tmp_block_nodes_unnest;

DROP TABLE IF EXISTS tmp_block_nodes;
SELECT
    tmp_blockunnest.block_id,
    array_agg(nodes.node_id) AS node_ids
INTO TEMP TABLE tmp_block_nodes
FROM
    tmp_blockunnest,
    scratch.bna_test_net_vert nodes
WHERE tmp_blockunnest.road_id = nodes.road_id
GROUP BY tmp_blockunnest.block_id
;

ALTER TABLE scratch.zones ADD COLUMN node_ids INTEGER[];
UPDATE scratch.zones zones
SET node_ids = tmp_block_nodes.node_ids
FROM tmp_block_nodes
WHERE
    array_length(zones.block_ids,1) = 1
    AND tmp_block_nodes.block_id = ANY(zones.block_ids)
;

-- closest to centroid
DROP TABLE IF EXISTS tmp_zone_node;
SELECT DISTINCT ON (tmp_blockzones.zone_id)
    tmp_blockzones.zone_id,
    tmp_block_nodes_unnest.node_id
INTO TEMP TABLE tmp_zone_node
FROM
    tmp_blockzones,
    tmp_block_nodes_unnest
WHERE tmp_block_nodes_unnest.block_id = tmp_blockzones.block_id
ORDER BY
    tmp_blockzones.zone_id,
    ST_Distance(tmp_blockzones.geom,tmp_block_nodes_unnest.geom)
;

UPDATE scratch.zones zones
SET node_ids = ARRAY[tmp_zone_node.node_id]
FROM tmp_zone_node
WHERE zones.id = tmp_zone_node.zone_id
;

-- furthest from centroid (round 1)
DROP TABLE IF EXISTS tmp_zone_node_dist;
SELECT DISTINCT ON (tmp_blockzones.zone_id, candidate_nodes.node_id)
    tmp_blockzones.zone_id,
    candidate_nodes.node_id,
    ST_Distance(existing_nodes.geom,candidate_nodes.geom) AS dist
INTO TEMP TABLE tmp_zone_node_dist
FROM
    tmp_blockzones,
    scratch.zones zones,
    tmp_block_nodes_unnest existing_nodes,
    tmp_block_nodes_unnest candidate_nodes
WHERE
    tmp_blockzones.block_id = ANY(zones.block_ids)
    AND existing_nodes.node_id = ANY(zones.node_ids)
    AND existing_nodes.block_id = tmp_blockzones.block_id
    AND candidate_nodes.block_id = tmp_blockzones.block_id
ORDER BY
    tmp_blockzones.zone_id,
    candidate_nodes.node_id,
    ST_Distance(existing_nodes.geom,candidate_nodes.geom) ASC
;

DROP TABLE IF EXISTS tmp_zone_node;
SELECT DISTINCT ON (zone_id)
    zone_id,
    node_id
INTO TEMP TABLE tmp_zone_node
FROM tmp_zone_node_dist
ORDER BY
    zone_id,
    dist DESC
;

UPDATE scratch.zones zones
SET node_ids = tmp_zone_node.node_id || zones.node_ids
FROM tmp_zone_node
WHERE zones.id = tmp_zone_node.zone_id
;

-- furthest from centroid (round 2)
DROP TABLE IF EXISTS tmp_zone_node_dist;
SELECT DISTINCT ON (tmp_blockzones.zone_id, candidate_nodes.node_id)
    tmp_blockzones.zone_id,
    candidate_nodes.node_id,
    ST_Distance(existing_nodes.geom,candidate_nodes.geom) AS dist
INTO TEMP TABLE tmp_zone_node_dist
FROM
    tmp_blockzones,
    scratch.zones zones,
    tmp_block_nodes_unnest existing_nodes,
    tmp_block_nodes_unnest candidate_nodes
WHERE
    tmp_blockzones.block_id = ANY(zones.block_ids)
    AND existing_nodes.node_id = ANY(zones.node_ids)
    AND existing_nodes.block_id = tmp_blockzones.block_id
    AND candidate_nodes.block_id = tmp_blockzones.block_id
ORDER BY
    tmp_blockzones.zone_id,
    candidate_nodes.node_id,
    ST_Distance(existing_nodes.geom,candidate_nodes.geom) ASC
;

DROP TABLE IF EXISTS tmp_zone_node;
SELECT DISTINCT ON (zone_id)
    zone_id,
    node_id
INTO TEMP TABLE tmp_zone_node
FROM tmp_zone_node_dist
ORDER BY
    zone_id,
    dist DESC
;

UPDATE scratch.zones zones
SET node_ids = tmp_zone_node.node_id || zones.node_ids
FROM tmp_zone_node
WHERE zones.id = tmp_zone_node.zone_id
;

-- furthest from centroid (round 3)
DROP TABLE IF EXISTS tmp_zone_node_dist;
SELECT DISTINCT ON (tmp_blockzones.zone_id, candidate_nodes.node_id)
    tmp_blockzones.zone_id,
    candidate_nodes.node_id,
    ST_Distance(existing_nodes.geom,candidate_nodes.geom) AS dist
INTO TEMP TABLE tmp_zone_node_dist
FROM
    tmp_blockzones,
    scratch.zones zones,
    tmp_block_nodes_unnest existing_nodes,
    tmp_block_nodes_unnest candidate_nodes
WHERE
    tmp_blockzones.block_id = ANY(zones.block_ids)
    AND existing_nodes.node_id = ANY(zones.node_ids)
    AND existing_nodes.block_id = tmp_blockzones.block_id
    AND candidate_nodes.block_id = tmp_blockzones.block_id
ORDER BY
    tmp_blockzones.zone_id,
    candidate_nodes.node_id,
    ST_Distance(existing_nodes.geom,candidate_nodes.geom) ASC
;

DROP TABLE IF EXISTS tmp_zone_node;
SELECT DISTINCT ON (zone_id)
    zone_id,
    node_id
INTO TEMP TABLE tmp_zone_node
FROM tmp_zone_node_dist
ORDER BY
    zone_id,
    dist DESC
;

UPDATE scratch.zones zones
SET node_ids = tmp_zone_node.node_id || zones.node_ids
FROM tmp_zone_node
WHERE zones.id = tmp_zone_node.zone_id
;

-- furthest from centroid (round 4)
DROP TABLE IF EXISTS tmp_zone_node_dist;
SELECT DISTINCT ON (tmp_blockzones.zone_id, candidate_nodes.node_id)
    tmp_blockzones.zone_id,
    candidate_nodes.node_id,
    ST_Distance(existing_nodes.geom,candidate_nodes.geom) AS dist
INTO TEMP TABLE tmp_zone_node_dist
FROM
    tmp_blockzones,
    scratch.zones zones,
    tmp_block_nodes_unnest existing_nodes,
    tmp_block_nodes_unnest candidate_nodes
WHERE
    tmp_blockzones.block_id = ANY(zones.block_ids)
    AND existing_nodes.node_id = ANY(zones.node_ids)
    AND existing_nodes.block_id = tmp_blockzones.block_id
    AND candidate_nodes.block_id = tmp_blockzones.block_id
ORDER BY
    tmp_blockzones.zone_id,
    candidate_nodes.node_id,
    ST_Distance(existing_nodes.geom,candidate_nodes.geom) ASC
;

DROP TABLE IF EXISTS tmp_zone_node;
SELECT DISTINCT ON (zone_id)
    zone_id,
    node_id
INTO TEMP TABLE tmp_zone_node
FROM tmp_zone_node_dist
ORDER BY
    zone_id,
    dist DESC
;

UPDATE scratch.zones zones
SET node_ids = tmp_zone_node.node_id || zones.node_ids
FROM tmp_zone_node
WHERE zones.id = tmp_zone_node.zone_id
;
