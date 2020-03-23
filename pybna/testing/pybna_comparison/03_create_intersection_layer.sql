


DROP TABLE IF EXISTS  scratch.dummy_ints;
CREATE TABLE          scratch.dummy_ints AS (
SELECT DISTINCT       intersection_from as id,
                      ST_StartPoint(geom) AS geom
FROM                  received.website_neighborhood_ways
UNION
SELECT DISTINCT       intersection_to as id,
                      ST_EndPoint(geom) AS  geom
FROM                  received.website_neighborhood_ways
);

ALTER TABLE           scratch.dummy_ints
ADD PRIMARY KEY       (id);

CREATE INDEX          madison_ints_geom_idx
          ON          scratch.dummy_ints
       USING          GIST (geom);
