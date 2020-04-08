
DROP TABLE IF EXISTS  automated.rebuilt_website_ints;
CREATE TABLE          automated.rebuilt_website_ints AS (
SELECT DISTINCT       intersection_from     AS id,
                      NULL::INT             AS dwnldd_int,
                      NULL::VARCHAR(30)     AS control,
                      FALSE::BOOLEAN        AS island,
                      ST_StartPoint(geom)   AS geom
FROM                  received.website_neighborhood_ways
UNION
SELECT DISTINCT       intersection_to       AS id,
                      NULL::INT             AS dwnldd_int,
                      NULL::VARCHAR(30)     AS control,
                      FALSE::BOOLEAN        AS island,
                      ST_EndPoint(geom)     AS geom
FROM                  received.website_neighborhood_ways
);

ALTER TABLE           automated.rebuilt_website_ints
ADD PRIMARY KEY       (id);

CREATE INDEX          rebuilt_website_ints_geom_idx
          ON          automated.rebuilt_website_ints
       USING          GIST (geom);

-- add contorl and island infomration from the nearest intersection within 25 meters
UPDATE                automated.rebuilt_website_ints t
SET                   dwnldd_int = z.int_id,
                      control    = z.control,
                      island     = z.island
FROM                  (SELECT DISTINCT ON (c.id) c.id, i.id as int_id, i.control, i.island
                      FROM  automated.rebuilt_website_ints c,
                            received.updated_ints i
                      WHERE ST_DWithin(c.geom, i.geom, 15)
                      ORDER BY c.id, ST_Distance(c.geom, i.geom)) z
WHERE                 t.id = z.id;
