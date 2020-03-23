

DROP TABLE IF EXISTS  automated.website_ways_intersections;
CREATE TABLE          automated.website_ways_intersections (
                      id_0 SERIAL PRIMARY KEY,
                      seg_id INT,
                      geom geometry(point,26916),
                      control TEXT,
                      island BOOLEAN DEFAULT FALSE
);

INSERT INTO           automated.website_ways_intersections (geom)
SELECT                geom
FROM                  tdg_MakeIntersections('received.website_neighborhood_ways',10) geom;


CREATE INDEX          madison_ints_geom_idx
          ON          automated.website_ways_intersections
       USING          GIST (geom);
