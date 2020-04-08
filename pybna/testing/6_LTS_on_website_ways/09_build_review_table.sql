
DROP TABLE IF EXISTS  automated.website_pybna_lts_neighborhood_ways;
SELECT                *,
                      NULL::BIGINT as ft_seg_stress_pybna,
                      NULL::BIGINT as tf_seg_stress_pybna,
                      NULL::BIGINT as ft_cross_stress_pybna,
                      NULL::BIGINT as tf_cross_stress_pybna
INTO                  automated.website_pybna_lts_neighborhood_ways
FROM                  received.website_neighborhood_ways;

ALTER TABLE           automated.website_pybna_lts_neighborhood_ways
ADD PRIMARY KEY       (id);

CREATE INDEX          madison_pybna_lts_neighborhood_ways_geom_idx
          ON          automated.website_pybna_lts_neighborhood_ways
       USING          GIST (geom);
ANALYZE               automated.website_pybna_lts_neighborhood_ways;
