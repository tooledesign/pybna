


import pybna

# calculate stress on website_neighborhood_ways network using pybna tool
bna = pybna.Stress(config='C:\\Users\\dpatterson\\code\\pybna\\pybna\\testing\\6_LTS_on_website_ways\\config_pybna_website_ways_lts.yaml',
                   create_lookups=True,
                   verbose=False)

bna.segment_stress(table="received.website_ways_pybna_stress_scores",
                    table_filter=None,
                    dry=None)

bna.crossing_stress(table="received.website_crossing_pybna_stress_scores",
                    angle=20,
                    table_filter=None,s
                    dry=None)
