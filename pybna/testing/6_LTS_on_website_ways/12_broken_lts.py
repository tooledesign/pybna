


import pybna

# calculate stress on website_neighborhood_ways network using pybna tool
bna = pybna.Stress(config='C:\\Users\\dpatterson\\code\\pybna\\pybna\\testing\\6_LTS_on_website_ways\\config_pybna_website_ways_lts_break.yaml',
                   create_lookups=True,
                   verbose=True)

bna.segment_stress(table="automated.website_ways_through_pybna_for_lts_segment_break",
                    table_filter=None,
                    dry="broken_lts")

bna.segment_stress(table="automated.website_ways_through_pybna_for_lts_segment_break",
                    table_filter=None,
                    dry=None)

# bna.crossing_stress(table="automated.website_ways_through_pybna_for_lts_crossing",
#                     angle=20,
#                     table_filter=None,
#                     dry=None)
