


import pybna

# calculate stress on downloaded OSM network using pybna tool
bna = pybna.Stress(config='C:\\Users\\dpatterson\\code\\pybna\\pybna\\testing\\5_LTS_on_imported_data\\config_website_lts.yaml',
                   create_lookups=True,
                   verbose=False)

bna.segment_stress(table="received.updated_segment_stress_scores",
                 table_filter=None,
                 dry=None)

bna.crossing_stress(table="received.updated_crossing_stress_scores",
                  angle=20,
                  table_filter=None,
                  dry=None)
