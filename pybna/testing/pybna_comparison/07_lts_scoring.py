


import pybna

# calculate stress
bna = pybna.Stress(config='C:\\Users\\dpatterson\\code\\pybna\\pybna\\testing\\pybna_comparison\\config_website_lts.yaml',
                   create_lookups=True,
                   verbose=False)

bna.segment_stress(table="received.updated_segment_stress_scores",
                 table_filter=None,
                 dry=None)

bna.crossing_stress(table="received.updated_crossing_stress_scores",
                  angle=20,
                  table_filter=None,
                  dry=None)
