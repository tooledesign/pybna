
import pybna

# connectivity
bna = pybna.pyBNA(config='C:\\Users\\dpatterson\\code\\pybna\\pybna\\testing\\pybna_comparison\\config_website.yaml',
                  force_net_build=False,
                  verbose=False,
                  debug=False,
                  host=None,
                  db_name=None,
                  user=None,
                  password=None)

bna.score_destinations(output_table="automated.bna_score_destinations",
                       scenario_id=None,                                       # no scenario ID
                       subtract=False,                                         # no scenario condition
                       with_geoms=True,                                        # inlcude geoms
                       overwrite=True,                                         # overwrite
                       dry=None)
