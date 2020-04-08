
import pybna

#config="/home/spencer/dev/pybna/pybna/testing/4_pybna_connectivity_and_destinations/config_pybna.yaml"
config="C:\\Users\\dpatterson\\code\\pybna\\pybna\\testing\\4_pybna_connectivity_and_destinations\\config_pybna.yaml"

# connectivity
bna = pybna.pyBNA(config=config,
                  force_net_build=False,
                  verbose=False,
                  debug=False,
                  host=None,
                  db_name=None,
                  user=None,
                  password=None)
# use wrapper
bna.score_destinations(output_table="automated.pybna_scores",
                       scenario_id=None,                                       # no scenario ID
                       subtract=False,                                         # no scenario condition
                       with_geoms=True,                                        # inlcude geoms
                       overwrite=True,                                         # overwrite
                       dry=None)
