

import pybna

#config="/home/spencer/dev/pybna/pybna/testing/4_pybna_connectivity_and_destinations/config_pybna.yaml"
config="C:\\Users\\dpatterson\\code\\pybna\\pybna\\testing\\4_pybna_connectivity_and_destinations\\config_pybna.yaml"

# connectivity
bna = pybna.pyBNA(config='C:\\Users\\dpatterson\\code\\pybna\\pybna\\testing\\pybna_comparison\\config_website.yaml',
                  force_net_build=False,
                  verbose=False,
                  debug=False,
                  host=None,
                  db_name=None,
                  user=None,
                  password=None)

bna.calculate_connectivity(blocks=None,                                         # use all blocks
                           network_filter=None,                                 # no filter
                           append=False,                                        #no append
                           dry=None)
