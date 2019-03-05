import pybna
bna = pybna.pyBNA(config="/home/spencer/dev/pybna/pybna/sql/zones/test_config.yaml",verbose=True)
bna.calculate_connectivity(tiles=[10],dry=True)
bna.make_zones_from_network('zones','scratch',roads_filter="f_class IN ('1','2','3','4','6','7')")
bna.make_zones_no_aggregation('zones','scratch')
