import pybna
bna = pybna.pyBNA(config="/home/spencer/dev/pybna/pybna/sql/zones/test_config.yaml",verbose=True)
bna.make_zones('zones','scratch',roads_filter="f_class IN ('1','2','3','4','6','7')")
