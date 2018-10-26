import pybna
bna = pybna.pyBNA(config="/home/spencer/dev/pybna/pybna/sql/zones/test_config.yaml",verbose=True)
bna.make_zones('zones','scratch')
