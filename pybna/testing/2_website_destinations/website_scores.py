
import pybna
config="/home/spencer/dev/pybna/pybna/testing/2_website_destinations/config_website.yaml"
# config="C:\\Users\\dpatterson\\code\\pybna\\pybna\\testing\\2_website_destinations\\config_website.yaml"

# connectivity
bna = pybna.pyBNA(config=config)
bna.score_destinations(output_table="automated.website_scores",
                       with_geoms=True,
                       overwrite=True)
