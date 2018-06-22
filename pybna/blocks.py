###################################################################
# Subclass of geodataframe to hold information about blocks
###################################################################
import geopandas as gpd


class Blocks:
    """pyBNA Blocks class"""

    def __init__(self):
        self.blocks = None
        self.table = None
        self.schema = None
        self.id_column = None
        self.geom = None
        self.pop_column = None
