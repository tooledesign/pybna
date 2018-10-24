from dbutils import DBUtils


class Zones(DBUtils):
    """pyBNA Destinations class"""

    def __init__(self):
        DBUtils.__init__(self,"")

        # these are vars that come from other classes
        self.config = None
        self.verbose = None
        self.debug = None


    def make_zones(table,schema=None,uid="id",geom="geom"):
        """
        Creates analysis zones that aggregate blocks into logical groupings
        based on islands of 100% low stress connectivity

        args
        table -- table name
        schema -- schema name
        uid -- uid column name
        geom -- geom column name
        """
