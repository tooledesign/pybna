###################################################################
# The Destination class stores a BNA destination for use in pyBNA.
###################################################################
import psycopg2
from psycopg2 import sql
import pandas as pd
import geopandas as gpd


class DestinationCategory:
    def __init__(self,bna,config,verbose=False,debug=False):
        """Sets up a new category of BNA destinations and retrieves data from
        the given db table

        bna -- reference to the parent bna object
        config -- dictionary of config settings (usually from yaml passed to parent BNA object)
        verbose -- output useful messages
        debug -- run in debug mode

        return: None
        """
        self.bna = bna
        self.config = config
        self.category = self.config["name"]
        self.table = self.config["table"]
        if "schema" in self.config:
            self.schema = self.config["schema"]
        else:
            self.schema = self.bna.get_schema(self.table)
        self.blocks_col = self.config["blocks"]
        if "uid" in self.config:
            self.id_col = self.config["uid"]
        else:
            self.id_col = self.bna.get_pkid_col(self.table)
        if "geom" in self.config:
            self.geom_col = self.config["geom"]
        else:
            self.geom_col = "geom"
        self.method = self.config["method"]
        self.verbose = verbose
        self.debug = debug

        self.ls_population = None
        self.hs_population = None

        self.set_destinations()
        self.query = self._select_query()


    def __unicode__(self):
        n = len(self.destinations)
        return u'%s: %i destinations' % (self.category, n)


    def __repr__(self):
        n = len(self.destinations)
        return r'%s: %i destinations' % (self.category, n)


    def set_destinations(self):
        """Retrieve destinations from the database and store them in
        this class' dataframe of destinations

        return: None
        """
        if self.verbose:
            print('Getting destinations for %s from %s' % (self.category,table))

        conn = self.bna.get_db_connection()
        cur = conn.cursor()

        subs = {
            "id": sql.Identifier(self.id_col),
            "blocks": sql.Identifier(self.blocks_col),
            "table": sql.Identifier(self.table)
        }

        q = sql.SQL('select {id}, {blocks} from {table};').format(**subs)

        if self.debug:
            print(q.as_string(conn))

        return pd.DataFrame.from_postgis(
            q,
            conn
        )


    def set_population(self,blocks,connected_blocks):
        pass
        # self.ls_population =
        # self.hs_population =


    def set_query(self,table,schema):
        """
        Prepares the query language for this category based on factors from
        its configuration

        args:
        table -- the table to apply the query to
        schema -- the table's schema
        """
        pass
