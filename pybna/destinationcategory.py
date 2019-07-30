###################################################################
# The Destination class stores a BNA destination for use in pyBNA.
###################################################################
import os
import psycopg2
from psycopg2 import sql
import pandas as pd
import geopandas as gpd

from dbutils import DBUtils


class DestinationCategory(DBUtils):
    def __init__(self,bna,config,query_path,verbose=False,debug=False):
        """Sets up a new category of BNA destinations and retrieves data from
        the given db table

        bna -- reference to the parent bna object
        config -- dictionary of config settings (usually from yaml passed to parent BNA object)
        verbose -- output useful messages
        debug -- run in debug mode

        return: None
        """
        DBUtils.__init__(self,"")
        self.bna = bna
        self.config = config
        self.query_path = query_path
        self.category = self.config["name"]
        self.schema, self.table = self.parse_table_name(self.config["table"])
        if self.schema is None:
            self.schema = self.bna.get_schema(self.table)
        self.blocks_col = self.config["blocks"]
        if "uid" in self.config:
            self.id_column = self.config["uid"]
        else:
            self.id_column = self.bna.get_pkid_col(self.table)
        if "geom" in self.config:
            self.geom_col = self.config["geom"]
        else:
            self.geom_col = "geom"
        self.method = self.config["method"]
        self.verbose = verbose
        self.debug = debug

        self.hs_column_name = self.category + "_hs"
        self.ls_column_name = self.category + "_ls"
        self.score_column_name = self.category + "_score"

        self.ls_population = None
        self.hs_population = None

        # self.set_destinations()
        self.query  = self._choose_query()


    def __unicode__(self):
        return u'%s destinations' % self.category


    def __repr__(self):
        return u'%s destinations' % self.category


    # def set_destinations(self):
    #     """Retrieve destinations from the database and store them in
    #     this class' dataframe of destinations
    #
    #     return: None
    #     """
    #     if self.verbose:
    #         print('Getting destinations for %s from %s' % (self.category,table))
    #
    #     conn = self.bna.get_db_connection()
    #     cur = conn.cursor()
    #
    #     subs = {
    #         "id": sql.Identifier(self.id_column),
    #         "blocks": sql.Identifier(self.blocks_col),
    #         "table": sql.Identifier(self.table)
    #     }
    #
    #     q = sql.SQL('select {id}, {blocks} from {table};').format(**subs)
    #
    #     if self.debug:
    #         print(q.as_string(conn))
    #
    #     return pd.DataFrame.from_postgis(
    #         q,
    #         conn
    #     )


    def set_population(self,blocks,connected_blocks):
        pass
        # self.ls_population =
        # self.hs_population =

    #
    # def score_destinations(self,out_field,stress,blocks_table=None,schema=None,dry=False):
    #     """
    #     Sets the given blocks table and field to scores for this set of destinations.
    #
    #     args:
    #     out_field -- the field to update
    #     stress -- one of [high, low]: whether to score high or low stress connections
    #     blocks_table -- the table of blocks to update (if none use the config blocks table)
    #     schema -- schema for the table. default is the schema where the census block table is stored.
    #     """
    #     # check inputs
    #     if not stress in ["high","low"]:
    #         raise ValueError("Stress must be given as \"high\" or \"low\"")
    #     if blocks_table is None:
    #         blocks_table = self.blocks.blocks_table
    #     if schema is None:
    #         schema = self.blocks.schema
    #
    #     q, subs = self._choose_query()
    #
    #     # add general stuff
    #
    #     q = q.format(**subs)
    #
    #     conn = self.bna.get_db_connection()
    #     if dry:
    #         print(q.as_string(conn))
    #     else:
    #         cur = conn.cursor()
    #         cur.execute(q)
    #         conn.commit()
    #     conn.close()


    def _choose_query(self):
        """
        Selects an appropriate scoring algorithm based on the inputs in
        the config file.

        Returns psycopg2 SQL composable stubbed out for subbing in a few remaining
        variables like output table name
        """
        # start with dict of universal subs
        subs = {
            "schema": sql.Identifier(self.schema),
            "table": sql.Identifier(self.table),
            "block_id_col": sql.Identifier(self.blocks_col),
            "block_connections": sql.Identifier(self.bna.config["bna"]["connectivity"]["table"]),
            "source_block": sql.Identifier(self.bna.config["bna"]["connectivity"]["source_column"]),
            "target_block": sql.Identifier(self.bna.config["bna"]["connectivity"]["target_column"]),
            "tmp_table": sql.SQL("{tmp_table}"),
            "connection_true": sql.SQL("{connection_true}"),
            "index": sql.SQL("{index}")
        }
        if self.config["method"] == "count":
            sql_file = os.path.join(self.query_path,"count_based_score.sql")
            subs["destination_id"] = sql.Identifier(self.id_column)
        elif self.config["method"] == "percentage":
            sql_file = os.path.join(self.query_path,"percentage_based_score.sql")
            subs["val"] = sql.Identifier(self.config["datafield"])
        else:
            raise ValueError("Unknown scoring method given for %s" % self.config["name"])

        f = open(sql_file)
        raw = f.read()
        f.close()

        conn = self.bna.get_db_connection()
        return sql.SQL(sql.SQL(raw).format(**subs).as_string(conn))
        conn.close()
