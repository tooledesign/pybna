###################################################################
# The Destination class stores a BNA destination for use in pyBNA.
###################################################################
import os
import psycopg2
from psycopg2 import sql
import pandas as pd
import geopandas as gpd
import warnings

from dbutils import DBUtils


class DestinationCategory(DBUtils):
    def __init__(self,config,query_path,sql_subs,db_connection_string):
        """Sets up a new category of BNA destinations and retrieves data from
        the given db table

        config -- dictionary of config settings (usually from yaml passed to parent BNA object)
        query_path -- path to the SQL file for destination calculations
        sql_subs -- dictionary of SQL substitutes from the main BNA

        return: None
        """
        DBUtils.__init__(self,db_connection_string)
        self.config = config
        self.query_path = query_path
        self.category = self.config["name"]
        self.schema, self.table = self.parse_table_name(self.config["table"])
        if self.schema is None:
            self.schema = self.get_schema(self.table)

        self.method = self.config["method"]

        self.hs_column_name = self.category + "_hs"
        self.ls_column_name = self.category + "_ls"
        self.score_column_name = self.category + "_score"

        # self.set_destinations()
        self.query  = self._choose_query(config,sql_subs)


    def __unicode__(self):
        return u'%s destinations' % self.category


    def __repr__(self):
        return u'%s destinations' % self.category


    def _choose_query(self,config,sql_subs):
        """
        Selects an appropriate scoring algorithm based on the inputs in
        the config file.

        Returns psycopg2 SQL composable stubbed out for subbing in a few remaining
        variables like output table name
        """
        if "uid" in config:
            id_column = config["uid"]
        else:
            id_column = self.get_pkid_col(self.table,schema=self.schema)
        # check for incompatible column name
        if self.method == "percentage" and sql.Identifier(id_column) != sql_subs["blocks_id_col"]:
            warnings.warn("Destination ID column doesn't match block ID column for {}".format(self.category))

        if "geom" in config:
            geom_col = config["geom"]
        else:
            geom_col = "geom"

        if "filter" in config:
            filter = sql.SQL(config["filter"])
        else:
            filter = sql.Literal(True)

        sql_subs["destinations_schema"] = sql.Identifier(self.schema)
        sql_subs["destinations_table"] = sql.Identifier(self.table)
        sql_subs["destinations_id_col"] = sql.Identifier(id_column)
        sql_subs["destinations_filter"] = filter
        sql_subs["tmp_table"] = sql.SQL("{tmp_table}")
        sql_subs["connection_true"] = sql.SQL("{connection_true}")
        sql_subs["index"] = sql.SQL("{index}")
        if config["method"] == "count":
            sql_subs["destinations_geom_col"] = sql.Identifier(geom_col)
            sql_file = os.path.join(self.query_path,"02_count_based_score.sql")
        elif config["method"] == "percentage":
            sql_file = os.path.join(self.query_path,"02_percentage_based_score.sql")
            sql_subs["val"] = sql.Identifier(config["datafield"])
        else:
            raise ValueError("Unknown scoring method given for %s" % config["name"])

        raw = self.read_sql_from_file(sql_file)
        conn = self.get_db_connection()
        query = sql.SQL(sql.SQL(raw).format(**sql_subs).as_string(conn))
        conn.close()
        return query
