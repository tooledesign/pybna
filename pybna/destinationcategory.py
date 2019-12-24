###################################################################
# The Destination class stores a BNA destination category for use in pyBNA.
###################################################################
import os
import psycopg2
from psycopg2 import sql
import warnings

from dbutils import DBUtils


class DestinationCategory(DBUtils):
    def __init__(self,config,db_connection_string,workspace_schema=None):
        """Sets up a new category of BNA destinations and retrieves data from
        the given db table

        config -- dictionary of config settings (usually from yaml passed to parent BNA object)
        db_connection_string -- string to connect to the database
        workspace_schema -- schema to save interim working tables to

        return: None
        """
        self.module_dir = os.path.dirname(os.path.abspath(__file__))
        DBUtils.__init__(self,db_connection_string)
        self.config = config

        if not self.table_exists(config.table):
            warnings.warn("No table found for {}".format(config.name))
        else:
            schema, table = self.parse_table_name(config.table)
            if schema is None:
                schema = self.get_schema(table)

            if "uid" in config:
                id_column = config.uid
            else:
                id_column = self.get_pkid_col(table,schema=schema)

            if "geom" in config:
                geom_col = config.geom
            else:
                geom_col = "geom"

            if "filter" in config:
                filter = sql.SQL(config.filter)
            else:
                filter = sql.Literal(True)

            if workspace_schema is None:
                self.workspace_schema = "pg_temp"
                self.persist = False
            else:
                self.workspace_schema = workspace_schema
                self.persist = True
            self.high_stress_name = config.name + "_hs"
            self.low_stress_name = config.name + "_ls"

            self.sql_subs = {
                "destinations_schema": sql.Identifier(schema),
                "destinations_table": sql.Identifier(table),
                "destinations_id_col": sql.Identifier(id_column),
                "workspace_schema": sql.Identifier(self.workspace_schema),
                "destinations_filter": filter
            }

            if config["method"] == "count":
                self.sql_subs["destinations_geom_col"] = sql.Identifier(geom_col)
            elif config["method"] == "percentage":
                self.sql_subs["val"] = sql.Identifier(config["datafield"])
            else:
                raise ValueError("Unknown scoring method given for {}".format(config.name))


    def __repr__(self):
        return u"{} destinations".format(self.config.name)


    @property
    def query(self):
        """
        Returns the raw query language appropriate to this category
        """
        dirs = [self.module_dir,"sql","destinations"]
        if self.config.method == "count":
            dirs.append("02_count_based_score.sql")
        elif self.config.method == "percentage":
            dirs.append("02_percentage_based_score.sql")
        else:
            raise ValueError("Unknown scoring method given for {}".format(self.config.name))

        return self.read_sql_from_file(os.path.join(*dirs))


    def count_connections(self,subs,conn=None):
        """
        Counts the number of destinations accessible to each block under high
        and low stress conditions

        args
        subs -- a list of sql substitutes to complement the substitutes
            associated with this category (generally from the main BNA object)
        conn -- a DB connection object. If none start a new connection and close it
            when complete
        """
        if conn is None:
            close_conn = True
            conn = self.get_db_connection()
        else:
            close_conn = False


        hs_subs = {
            "workspace_table": sql.Identifier(self.high_stress_name),
            "index": sql.Identifier("tidx_"+self.high_stress_name+"_block_id"),
            "connection_true": sql.Literal(True)
        }
        hs_subs.update(subs)
        hs_subs.update(self.sql_subs)

        ls_subs = {
            "workspace_table": sql.Identifier(self.low_stress_name),
            "index": sql.Identifier("tidx_"+self.low_stress_name+"_block_id"),
            "connection_true": sql.SQL("low_stress")
        }
        ls_subs.update(subs)
        ls_subs.update(self.sql_subs)

        self._run_sql(self.query,hs_subs,conn=conn)
        self._run_sql(self.query,ls_subs,conn=conn)

        if close_conn:
            conn.close()


    def calculate_score(self,subs,conn=None):
        """
        Calculates the score for this destination category

        args
        subs -- a list of sql substitutes to complement the substitutes
            associated with this category (generally from the main BNA object)
        conn -- a DB connection object. If none start a new connection and close it
            when complete
        """
        if conn is None:
            close_conn = True
            conn = self.get_db_connection()
        else:
            close_conn = False
