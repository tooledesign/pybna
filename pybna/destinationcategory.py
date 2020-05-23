###################################################################
# The Destination class stores a BNA destination category for use in pyBNA.
###################################################################
import os
import psycopg2
from psycopg2 import sql
import numpy as np
import warnings

from .dbutils import DBUtils


class DestinationCategory(DBUtils):
    def __init__(self,config,db_connection_string,workspace_schema=None):
        """Sets up a new category of BNA destinations and retrieves data from
        the given db table

        config : dict
            dictionary of config settings (usually from yaml passed to parent BNA object)
        db_connection_string : str
            string to connect to the database
        workspace_schema : str, optional
            schema to save interim working tables to

        return: None
        """
        self.module_dir = os.path.dirname(os.path.abspath(__file__))
        DBUtils.__init__(self,db_connection_string)
        self.config = config
        self.has_subcats = False
        self.has_count = False

        if "maxpoints" in config:
            self.maxpoints = self.config.maxpoints
        else:
            self.maxpoints = None

        if "subcats" in config:
            self.has_subcats = True

        if "table" in config:
            self.has_count = True
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
                    if isinstance(config.geom, str):
                        geom_col = sql.Identifier(config.geom)
                    else:
                        cols = [sql.Identifier(c) for c in config.geom]
                        geom_col = sql.SQL("COALESCE(") \
                                    + sql.SQL(",").join([sql.Identifier(c) for c in config.geom]) \
                                    + sql.SQL(")")
                else:
                    geom_col = sql.SQL("geom")

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
                self.workspace_table = "scores_" + config.name

                self.sql_subs = {
                    "destinations_schema": sql.Identifier(schema),
                    "destinations_table": sql.Identifier(table),
                    "destinations_id_col": sql.Identifier(id_column),
                    "index": sql.Identifier("idx_{}_blocks".format(config.name)),
                    "workspace_schema": sql.Identifier(self.workspace_schema),
                    "workspace_table": sql.Identifier(self.workspace_table),
                    "destinations_filter": filter
                }

                if config["method"] == "count":
                    self.sql_subs["destinations_geom_col"] = geom_col
                elif config["method"] == "percentage":
                    self.sql_subs["val"] = sql.Identifier(config["datafield"])
                else:
                    raise ValueError("Unknown scoring method given for {}".format(config.name))


    def __repr__(self):
        return "{} destinations\nmaxpoints: {}\nhas subcats? {}".format(self.config.name,self.maxpoints,self.has_subcats)


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

        Parameters
        ----------
        subs : dict
            a list of sql substitutes to complement the substitutes
            associated with this category (generally from the main BNA object)
        conn : psycopg2 connection object, optional
            a DB connection object. If none start a new connection and close it
            when complete
        """
        if not self.has_count:
            return

        if conn is None:
            close_conn = True
            conn = self.get_db_connection()
        else:
            close_conn = False

        subs.update(self.sql_subs)

        hs_subs = {
            "tbl": sql.Identifier("high_stress"),
            "connection_true": sql.Literal(True)
        }
        hs_subs.update(subs)

        ls_subs = {
            "tbl": sql.Identifier("low_stress"),
            "connection_true": sql.SQL("low_stress")
        }
        ls_subs.update(subs)

        self._run_sql(self.query,hs_subs,conn=conn)
        self._run_sql(self.query,ls_subs,conn=conn)
        self._run_sql_script("03_combine_counts.sql",subs,["sql","destinations"],conn=conn)

        if close_conn:
            conn.close()


    def calculate_score(self,subs,conn=None):
        """
        Calculates the score for this destination category

        Parameters
        ----------
        subs : dict
            a list of sql substitutes to complement the substitutes
            associated with this category (generally from the main BNA object)
        conn : psycopg2 connection object, optional
            a DB connection object. If none start a new connection and close it
            when complete
        """
        if not self.has_count:
            return

        if conn is None:
            close_conn = True
            conn = self.get_db_connection()
        else:
            close_conn = False

        subs.update(self.sql_subs)
        subs["case"] = self._concat_case("hs","ls")

        self._run_sql("""
            UPDATE {workspace_schema}.{workspace_table}
            SET score = {case}
        """,subs,conn=conn)


    def _concat_case(self,hs_column,ls_column):
        """
        Builds a case statement for comparing high stress and low stress destination
        counts using defined break points

        Parameters
        ----------
        hs_column : str
            the name of the column with high stress destination counts
        ls_column : str
            the name of the column with low stress destination counts

        returns
        a composed psycopg2 SQL object representing a full CASE ... END statement
        """
        # add zero
        breaks = dict(self.config.breaks)
        breaks[0] = 0

        subs = {
            "hs_column": sql.Identifier(hs_column),
            "ls_column": sql.Identifier(ls_column),
            "maxpoints": sql.Literal(self.maxpoints)
        }

        case = sql.SQL("""
            CASE
            WHEN COALESCE({hs_column},0) = 0 AND COALESCE({ls_column},0) = 0 THEN NULL
            WHEN COALESCE({ls_column},0) >= COALESCE({hs_column},0) THEN {maxpoints}
            WHEN COALESCE({hs_column},0) = COALESCE({ls_column},0) THEN {maxpoints}
        """).format(**subs)

        # assign scores at the boundaries
        cumul_score = 0
        for brk, score in sorted(breaks.items()):
            subs["break"] = sql.Literal(brk)
            subs["score"] = sql.Literal(score)
            subs["cumul_score"] = sql.Literal(cumul_score)
            if self.config.method == "count":
                case += sql.SQL("""
                    WHEN COALESCE({ls_column},0) = {break} THEN {score} + {cumul_score}
                """).format(**subs)
                cumul_score += score
            if self.config.method == "percentage":
                case += sql.SQL("""
                    WHEN COALESCE({ls_column},0) = {break} THEN {score}
                """).format(**subs)
                cumul_score = score

        # assign scores within the boundaries
        del breaks[0]
        cumul_score = 0
        prev_break = 0
        for brk, score in sorted(breaks.items()):
            subs["break"] = sql.Literal(brk)
            subs["score"] = sql.Literal(score)
            subs["cumul_score"] = sql.Literal(cumul_score)
            subs["prev_break"] = sql.Literal(prev_break)
            if self.config.method == "count":
                subs["val"] = sql.SQL("COALESCE({ls_column},0)").format(**subs)
                case += sql.SQL("""
                    WHEN {val} < {break}
                        THEN {cumul_score} + (({val} - {prev_break})::FLOAT/({break} - {prev_break})) * ({score} - {cumul_score})
                """).format(**subs)
                cumul_score += score
            if self.config.method == "percentage":
                subs["val"] = sql.SQL("(COALESCE({ls_column},0)::FLOAT/{hs_column})").format(**subs)
                case += sql.SQL("""
                    WHEN {val} < {break}
                        THEN {cumul_score} + (({val} - {prev_break})::FLOAT/({break} - {prev_break})) * ({score} - {cumul_score})
                """).format(**subs)
                cumul_score = score
            prev_break = brk

        # assign scores for top (if not already assigned in breaks)
        brk, score = sorted(breaks.items())[-1]
        subs["break"] = sql.Literal(brk)
        subs["cumul_score"] = sql.Literal(cumul_score)
        if np.isclose(self.maxpoints,cumul_score):
            case += sql.SQL("""
                WHEN {val} > {break} THEN {maxpoints}
            """).format(**subs)
        elif self.maxpoints > cumul_score:
            if self.config.method == "count":
                case += sql.SQL("""
                    ELSE {cumul_score} + ((COALESCE({ls_column},0) - {break})::FLOAT/({hs_column} - {break})) * ({maxpoints} - {cumul_score})
                """).format(**subs)
            elif self.config.method == "percentage":
                case += sql.SQL("""
                    ELSE {cumul_score} + ((COALESCE({ls_column},0)::FLOAT/{hs_column}) - {break})::FLOAT/(1 - {break}) * ({maxpoints} - {cumul_score})
                """).format(**subs)
        case += sql.SQL(" END")

        return case
