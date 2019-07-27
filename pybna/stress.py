#! /usr/bin/python

# Module for conducting level of traffic stress analysis on roadway datasets.
# Allows for flexible naming of columns via a config file.
import os, StringIO
import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor
import yaml
import pandas as pd

from conf import Conf
from dbutils import DBUtils
from core import FORWARD_DIRECTION
from core import BACKWARD_DIRECTION


class Stress(DBUtils,Conf):
    def __init__(self, config=None, create_lookups=True,
                 verbose=False,debug=False):
        """
        Reads the config file, sets up a connection

        args
        config -- path to the config file, if not given use the default config.yaml
        create_lookups -- creates lookup tables in the db if none are found
        verbose -- output useful messages
        debug -- generates debug outputs
        """
        Conf.__init__(self)
        self.verbose = verbose
        self.debug = debug
        self.module_dir = os.path.dirname(os.path.abspath(__file__))
        if config is None:
            config = os.path.join(self.module_dir,"config.yaml")
        self.config = self.parse_config(yaml.safe_load(open(config)))
        print("Connecting to database")
        host = self.config.db.host
        db_name = self.config.db.dbname
        user = self.config.db.user
        password = self.config.db.password
        db_connection_string = " ".join([
            "dbname=" + db_name,
            "user=" + user,
            "host=" + host,
            "password=" + password
        ])
        DBUtils.__init__(self,db_connection_string,self.verbose,self.debug)
        schema, table = self.parse_table_name(self.config.bna.network.roads.table)
        self.table = table
        if schema is None:
            self.schema = self.get_schema(self.table)
        else:
            self.schema = schema

        # check for and set lookup tables
        if self.verbose:
            print("Checking lookup tables")
        missing = self._missing_lookup_tables()
        if create_lookups and len(missing) > 0:
            for t in missing:
                self._create_lookup_table(*t)

        # add functions to db
        self._run_sql_script("bna_CompareAzimuths.sql",dict(),dirs=["sql","stress","db_functions"])
        self._run_sql_script("bna_IsCorridor.sql",dict(),dirs=["sql","stress","db_functions"])
        self._run_sql_script("bna_MultiEndPoint.sql",dict(),dirs=["sql","stress","db_functions"])
        self._run_sql_script("bna_MultiStartPoint.sql",dict(),dirs=["sql","stress","db_functions"])

        # build SQL substitutions
        self.segment_subs = dict()
        for direction in [FORWARD_DIRECTION,BACKWARD_DIRECTION]:
            self.segment_subs[direction] = self._build_segment_sql_substitutions(direction)

        self.crossing_subs = dict()
        for direction in [FORWARD_DIRECTION,BACKWARD_DIRECTION]:
            self.crossing_subs[direction] = self._build_crossing_sql_substitutions(direction)


    def __repr__(self):
        return "Stress object  |  %r  |  table: %r" % (self.db_connection_string,
                                                       self.table)


    def _missing_lookup_tables(self):
        """
        Check for the lookup tables specified in the config file. Returns any
        missing tables as a tuple of (table type, table name)

        returns:
        list
        """
        missing = []
        for k, v in self.config.stress.lookup_tables.iteritems():
            schema, table = self.parse_table_name(v)
            if not self.table_exists(table,schema):
                missing.append((k,table,schema))
                if self.verbose:
                    print("Table %s not identified, will create" % v)
        return missing


    def _create_lookup_table(self,lu_type,table,schema=None,fname=None):
        """
        Create a stress lookup table of the given type and name

        args
        lu_type -- either shared, bike_lane, or crossing
        table -- name of the table
        schema -- name of the schema
        fname -- optional csv file to populate the table with (if empty uses default)
        """
        in_file = None
        if fname:
            if os.path.isfile(fname):
                in_file = fname
            else:
                raise ValueError("File not found at %s" % fname)

        if lu_type == "shared":
            columns = [
                ("lanes", "integer"),
                ("marked_centerline", "boolean"),
                ("speed", "integer"),
                ("effective_aadt", "integer"),
                ("stress", "integer")
            ]
            if not in_file:
                in_file = os.path.join(self.module_dir,"sql","stress","tables","stress_shared.csv")
        elif lu_type == "bike_lane":
            columns = (
                ("lanes", "integer"),
                ("oneway", "boolean"),
                ("parking", "boolean"),
                ("reach", "integer"),
                ("speed", "integer"),
                ("stress", "integer")
            )
            if not in_file:
                in_file = os.path.join(self.module_dir,"sql","stress","tables","stress_bike_lane.csv")
        elif lu_type == "crossing":
            columns = (
                ("control", "text"),
                ("lanes", "integer"),
                ("speed", "integer"),
                ("island", "boolean"),
                ("stress", "integer")
            )
            if not in_file:
                in_file = os.path.join(self.module_dir,"sql","stress","tables","stress_crossing.csv")
        else:
            raise ValueError("Unrecognized lookup table %s" % lu_type)


        conn = self.get_db_connection()
        cur = conn.cursor()
        if schema:
            q = sql.SQL(" \
                create table {}.{} (id serial primary key, \
                " + ",".join(" ".join(c) for c in columns) + ")"
            ).format(
                sql.Identifier(schema),
                sql.Identifier(table)
            )
        else:
            q = sql.SQL(" \
                create table {} (id serial primary key, \
                " + ",".join(" ".join(c) for c in columns) + ")"
            ).format(
                sql.Identifier(table)
            )

        try:
            cur.execute(q)
        except Exception as e:
            print("Error creating table %s" % table)
            raise e

        if self.verbose:
            print("Copying default stress thresholds into %s" % table)
        # f = StringIO.StringIO(in_file)
        f = open(in_file)
        cur.copy_from(f,table,columns=[c[0] for c in columns],sep=";",null="")
        cur.close()
        conn.commit()
        conn.close()
        f.close()


    def segment_stress(self,table,table_filter=None,dry=None):
        """
        Creates a new table of LTS scores for each direction (forward/backward).
        The new table also includes the attributes (actual or assumed) that were
        used the calculate the LTS score.
        This is basically a pass-through function that calls several helpers to
        calculate the various parts

        args
        table -- the table root (optionally schema-qualified) to use for outputting
            LTS scores. Final table name will have forward/backward appended to
            indicate the direction the score applies to.
        table_filter -- SQL filter to limit rows that should be updated
        dry -- file path to save sql statements to instead of running them in
            the DB
        """
        schema, table = self.parse_table_name(table)
        if schema is None:
            schema = self.schema

        if table_filter:
            table_filter = sql.SQL(table_filter)
        else:
            table_filter = sql.SQL("TRUE")

        if not dry is None:
            if os.path.isfile(dry):
                raise ValueError("File already exists at {}".format(dry))

        conn = self.get_db_connection()
        try:
            for direction in [FORWARD_DIRECTION,BACKWARD_DIRECTION]:
                if self.verbose:
                    print("  ....{}".format(direction))

                # create output table
                subs = self.segment_subs[direction].copy()
                subs["out_schema"] = sql.Identifier(schema)
                subs["out_table"] = sql.Identifier("_".join([table,direction]))
                self._run_sql_script("create_output.sql",subs,dirs=["sql","stress","segment"],conn=conn,dry=dry)

                # call the various segment stress methods
                self._segment_stress_shared(conn,subs,table_filter,dry=dry)
                self._segment_stress_bike_lane(conn,subs,table_filter,dry=dry)
                self._segment_stress_track(conn,subs,table_filter,dry=dry)
                self._segment_stress_path(conn,subs,table_filter,dry=dry)

                # copy back to the base table
                subs["stress"] = sql.Identifier(self.config.bna.network.roads.stress.segment[direction])
                self._run_sql_script("copy_to_base.sql",subs,dirs=["sql","stress"],conn=conn,dry=dry)

        except Exception as e:
            if conn.closed == 0:
                conn.rollback()
                conn.close()
            raise e

        conn.commit()
        conn.close()


    def _segment_stress_shared(self,conn,subs,table_filter=None,dry=None):
        """
        Calculates segment stress for shared lanes

        * NB: this method does not commit any transactions on the connection
        object passed to it. Commits must happen by a calling method
        or user interaction. If you don't commit the changes you won't see
        them in the database.

        args
        conn -- a psycopg2 connection object. if none, procures new one
        subs -- mappings of column names from the config file
        table_filter -- filter to limit rows that should be updated
        dry -- file path to save sql statements to instead of running them in
            the DB (if simply True print them to stdout)
        """
        print("Calculating stress on shared streets")

        # filter
        if table_filter is None:
            table_filter = sql.SQL("TRUE")

        subs["filter"] = table_filter

        # execute the query
        self._run_sql_script("shared.sql",subs,dirs=["sql","stress","segment"],conn=conn,dry=dry)


    def _segment_stress_bike_lane(self,conn,subs,table_filter=None,dry=None):
        """
        Calculates segment stress for bike lanes
        (includes buffered lanes)

        * NB: this method does not commit any transactions on the connection
        object passed to it. Commits must happen by a calling method
        or user interaction. If you don't commit the changes you won't see
        them in the database.

        args
        conn -- a psycopg2 connection object. if none, procures new one
        subs -- mappings of column names from the config file
        table_filter -- filter to limit rows that should be updated
        dry -- file path to save sql statements to instead of running them in
            the DB (if simply True print them to stdout)
        """
        print("Calculating stress on streets with bike lanes")

        # filter
        if table_filter is None:
            table_filter = sql.SQL("TRUE")

        subs["filter"] = table_filter

        # execute the query
        self._run_sql_script("bike_lane.sql",subs,dirs=["sql","stress","segment"],conn=conn,dry=dry)


    def _segment_stress_track(self,conn,subs,table_filter=None,dry=None):
        """
        Calculates segment stress for cycle tracks

        * NB: this method does not commit any transactions on the connection
        object passed to it. Commits must happen by a calling method
        or user interaction. If you don't commit the changes you won't see
        them in the database.

        args
        conn -- a psycopg2 connection object. if none, procures new one
        subs -- mappings of column names from the config file
        table_filter -- filter to limit rows that should be updated
        dry -- file path to save sql statements to instead of running them in
            the DB (if simply True print them to stdout)
        """
        print("Calculating stress on streets with cycle tracks")

        # filter
        if table_filter is None:
            table_filter = sql.SQL("TRUE")

        subs["filter"] = table_filter

        # execute the query
        self._run_sql_script("track.sql",subs,dirs=["sql","stress","segment"],conn=conn,dry=dry)


    def _segment_stress_path(self,conn,subs,table_filter=None,dry=None):
        """
        Calculates segment stress for cycle tracks

        * NB: this method does not commit any transactions on the connection
        object passed to it. Commits must happen by a calling method
        or user interaction. If you don't commit the changes you won't see
        them in the database.

        args
        subs -- mappings of column names from the config file
        table_filter -- filter to limit rows that should be updated
        dry -- file path to save sql statements to instead of running them in
            the DB (if simply True print them to stdout)
        """
        print("Calculating stress on paths")

        # filter
        if table_filter is None:
            table_filter = sql.SQL("TRUE")

        subs["filter"] = table_filter

        # execute the query
        self._run_sql_script("path.sql",subs,dirs=["sql","stress","segment"],conn=conn,dry=dry)


    def crossing_stress(self,table,angle=20,table_filter=None,dry=None):
        """
        Calculates stress for crossings

        args
        table -- the table root (optionally schema-qualified) to use for outputting
            LTS scores. Final table name will have forward/backward appended to
            indicate the direction the score applies to.
        angle -- the angle that determines whether a connection from
                    one road to another constitutes a crossing
        table_filter -- filter to limit rows that should be updated
        dry -- file path to save sql statements to instead of running them in
            the DB (if simply True print them to stdout)
        """
        schema, table = self.parse_table_name(table)
        if schema is None:
            schema = self.schema

        if self.debug:
            dry = True

        conn = self.get_db_connection()
        try:
            for direction in [FORWARD_DIRECTION,BACKWARD_DIRECTION]:
                print("  ....{}".format(direction))

                cross_subs = self.crossing_subs[direction].copy()
                cross_subs["out_schema"] = sql.Identifier(schema)
                cross_subs["out_table"] = sql.Identifier("_".join([table,direction]))
                cross_subs["angle"] = sql.Literal(angle)
                cross_subs["point"] = sql.Identifier("_".join([direction,"pt"]))
                cross_subs["line"] = sql.Identifier("_".join([direction,"ln"]))

                # execute the query
                self._run_sql_script("create_output.sql",cross_subs,dirs=["sql","stress","crossing"],conn=conn,dry=dry)
                self._run_sql_script("crossing.sql",cross_subs,dirs=["sql","stress","crossing"],conn=conn,dry=dry)

                # copy back to the base table
                cross_subs["stress"] = sql.Identifier(self.config.bna.network.roads.stress.crossing[direction])
                self._run_sql_script("copy_to_base.sql",cross_subs,dirs=["sql","stress"],conn=conn,dry=dry)
        except Exception as e:
            if conn.closed == 0:
                conn.rollback()
                conn.close()
            raise e

        conn.commit()
        conn.close()
