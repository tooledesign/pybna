#! /usr/bin/python

# Module for conducting level of traffic stress analysis on roadway datasets.
# Allows for flexible naming of columns via a config file.
import os, io
import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor
import yaml
import pandas as pd

from .conf import Conf
from .dbutils import DBUtils
from .core import FORWARD_DIRECTION
from .core import BACKWARD_DIRECTION


class Stress(Conf):
    def __init__(self,config=None,create_lookups=True,host=None,db_name=None,
                 user=None,password=None,verbose=False):
        """
        Reads the config file, sets up a connection

        Parameters
        ----------
        config : str, optional
            path to the config file, if not given use the default config.yaml
        create_lookups : bool, optional
            creates lookup tables in the db if none are found
        host : str, optional
            host to connect to
        db_name : str, optional
            database name
        user : str, optional
            database user
        password : str, optional
            database password
        verbose : bool, optional
            output useful messages
        """
        Conf.__init__(self)
        self.verbose = verbose
        self.module_dir = os.path.dirname(os.path.abspath(__file__))
        if config is None:
            config = os.path.join(self.module_dir,"config.yaml")
        self.config = self.parse_config(yaml.safe_load(open(config)))
        print("Connecting to database")
        if host is None:
            host = self.config.db.host
        if db_name is None:
            db_name = self.config.db.dbname
        if user is None:
            user = self.config.db.user
        if password is None:
            password = self.config.db.password
        db_connection_string = " ".join([
            "dbname=" + db_name,
            "user=" + user,
            "host=" + host,
            "password=" + password
        ])
        DBUtils.__init__(self,db_connection_string,self.verbose,False)
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
            if "units" in self.config:
                if self.config.units == "mi":
                    km = False
                elif self.config.units == "km":
                    km = True
                else:
                    raise ValueError("Invalid units \"{}\" in config".format(self.config.units))
            else:
                km = False
            for t in missing:
                self._create_lookup_table(*t,km=km)

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
        for k, v in self.config.stress.lookup_tables.items():
            schema, table = self.parse_table_name(v)
            if not self.table_exists(table,schema):
                missing.append((k,table,schema))
                if self.verbose:
                    print("Table {} not identified, will create".format(v))
        return missing


    def _create_lookup_table(self,lu_type,table,schema=None,fname=None,km=False):
        """
        Create a stress lookup table of the given type and name

        Parameters
        ----------
        lu_type : ['shared', 'bike_lane', 'crossing']
            The type of lookup table to create
        table : str
            name of the table
        schema : str, optional
            name of the schema
        fname : str, optional
            optional csv file to populate the table with (if empty uses default)
        km : boolean, optional
            if true, use metric lookup tables instead of imperial
        """
        if schema is None:
            schema = self.schema
        in_file = None
        if fname:
            if os.path.isfile(fname):
                in_file = fname
            else:
                raise ValueError("File not found at %s" % fname)

        if lu_type == "shared":
            if km:
                columns = [
                    ("lanes", "integer"),
                    ("marked_centerline", "boolean"),
                    ("speed", "integer"),
                    ("width", "float"),
                    ("parking", "boolean"),
                    ("effective_aadt", "integer"),
                    ("stress", "integer")
                ]
            else:
                columns = [
                    ("lanes", "integer"),
                    ("marked_centerline", "boolean"),
                    ("speed", "integer"),
                    ("width", "integer"),
                    ("parking", "boolean"),
                    ("effective_aadt", "integer"),
                    ("stress", "integer")
                ]
            if not in_file:
                if km:
                    in_file = os.path.join(self.module_dir,"sql","stress","tables","stress_shared_km.xlsx")
                else:
                    in_file = os.path.join(self.module_dir,"sql","stress","tables","stress_shared.xlsx")
        elif lu_type == "bike_lane":
            if km:
                columns = (
                    ("lanes", "integer"),
                    ("oneway", "boolean"),
                    ("parking", "boolean"),
                    ("low_parking", "boolean"),
                    ("reach", "float"),
                    ("speed", "integer"),
                    ("stress", "integer")
                )
            else:
                columns = (
                    ("lanes", "integer"),
                    ("oneway", "boolean"),
                    ("parking", "boolean"),
                    ("low_parking", "boolean"),
                    ("reach", "integer"),
                    ("speed", "integer"),
                    ("stress", "integer")
                )
            if not in_file:
                if km:
                    in_file = os.path.join(self.module_dir,"sql","stress","tables","stress_bike_lane_km.xlsx")
                else:
                    in_file = os.path.join(self.module_dir,"sql","stress","tables","stress_bike_lane.xlsx")
        elif lu_type == "crossing":
            columns = (
                ("control", "text"),
                ("lanes", "integer"),
                ("speed", "integer"),
                ("island", "boolean"),
                ("stress", "integer")
            )
            if not in_file:
                if km:
                    in_file = os.path.join(self.module_dir,"sql","stress","tables","stress_crossing_km.xlsx")
                else:
                    in_file = os.path.join(self.module_dir,"sql","stress","tables","stress_crossing.xlsx")
        else:
            raise ValueError("Unrecognized lookup table %s" % lu_type)

        in_table = pd.read_excel(in_file)
        in_table = in_table.astype(str)
        conn = self.get_db_connection()
        self.gdf_to_postgis(in_table,table,schema=schema,no_geom=True,conn=conn)
        for col_name, col_type in columns:
            subs = {
                "schema": sql.Identifier(schema),
                "table": sql.Identifier(table),
                "col": sql.Identifier(col_name),
                "type": sql.SQL(col_type)
            }
            self._run_sql(
                """
                    update {schema}.{table} set {col} = null where {col} IN ('NaN','nan');
                    alter table {schema}.{table}
                        alter column {col} type {type} using {col}::{type};
                """,
                subs,
                conn=conn
            )
        conn.commit()
        conn.close()


    def segment_stress(self,table=None,table_filter=None,dry=None):
        """
        Creates a new table of LTS scores for each direction (forward/backward).
        The new table also includes the attributes (actual or assumed) that were
        used the calculate the LTS score.
        This is basically a pass-through function that calls several helpers to
        calculate the various parts

        Parameters
        ----------
        table : str, optional
            the table root (optionally schema-qualified) to use for outputting
            LTS scores. Final table name will have forward/backward appended to
            indicate the direction the score applies to. Defaults to "bna_stress_seg"
        table_filter : str, optional
            SQL filter to limit rows that should be updated
        dry : str, optional
            a path to save SQL statements to instead of executing in DB
        """
        if table is None:
            schema = self.schema
            table = "bna_stress_seg"
        else:
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
                self._run_sql_script("create_output.sql",subs,dirs=["sql","stress","segment"],conn=conn)

                # call the various segment stress methods
                self._segment_stress_shared(conn,subs,table_filter)
                self._segment_stress_bike_lane(conn,subs,table_filter)
                self._segment_stress_track(conn,subs,table_filter)
                self._segment_stress_path(conn,subs,table_filter)

                # copy back to the base table
                subs["stress"] = sql.Identifier(self.config.bna.network.roads.stress.segment[direction])
                self._run_sql_script("copy_to_base.sql",subs,dirs=["sql","stress"],conn=conn)

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

        Parameters
        ----------
        conn : psycopg2 connection object
            a psycopg2 connection object
        subs : dict
            mappings of column names from the config file
        table_filter : str, optional
            filter to limit rows that should be updated
        dry : str, optional
            a path to save SQL statements to instead of executing in DB
        """
        print("Calculating stress on shared streets")

        # filter
        if table_filter is None:
            table_filter = sql.SQL("TRUE")

        subs["filter"] = table_filter

        # execute the query
        self._run_sql_script("shared.sql",subs,dirs=["sql","stress","segment"],conn=conn)


    def _segment_stress_bike_lane(self,conn,subs,table_filter=None,dry=None):
        """
        Calculates segment stress for bike lanes
        (includes buffered lanes)

        * NB: this method does not commit any transactions on the connection
        object passed to it. Commits must happen by a calling method
        or user interaction. If you don't commit the changes you won't see
        them in the database.

        Parameters
        ----------
        conn : psycopg2 connection object
            a psycopg2 connection object
        subs : dict
            mappings of column names from the config file
        table_filter : str, optional
            filter to limit rows that should be updated
        dry : str, optional
            a path to save SQL statements to instead of executing in DB
        """
        print("Calculating stress on streets with bike lanes")

        # filter
        if table_filter is None:
            table_filter = sql.SQL("TRUE")

        subs["filter"] = table_filter

        # execute the query
        self._run_sql_script("bike_lane.sql",subs,dirs=["sql","stress","segment"],conn=conn)


    def _segment_stress_track(self,conn,subs,table_filter=None,dry=None):
        """
        Calculates segment stress for cycle tracks

        * NB: this method does not commit any transactions on the connection
        object passed to it. Commits must happen by a calling method
        or user interaction. If you don't commit the changes you won't see
        them in the database.

        Parameters
        ----------
        conn : psycopg2 connection object
            a psycopg2 connection object
        subs : dict
            mappings of column names from the config file
        table_filter : str, optional
            filter to limit rows that should be updated
        dry : str, optional
            a path to save SQL statements to instead of executing in DB
        """
        print("Calculating stress on streets with cycle tracks")

        # filter
        if table_filter is None:
            table_filter = sql.SQL("TRUE")

        subs["filter"] = table_filter

        # execute the query
        self._run_sql_script("track.sql",subs,dirs=["sql","stress","segment"],conn=conn)


    def _segment_stress_path(self,conn,subs,table_filter=None,dry=None):
        """
        Calculates segment stress for cycle tracks

        * NB: this method does not commit any transactions on the connection
        object passed to it. Commits must happen by a calling method
        or user interaction. If you don't commit the changes you won't see
        them in the database.

        Parameters
        ----------
        subs : dict
            mappings of column names from the config file
        table_filter : str, optional
            filter to limit rows that should be updated
        dry : str, optional
            a path to save SQL statements to instead of executing in DB
        """
        print("Calculating stress on paths")

        # filter
        if table_filter is None:
            table_filter = sql.SQL("TRUE")

        subs["filter"] = table_filter

        # execute the query
        self._run_sql_script("path.sql",subs,dirs=["sql","stress","segment"],conn=conn)


    def crossing_stress(self,table=None,angle=20,table_filter=None,dry=None):
        """
        Calculates stress for crossings

        Parameters
        ----------
        table : str, optional
            the table root (optionally schema-qualified) to use for outputting
            LTS scores. Final table name will have forward/backward appended to
            indicate the direction the score applies to. Defaults to "bna_stress_cross".
        angle : int or float
            the angle that determines whether a connection from
                    one road to another constitutes a crossing
        table_filter : str, optional
            filter to limit rows that should be updated
        dry : str, optional
            a path to save SQL statements to instead of executing in DB
        """
        if table is None:
            schema = self.schema
            table = "bna_stress_cross"
        else:
            schema, table = self.parse_table_name(table)
            if schema is None:
                schema = self.schema

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
                self._run_sql_script("create_output.sql",cross_subs,dirs=["sql","stress","crossing"],conn=conn)
                self._run_sql_script("inputs.sql",cross_subs,dirs=["sql","stress","crossing"],conn=conn)
                self._run_sql_script("crossing.sql",cross_subs,dirs=["sql","stress","crossing"],conn=conn)
                self._run_sql_script("priority.sql",cross_subs,dirs=["sql","stress","crossing"],conn=conn)

                # copy back to the base table
                cross_subs["stress"] = sql.Identifier(self.config.bna.network.roads.stress.crossing[direction])
                self._run_sql_script("copy_to_base.sql",cross_subs,dirs=["sql","stress"],conn=conn)
        except Exception as e:
            if conn.closed == 0:
                conn.rollback()
                conn.close()
            raise e

        conn.commit()
        conn.close()
