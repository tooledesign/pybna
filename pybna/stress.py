#! /usr/bin/python

# Module for conducting level of traffic stress analysis on roadway datasets.
# Allows for flexible naming of columns via a config file.
import os, StringIO
import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor
import yaml
from beautifultable import BeautifulTable
from tooles import DBUtils
import pandas as pd

FORWARD_DIRECTION = "forward"
BACKWARD_DIRECTION = "backward"

class Stress(DBUtils):
    def __init__(self, config, create_lookups=True,
                 verbose=False,debug=False):
        """
        Reads the config file, sets up a connection

        args
        config -- a YAML file holding config options, default
        create_lookups -- creates lookup tables in the db if none are found
        verbose -- output useful messages
        debug -- generates debug outputs
        """
        self.verbose = verbose
        self.debug = debug
        if self.verbose:
            print("Loading configuration")
        path = os.path.dirname(os.path.abspath(__file__)) # can't use dbutils yet
        self.config = yaml.safe_load(open(os.path.join(path,config)))

        # set up db connection
        host = self.config["db"]["host"]
        db = self.config["db"]["dbname"]
        user = self.config['db']["user"]
        password = self.config['db']["password"]
        DBUtils.__init__(self, host=host, db=db, user=user, password=password,
                         verbose=verbose, debug=debug, filename=__file__)

        schema, table = self.parse_table_name(self.config["stress"]["table"]["name"])
        self.table = table
        if schema is None:
            self.schema = self._get_schema(self.table)
        else:
            self.schema = schema
        self.geom = self.config["stress"]["table"]["geom"]
        self.srid = self._get_srid(self.table,self.geom,self.schema)

        # check for and set lookup tables
        if self.verbose:
            print("Checking lookup tables")
        missing = self._missing_lookup_tables()
        if create_lookups and len(missing) > 0:
            for t in missing:
                self._create_lookup_table(*t)

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
        for k, v in self.config["stress"]["lookup_tables"].iteritems():
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
                in_file = os.path.join(self.module_dir(),"stress_shared.csv")
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
                in_file = os.path.join(self.module_dir(),"stress_bike_lane.csv")
        elif lu_type == "crossing":
            columns = (
                ("control", "text"),
                ("lanes", "integer"),
                ("speed", "integer"),
                ("island", "boolean"),
                ("stress", "integer")
            )
            if not in_file:
                in_file = os.path.join(self.module_dir(),"stress_crossing.csv")
        else:
            raise ValueError("Unrecognized lookup table %s" % lu_type)


        conn = self._get_connection()
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
        except Exception, e:
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


    def _build_segment_sql_substitutions(self,direction):
        """
        Builds commonly-shared segment-oriented SQL substitutions from the
        entries in the config file

        args:
        direction -- the direction to generate substitutions for

        returns:
        a dictionary holding SQL objects
        """
        assumptions = self.config["stress"]["assumptions"]["segment"]
        settings = self.config["stress"]["segment"][direction]

        # check required inputs
        if "lanes" not in settings and "lanes" not in assumptions:
            raise ValueError("Lane data is required as either an attribute or an assumption")
        if "speed" not in settings and "speed" not in assumptions:
            raise ValueError("Speed data is required as either an attribute or an assumption")
        if "aadt" not in settings and "aadt" not in assumptions:
            raise ValueError("AADT data is required as either an attribute or an assumption")

        # lanes
        if "lanes" in settings:
            lanes = sql.Identifier(settings["lanes"])
        else:
            lanes = sql.SQL("NULL")
        if "lanes" in assumptions:
            assumed_lanes = self._build_case(assumptions["lanes"])
        else:
            assumed_lanes = sql.SQL("NULL")

        # centerline
        if "centerline" in settings:
            centerline_column = sql.Identifier(settings["centerline"]["name"])
            centerline_value = sql.Literal(settings["centerline"]["val"])
        else:
            centerline_column = sql.SQL("NULL")
            centerline_value = sql.SQL("NULL")
        centerline = sql.SQL("({}={})").format(centerline_column,centerline_value)
        if "centerline" in assumptions:
            assumed_centerline = self._build_case(assumptions["centerline"])
        else:
            assumed_centerline = sql.SQL("FALSE")

        # speed
        if "speed" in settings:
            speed = sql.Identifier(settings["speed"])
        else:
            speed = sql.SQL("NULL")
        if "speed" in assumptions:
            assumed_speed = self._build_case(assumptions["speed"])
        else:
            assumed_speed = sql.SQL("NULL")

        # oneway
        if "oneway" in settings:
            oneway_column = sql.Identifier(settings["oneway"]["name"])
            oneway_value = sql.Literal(settings["oneway"]["val"])
            all_oneways = list()
            for d in [FORWARD_DIRECTION,BACKWARD_DIRECTION]:
                s = self.config["stress"]["segment"][d]
                all_oneways.append(sql.Literal(s["oneway"]["val"]))
            all_oneway_values = sql.SQL(",").join(all_oneways)
        else:
            oneway_column = sql.Literal(0)
            oneway_value = sql.Literal(1)
            all_oneway_values = sql.Literal(1)
        twoway = sql.SQL("({} IS NULL OR {} NOT IN ({}))").format(
            oneway_column,
            oneway_column,
            all_oneway_values
        )
        oneway = sql.SQL("({}={})").format(oneway_column,oneway_value)

        # aadt
        if "aadt" in settings:
            aadt = sql.Identifier(settings["aadt"])
        else:
            aadt = sql.SQL("NULL")
        if "aadt" in assumptions:
            assumed_aadt = self._build_case(assumptions["aadt"])
        else:
            assumed_aadt = sql.SQL("NULL")

        # parking
        if "parking" in settings:
            parking_column = sql.Identifier(settings["parking"]["name"])
            parking_value = sql.Literal(settings["parking"]["val"])
        else:
            parking_column = sql.Literal(0)
            parking_value = sql.Literal(1)
        parking = sql.SQL("({}={})").format(parking_column,parking_value)
        if "parking" in assumptions:
            assumed_parking = self._build_case(assumptions["parking"])
        else:
            assumed_parking = sql.SQL("NULL")

        # parking_width
        if "parking_width" in settings:
            parking_width = sql.Identifier(settings["parking_width"])
        else:
            parking_width = sql.SQL("NULL")
        if "parking_width" in assumptions:
            assumed_parking_width = self._build_case(assumptions["parking_width"])
        else:
            assumed_parking_width = sql.SQL("NULL")

        # bike_lane_width
        if "bike_lane_width" in settings:
            bike_lane_width = sql.Identifier(settings["bike_lane_width"])
        else:
            bike_lane_width = sql.SQL("NULL")
        if "bike_lane_width" in assumptions:
            assumed_bike_lane_width = self._build_case(assumptions["bike_lane_width"])
        else:
            assumed_bike_lane_width = sql.SQL("NULL")

        # shared
        shared = sql.SQL("{c} IS NULL OR {c} NOT IN ({l},{bl},{t},{p})").format(
            c=sql.Identifier(settings["bike_infra"]["name"]),
            l=sql.Literal(settings["bike_infra"]["lane"]),
            bl=sql.Literal(settings["bike_infra"]["buffered_lane"]),
            t=sql.Literal(settings["bike_infra"]["track"]),
            p=sql.Literal(settings["bike_infra"]["path"])
        )

        # bike_lane
        bike_lane = sql.SQL("({} IN ({},{}))").format(
            sql.Identifier(settings["bike_infra"]["name"]),
            sql.Literal(settings["bike_infra"]["lane"]),
            sql.Literal(settings["bike_infra"]["buffered_lane"])
        )

        # track
        track = sql.SQL("({}={})").format(
            sql.Identifier(settings["bike_infra"]["name"]),
            sql.Literal(settings["bike_infra"]["track"])
        )

        # path
        path = sql.SQL("({}={})").format(
            sql.Identifier(settings["bike_infra"]["name"]),
            sql.Literal(settings["bike_infra"]["path"])
        )

        # other vals
        schema, table = self.parse_table_name(self.config["stress"]["table"]["name"])
        if schema is None:
            schema = self._get_schema(table)
        if "id" in self.config["stress"]["table"]:
            id_column = self.config["stress"]["table"]["id"]
        else:
            id_column = self._get_pkid_col(table,schema)
        if "geom" in self.config["stress"]["table"]:
            geom = self.config["stress"]["table"]["geom"]
        else:
            geom = self._get_geom_column(table,schema)
        shared_lts_schema, shared_lts_table = self.parse_table_name(self.config["stress"]["lookup_tables"]["shared"])
        if shared_lts_schema is None:
            shared_lts_schema = self._get_schema(shared_lts_table)
        bike_lane_lts_schema, bike_lane_lts_table = self.parse_table_name(self.config["stress"]["lookup_tables"]["bike_lane"])
        if bike_lane_lts_schema is None:
            bike_lane_lts_schema = self._get_schema(bike_lane_lts_table)

        # set up substitutions
        subs = {
            "id_column": sql.Identifier(id_column),
            "lanes": lanes,
            "assumed_lanes": assumed_lanes,
            "centerline": centerline,
            "assumed_centerline": assumed_centerline,
            "speed": speed,
            "assumed_speed": assumed_speed,
            "aadt": aadt,
            "assumed_aadt": assumed_aadt,
            "parking": parking,
            "assumed_parking": assumed_parking,
            "parking_width": parking_width,
            "assumed_parking_width": assumed_parking_width,
            "bike_lane_width": bike_lane_width,
            "assumed_bike_lane_width": assumed_bike_lane_width,
            "in_schema": sql.Identifier(schema),
            "in_table": sql.Identifier(table),
            "geom": sql.Identifier(geom),
            "oneway": oneway,
            "twoway": twoway,
            "shared": shared,
            "bike_lane": bike_lane,
            "track": track,
            "path": path,
            "shared_lts_schema": sql.Identifier(shared_lts_schema),
            "shared_lts_table": sql.Identifier(shared_lts_table),
            "bike_lane_lts_schema": sql.Identifier(bike_lane_lts_schema),
            "bike_lane_lts_table": sql.Identifier(bike_lane_lts_table)
        }

        return subs


    def _build_crossing_sql_substitutions(self,direction):
        """
        Builds crossing SQL substitutions from the entries in the config
        file

        args:
        direction -- the direction to generate substitutions for

        returns:
        a dictionary holding SQL objects
        """
        assumptions = self.config["stress"]["assumptions"]["crossing"]
        if self.config["stress"]["crossing"][direction] is None:
            settings = dict()
        else:
            settings = self.config["stress"]["crossing"][direction]

        # check required inputs
        if "intersection_tolerance" not in self.config["stress"]["crossing"]:
            raise ValueError("Intersection tolerance not specified in config")
        if "control" not in self.config["stress"]["crossing"]:
            raise ValueError("Control data not specified in config")

        intersection_tolerance = self.config["stress"]["crossing"]["intersection_tolerance"]

        # stress table
        schema, table = self.parse_table_name(self.config["stress"]["table"]["name"])
        if schema is None:
            schema = self._get_schema(table)
        if "id" in self.config["stress"]["table"]:
            id_column = self.config["stress"]["table"]["id"]
        else:
            id_column = self._get_pkid_col(table,schema)
        if "geom" in self.config["stress"]["table"]:
            geom = self.config["stress"]["table"]["geom"]
        else:
            geom = self._get_geom_column(table,schema)

        # control
        control_schema, control_table = self.parse_table_name(self.config["stress"]["crossing"]["control"]["table"])
        if control_schema is None:
            control_schema = self._get_schema(control_table)
        if "geom" in self.config["stress"]["crossing"]["control"]:
            control_geom = self.config["stress"]["crossing"]["control"]["geom"]
        else:
            control_geom = self._get_geom_column(control_table,control_schema)
        control_column = self.config["stress"]["crossing"]["control"]["column"]["name"]
        four_way_stop = self.config["stress"]["crossing"]["control"]["column"]["four_way_stop"]
        signal = self.config["stress"]["crossing"]["control"]["column"]["signal"]
        rrfb = self.config["stress"]["crossing"]["control"]["column"]["rrfb"]
        hawk = self.config["stress"]["crossing"]["control"]["column"]["hawk"]

        # island
        island_schema, island_table = self.parse_table_name(self.config["stress"]["crossing"]["island"]["table"])
        if island_schema is None:
            island_schema = self._get_schema(island_table)
        if "geom" in self.config["stress"]["crossing"]["island"]:
            island_geom = self.config["stress"]["crossing"]["island"]["geom"]
        else:
            island_geom = self._get_geom_column(island_table,island_schema)
        island_column = self.config["stress"]["crossing"]["island"]["column"]["name"]

        # directional_attribute_aggregation
        f = open(os.path.join(self.module_dir(),"crossing","directional_attributes.sql"))
        data_insert = f.read()
        f.close()
        f = open(os.path.join(self.module_dir(),"crossing","directional_attributes_table.sql"))
        directional_attributes = f.read()
        f.close()
        data_insert_query = sql.SQL("")
        for d in [FORWARD_DIRECTION,BACKWARD_DIRECTION]:
            data_insert_query += sql.SQL(data_insert).format(**self.segment_subs[d])
        data_insert_subs = self.segment_subs["forward"].copy()
        data_insert_subs["data_insert"] = data_insert_query
        directional_attribute_aggregation = sql.SQL(directional_attributes).format(**data_insert_subs)

        #
        # grab direct settings (if any are specified)
        #

        # lanes
        if "lanes" in settings:
            cross_lanes = sql.SQL("actual.") + sql.Identifier(settings["lanes"])
        else:
            cross_lanes = sql.SQL("NULL")

        # speed
        if "speed" in settings:
            cross_speed = sql.SQL("actual.") + sql.Identifier(settings["speed"])
        else:
            cross_speed = sql.SQL("NULL")

        # control
        if "control" in settings:
            cross_control = sql.SQL("actual.") + sql.Identifier(settings["control"])
        else:
            cross_control = sql.SQL("NULL")

        # island
        if "island" in settings:
            cross_island = sql.SQL("actual.") + sql.Identifier(settings["island"])
        else:
            cross_island = sql.SQL("NULL")

        # misc
        cross_lts_schema, cross_lts_table = self.parse_table_name(self.config["stress"]["lookup_tables"]["crossing"])
        if cross_lts_schema is None:
            cross_lts_schema = self._get_schema(cross_lts_table)

        subs = {
            "directional_attribute_aggregation": directional_attribute_aggregation,
            "intersection_tolerance": sql.Literal(intersection_tolerance),
            "point": sql.Identifier("_".join([direction,"pt"])),
            "in_schema": sql.Identifier(schema),
            "in_table": sql.Identifier(table),
            "geom": sql.Identifier(geom),
            "id_column": sql.Identifier(id_column),
            "control_schema": sql.Identifier(control_schema),
            "control_table": sql.Identifier(control_table),
            "control_geom": sql.Identifier(control_geom),
            "control_column": sql.Identifier(control_column),
            "four_way_stop": sql.Literal(four_way_stop),
            "signal": sql.Literal(signal),
            "rrfb": sql.Literal(rrfb),
            "hawk": sql.Literal(hawk),
            "island_schema": sql.Identifier(island_schema),
            "island_table": sql.Identifier(island_table),
            "island_geom": sql.Identifier(island_geom),
            "island_column": sql.Identifier(island_column),
            "cross_lanes": cross_lanes,
            "cross_speed": cross_speed,
            "cross_control": cross_control,
            "cross_island": cross_island,
            "cross_lts_schema": sql.Identifier(cross_lts_schema),
            "cross_lts_table": sql.Identifier(cross_lts_table)
        }

        # control_assignment
        f = open(os.path.join(self.module_dir(),"crossing","control_assignment.sql"))
        raw = f.read()
        f.close()
        control_assignment = sql.SQL(raw).format(**subs)
        subs["control_assignment"] = control_assignment

        # island_assignment
        f = open(os.path.join(self.module_dir(),"crossing","island_assignment.sql"))
        raw = f.read()
        f.close()
        island_assignment = sql.SQL(raw).format(**subs)
        subs["island_assignment"] = island_assignment

        # priority_assignment
        f = open(os.path.join(self.module_dir(),"crossing","priority_assignment.sql"))
        raw = f.read()
        f.close()
        priority_assignment = sql.SQL("")
        if "priority" in assumptions:
            for i, w in enumerate(assumptions["priority"]):
                s = subs.copy()
                this_priority_table = "tmp_this_priority_" + str(i)
                this_where_test = w["where"]
                if this_where_test == "*":
                    this_where_test = "TRUE"
                that_priority_table = "tmp_that_priority_" + str(i)
                that_where_test = w["meets"]
                if that_where_test == "*":
                    that_where_test = "TRUE"
                priority_table = "tmp_priority_" + str(i)

                s["this_priority_table"] = sql.Identifier(this_priority_table)
                s["this_where_test"] = sql.SQL(this_where_test)
                s["that_priority_table"] = sql.Identifier(that_priority_table)
                s["that_where_test"] = sql.SQL(that_where_test)
                s["priority_table"] = sql.Identifier(priority_table)

                priority_assignment += sql.SQL(raw).format(**s)

        subs["priority_assignment"] = priority_assignment

        return subs


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
            the DB (if simply True print them to stdout)
        """
        schema, table = self.parse_table_name(table)
        if schema is None:
            schema = self.schema

        if table_filter:
            table_filter = sql.SQL(table_filter)
        else:
            table_filter = sql.SQL("TRUE")

        if self.debug:
            dry = True

        conn = self._get_connection()
        try:
            for direction in [FORWARD_DIRECTION,BACKWARD_DIRECTION]:
                if self.verbose:
                    print("  ....{}".format(direction))

                # create output table
                subs = self.segment_subs[direction].copy()
                subs["out_schema"] = sql.Identifier(schema)
                subs["out_table"] = sql.Identifier("_".join([table,direction]))
                self._run_sql_script("create_output.sql",subs,dir_name="segment",conn=conn,dry=dry)

                # call the various segment stress methods
                self._segment_stress_shared(conn,subs,table_filter,dry=dry)
                self._segment_stress_bike_lane(conn,subs,table_filter,dry=dry)
                self._segment_stress_track(conn,subs,table_filter,dry=dry)
                self._segment_stress_path(conn,subs,table_filter,dry=dry)
        except Exception as e:
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
        self._run_sql_script("shared.sql",subs,dir_name="segment",conn=conn,dry=dry)


    def _segment_stress_bike_lane(self,conn,subs,table_filter=None,dry=False):
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
        self._run_sql_script("bike_lane.sql",subs,dir_name="segment",conn=conn,dry=dry)


    def _segment_stress_track(self,conn,subs,table_filter=None,dry=False):
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
        self._run_sql_script("track.sql",subs,dir_name="segment",conn=conn,dry=dry)


    def _segment_stress_path(self,conn,subs,table_filter=None,dry=False):
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
        self._run_sql_script("path.sql",subs,dir_name="segment",conn=conn,dry=dry)


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

        conn = self._get_connection()
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
                self._run_sql_script("create_output.sql",cross_subs,dir_name="crossing",conn=conn,dry=dry)
                self._run_sql_script("crossing.sql",cross_subs,dir_name="crossing",conn=conn,dry=dry)
        except Exception as e:
            conn.rollback()
            conn.close()
            raise e

        conn.commit()
        conn.close()


    def _build_case(self,vals,prefix=None):
        if prefix is None:
            prefix = sql.SQL("")
        else:
            prefix = sql.SQL(prefix + ".")
        case = sql.SQL(" CASE ")
        for val in vals:
            if "else" in val:
                pass
            elif "where" in val:
                if val["where"] == "*":
                    case += sql.SQL(" WHEN TRUE THEN ") + sql.Literal(val["val"])
                else:
                    case += sql.SQL(" WHEN ") + prefix + sql.SQL(val["where"]) + sql.SQL(" THEN ") + sql.Literal(val["val"])
            else:
                raise
        if "else" in vals[-1]:
            case += sql.SQL(" ELSE ") + sql.Literal(vals[-1]["else"])
        case += sql.SQL(" END ")
        return case


    def _build_priority(self,priority):
        case = ""
        for entry in priority:
            case += " WHEN this_way." + entry["where"]
            if entry["meets"] != "*":
                case += " AND crossing_way." + entry["meets"]
            case += " THEN 1 "
        return sql.SQL(case)


    def _build_control(self,control):
        case = sql.SQL(" CASE ")
        for entry in control:
            case += sql.SQL(" WHEN this_way." + entry["where"])
            if entry["meets"] != "*":
                case += sql.SQL(" AND crossing_way." + entry["meets"])
            case += sql.SQL(" THEN {}").format(
                sql.Literal(entry["control"])
            )
        case += sql.SQL(" ELSE NULL END ")
        return case


    def _wrap_coalesce(self,q):
        pass


    def make_carto_stress_table(self,out_table,max_int_length,
                                segment_forward,segment_backward,
                                cross_forward,cross_backward,
                                in_table=None,directional=False,
                                overwrite=False,dry=False):
        """
        Creates a table with cartographic representations of the segment and
        crossing stress.

        args
        out_table -- the table (optionally schema-qualified) to use for outputting the new layer.
        max_int_length -- the length to make "crossing" segments
        segment_forward -- if in_table is given, this is the name of the column
            in the table holding forward segment stress scores. if in_table is not
            given, this is the name of the table holding forward segment stress scores
        segment_backward -- if in_table is given, this is the name of the column
            in the table holding backward segment stress scores. if in_table is not
            given, this is the name of the table holding backward segment stress scores
        cross_forward -- if in_table is given, this is the name of the column
            in the table holding forward crossing stress scores. if in_table is not
            given, this is the name of the table holding forward crossing stress scores
        cross_backward -- if in_table is given, this is the name of the column
            in the table holding backward crossing stress scores. if in_table is not
            given, this is the name of the table holding backward crossing stress scores
        in_table -- the table to use for reading in LTS scores. If not given, the
            method assumes scores are stored in separate tables.
        directional -- whether to create separate lines for each direction of travel.
            Because separate stress tables aren't guaranteed to have information about
            one-way travel, this option is incompatible with multiple tables.
        overwrite -- overwrite an existing table
        dry -- output SQL commands to terminal instead of running in the database
        """
        if directional and in_table is None:
            raise ValueError("in_table is required for directional outputs")

        out_schema, out_table = self.parse_table_name(out_table)
        if out_schema is None:
            out_schema = self.schema

        # check if table exists
        if not overwrite:
            if self.table_exists(out_table, schema=out_schema):
                raise ValueError("Table {}.{} already exists".format(out_schema,out_table))

        # set up base sql subs
        subs = {
            "out_schema": sql.Identifier(out_schema),
            "out_table": sql.Identifier(out_table),
            "id": sql.Identifier(self._get_pkid_col(self.table)),
            "geom": sql.Identifier(self.geom),
            "srid": sql.Literal(self.srid),
            "segmentation": sql.Literal(max_int_length),
            "idx_name": sql.Identifier("sidx_"+out_table)
        }

        if in_table is None:
            self._make_carto_stress_table_from_multiple(
                subs,
                segment_forward,
                segment_backward,
                cross_forward,
                cross_backward,
                directional,
                dry
            )
        else:
            self._make_carto_stress_table_from_single(
                subs,
                in_table,
                segment_forward,
                segment_backward,
                cross_forward,
                cross_backward,
                directional,
                dry
            )


    def _make_carto_stress_table_from_single(self,subs,in_table,
                                             segment_forward,segment_backward,
                                             cross_forward,cross_backward,
                                             directional,dry,conn=None):
        """
        Creates a table with cartographic representations of the segment and
        crossing stress with a single table as input.
        """
        in_schema, in_table = self.parse_table_name(in_table)
        if in_schema is None:
            in_schema = self._get_schema(in_table)

        subs["in_schema"] = sql.Identifier(in_schema)
        subs["in_table"] = sql.Identifier(in_table)
        subs["segment_forward"] = sql.Identifier(segment_forward)
        subs["segment_backward"] = sql.Identifier(segment_backward)
        subs["cross_forward"] = sql.Identifier(cross_forward)
        subs["cross_backward"] = sql.Identifier(cross_backward)

        if directional:
            close_conn = False
            if conn is None:
                conn = self._get_connection()
                close_conn = True
            self._run_sql_script("01_create_table.sql",subs,dir_name=["make_carto_layer","single","directional"],dry=dry,conn=conn)
            subs["forward_oneway"] = sql.SQL("{c}={v}").format(
                c=sql.Identifier(self.config["stress"]["segment"]["forward"]["oneway"]["name"]),
                v=sql.Literal(self.config["stress"]["segment"]["forward"]["oneway"]["val"]),
            )
            subs["backward_oneway"] = sql.SQL("{c}={v}").format(
                c=sql.Identifier(self.config["stress"]["segment"]["backward"]["oneway"]["name"]),
                v=sql.Literal(self.config["stress"]["segment"]["backward"]["oneway"]["val"]),
            )
            subs["twoway"] = sql.SQL("{c}=NULL").format(
                c=sql.Identifier(self.config["stress"]["segment"]["forward"]["oneway"]["name"])
            )
            self._run_sql_script("02_insert.sql",subs,dir_name=["make_carto_layer","single","directional"],dry=dry,conn=conn)
            self._run_sql_script("03_cleanup.sql",subs,dir_name=["make_carto_layer","single","directional"],dry=dry,conn=conn)
            if close_conn:
                conn.commit()
                conn.close()
        else:
            self._run_sql_script("combined.sql",subs,dir_name=["make_carto_layer","single"],dry=dry,conn=conn)


    def _make_carto_stress_table_from_multiple(self,subs,
                                               segment_forward,segment_backward,
                                               cross_forward,cross_backward,
                                               directional,dry):
        # get schemas
        sf_schema, sf_table = self.parse_table_name(segment_forward)
        if sf_schema is None:
            sf_schema = self._get_schema(sf_table)
        sb_schema, sb_table = self.parse_table_name(segment_backward)
        if sb_schema is None:
            sb_schema = self._get_schema(sb_table)
        cf_schema, cf_table = self.parse_table_name(cross_forward)
        if cf_schema is None:
            cf_schema = self._get_schema(cf_table)
        cb_schema, cb_table = self.parse_table_name(cross_backward)
        if cb_schema is None:
            cb_schema = self._get_schema(cb_table)

        # build subs
        subs["sf_schema"] = sql.Identifier(sf_schema)
        subs["sf_table"] = sql.Identifier(sf_table)
        subs["sb_schema"] = sql.Identifier(sb_schema)
        subs["sb_table"] = sql.Identifier(sb_table)
        subs["cf_schema"] = sql.Identifier(cf_schema)
        subs["cf_table"] = sql.Identifier(cf_table)
        subs["cb_schema"] = sql.Identifier(cb_schema)
        subs["cb_table"] = sql.Identifier(cb_table)

        conn = self._get_connection()
        self._run_sql_script("make_single.sql",subs,dir_name=["make_carto_layer","multiple"],dry=dry,conn=conn)
        self._make_carto_stress_table_from_single(
            subs,
            in_table="pg_temp.tmp_carto",
            segment_forward="stress_segment_forward",
            segment_backward="stress_segment_backward",
            cross_forward="stress_cross_forward",
            cross_backward="stress_cross_backward",
            directional=directional,
            conn=conn,
            dry=dry
        )
        conn.commit()
        conn.close()


    def export_stress_tables(self,path,stress_type="all",overwrite=False):
        """
        Exports the stress tables in human-readable format

        args:
        path -- Location to save the output
        stress_type -- One of: all, shared, bike_lane, crossing
        overwrite -- whether to overwrite an existing excel file
        """
        # check inputs
        if stress_type not in ["all","shared","bike_lane","crossing"]:
            raise ValueError("Invalid stress_type. Must be one of [all, shared, bike_lane, crossing]")
        if (not os.path.splitext(path)[1] == ".xlsx"):
            raise ValueError("File must be of type xlsx")
        if os.path.isfile(path) and not overwrite:
            raise ValueError("File {} already exists".format(path))


        shared = self._export_shared()
        bike = self._export_bike()
        crossing = self._export_crossing()

        with pd.ExcelWriter(path) as writer:
            shared.to_excel(writer, sheet_name="Shared")
            bike.to_excel(writer, sheet_name="Bike Lane")
            crossing.to_excel(writer, sheet_name="Crossing")


    def _export_shared(self):
        """
        Returns a pivot table dataframe of the shared LTS table
        """
        conn = self._get_connection()
        q = sql.SQL("select * from {}").format(sql.Identifier(self.config["stress"]["lookup_tables"]["shared"]))
        df = pd.read_sql(q.as_string(conn),conn,index_col="id")
        df = df.rename(columns={"lanes":"Lanes","effective_aadt":"Effective AADT","speed":"Prevailing speed"})
        return pd.pivot_table(df,values="stress",index=["Lanes","Effective AADT"],columns=["Prevailing speed"])


    def _export_bike(self):
        """
        Returns a pivot table dataframe of the bike lane LTS table
        """
        conn = self._get_connection()
        q = sql.SQL("select * from {}").format(sql.Identifier(self.config["stress"]["lookup_tables"]["bike_lane"]))
        df = pd.read_sql(q.as_string(conn),conn,index_col="id")
        df.oneway = df.oneway.fillna(False)
        mapping = {
            True: "Yes",
            False: "No"
        }
        df.oneway = df.oneway.map(mapping)
        df.parking = df.parking.map(mapping)
        df = df.rename(columns={"lanes":"Lanes","oneway":"One-way street","parking":"On-street parking","speed":"Prevailing speed","reach":"Bike lane width or reach"})
        return pd.pivot_table(df,values="stress",index=["On-street parking","Lanes","Bike lane width or reach"],columns=["Prevailing speed"])


    def _export_crossing(self):
        """
        Returns a pivot table dataframe of the crossing LTS table
        """
        conn = self._get_connection()
        q = sql.SQL("select * from {}").format(sql.Identifier(self.config["stress"]["lookup_tables"]["crossing"]))
        df = pd.read_sql(q.as_string(conn),conn,index_col="id")
        df.control = df.control.fillna("Uncontrolled")
        mapping = {
            "Uncontrolled":"Uncontrolled",
            "four way stop":"4 way stop",
            "rrfb":"RRFB",
            "hawk":"HAWK",
            "signal":"Signal"
        }
        df.control = df.control.map(mapping)
        df.control = pd.Categorical(df.control,["Uncontrolled","4 way stop","RRFB","HAWK","Signal"])
        mapping = {
            True: "Yes",
            False: "No"
        }
        df.island = df.island.map(mapping)
        df = df.rename(columns={"control":"Control","lanes":"Lanes","speed":"Prevailing speed","island":"Island"})
        return pd.pivot_table(df,values="stress",index=["Control","Island","Lanes"],columns=["Prevailing speed"])
