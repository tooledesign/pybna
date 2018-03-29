###################################################################
# pybna is a Python module that uses networkx to implement the
# connectivity logic developed in the BNA.
###################################################################
import os
import yaml
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import quote_ident
import pandas as pd
import geopandas as gpd
import pickle

from scenario import Scenario


class pyBNA:
    """Collection of BNA scenarios and attendant functions."""

    def __init__(self, config="default.config", host=None, db=None, user=None,
                 password=None, verbose=False):
        """Connects to the BNA database

        kwargs:
        config -- path to the config file
        host -- hostname or address
        db -- name of database on server
        user -- username to connect to database
        password -- password to connect to database
        verbose -- output useful messages

        return: pyBNA object
        """
        self.verbose = verbose
        self.config = yaml.safe_load(open(config))

        if self.verbose:
            print("\n \
            ---------------pyBNA---------------\n \
            Create and test BNA scenarios")

        # set up db connection
        if host is None:
            host = self.config["db"]["host"]
        if db is None:
            db = self.config["db"]["database"]
        if user is None:
            user = self.config["db"]["user"]
        if password is None:
            password = self.config["db"]["password"]
        db_connection_string = " ".join([
            "dbname=" + db,
            "user=" + user,
            "host=" + host,
            "password=" + password
        ])
        self.conn = psycopg2.connect(db_connection_string)

        # Create dictionaries to hold scenarios and destinations
        self.scenarios = dict()
        self._set_scenarios()

        # Set destinations from config file
        self.destinations = dict()
        self.destination_blocks = set()
        self._set_destinations()

        # blocks
        self.blocks = None
        self.blocks_table = None
        self.blocks_schema = None
        self.block_id_col = None
        p._set_blocks()

        # get srid
        try:
            self.srid = self.config["db"]["srid"]
        except KeyError:
            self.srid = self._get_srid(self.blocks_table)

        # Get tiles for running connectivity (if given)
        self.tiles = None
        if tiles_shp_path and tiles_table_name:
            raise ValueError("Cannot accept tile sources from both shapefile _and_ pg table")
        if tiles_shp_path:
            self.tiles = self._get_tiles_shp(tiles_shp_path)
        if tiles_table_name:
            self.tiles = self._get_tiles_pg(tiles_table_name,tiles_table_geom_col,tiles_columns)


    def _get_pkid_col(self, table):
        # connect to pg and read id col
        self._reestablish_conn()
        cur = self.conn.cursor()
        cur.execute(" \
        SELECT a.attname \
        FROM   pg_index i \
        JOIN   pg_attribute a ON a.attrelid = i.indrelid \
                AND a.attnum = ANY(i.indkey) \
        WHERE  i.indrelid = %(table)s::regclass \
        AND    i.indisprimary;", {"table": quote_ident(table, cur)})

        if cur.rowcount == 0:
            raise Error("No primary key defined on table %s" % table)

        row = cur.fetchone()
        if self.verbose:
            print("   ID: %s" % row[0])
        cur.close()
        return row[0]


    def _get_tiles_shp(self,path):
        return 1


    def _get_tiles_pg(self,tableName,geom_col,add_columns):
        pkid = self._get_pkid_col(tableName)

        # handle additional columns
        cols = " "
        for c in add_columns:
            cols = cols + sql.SQL(",{}").format(sql.Identifier(c)).as_string(self.conn)

        # query
        q = sql.SQL("select {} as id, {} as geom %s from {};" % cols).format(
            sql.Identifier(pkid),
            sql.Identifier(geom_col),
            sql.Identifier(tableName)
        ).as_string(self.conn)

        if self.verbose:
            print(q)

        return gpd.GeoDataFrame.from_postgis(
            q,
            self.conn,
            geom_col="geom",
            index_col="id"
        )


    def list_scenarios(self):
        """Prints the current stored scenarios

        Return: None
        """
        for k, v in self.scenarios.iteritems():
            print(v)


    def _check_scenario_name(self, name, raise_error=True):
        """Checks the scenarios for whether a scenario by the given name exists.
        If raise_error is true then raise an error if a match is found.
        Returns true if the check is passed (meaning the name DOES NOT exist).

        Return: Boolean
        """
        if self.verbose:
            print("Checking name %s" % name)
        if name in self.scenarios:
            if raise_error:
                raise KeyError("A scenario named %s already exists" % name)
            else:
                return False
        else:
            return True


    def add_scenario_existing(self, scenario):
        """Register a pre-existing scenario object with this pyBNA

        Return: None
        """
        if self._check_scenario_name(scenario.name):
            if self.verbose:
                print("Adding scenario %s" % scenario)
            self.scenarios[scenario.name] = scenario


    def _set_scenarios(self):
        for scenario in self.config["bna"]["scenarios"]:
            self.add_scenario_new(scenario)


    def add_scenario_new(self, config, verbose=False):
        """Creates a new scenario and registers it

        args:
        name -- this scenario's name. a test is run to make sure there's not
            already a scenario of the same name.
        verbose -- output useful messages

        Return: None
        """
        name = config["name"]
        defaults = self.config["bna"]["defaults"]["scenario"]
        self._reestablish_conn()
        if self._check_scenario_name(name):
            if self.verbose:
                print("Creating scenario %s" % name)

            if "notes" in config:
                notes = config["notes"]
            else:
                notes = "Scenario %s" % name

            if "roads" in config:
                road_table = config["roads"]
            else:
                road_table = defaults["roads"]
            road_id_col = self._get_pkid_col(road_table)

            if "nodes" in config:
                node_table = config["nodes"]
            else:
                node_table = defaults["nodes"]
            node_id_col = self._get_pkid_col(node_table)

            if "edges" in config and "table" in config["edges"]:
                edge_table = config["edges"]["table"]
            else:
                edge_table = defaults["edges"]["table"]
            edge_id_col = self._get_pkid_col(edge_table)

            if "edges" in config and "source_column" in config["edges"]:
                node_source_col = config["edges"]["source_column"]
            else:
                node_source_col = defaults["edges"]["source_column"]

            if "edges" in config and "target_column" in config["edges"]:
                node_target_col = config["edges"]["target_column"]
            else:
                node_target_col = defaults["edges"]["target_column"]

            if "edges" in config and "stress_column" in config["edges"]:
                edge_stress_col = config["edges"]["stress_column"]
            else:
                edge_stress_col = defaults["edges"]["stress_column"]

            if "edges" in config and "cost_column" in config["edges"]:
                edge_cost_col = config["edges"]["cost_column"]
            else:
                edge_cost_col = defaults["edges"]["cost_column"]

            if "max_distance" in config:
                max_distance = config["max_distance"]
            else:
                max_distance = defaults["max_distance"]

            if "max_detour" in config:
                max_detour = config["max_detour"]
            else:
                max_detour = defaults["max_detour"]

            if "max_stress" in config:
                max_stress = config["max_stress"]
            else:
                max_stress = defaults["max_stress"]

            self.scenarios[name] = Scenario(
                name, notes, self.conn, self.blocks, self.srid,
                max_distance, max_stress, max_detour,
                self.blocks_schema, self.blocks_table, self.block_id_col,
                road_table, road_id_col,
                node_table, node_id_col,
                edge_table, edge_id_col, node_source_col, node_target_col, edge_stress_col, edge_cost_col,
                self.verbose
            )


    def add_scenario_from_pickle(self, path, name=None):
        """Unpickles a saved scenario and registers it. If name is None uses
        the scenario's given name. Else use the given name and update the scenario
        name.

        Return: None
        """
        # check if name is specified and is OK
        if name:
            self._check_scenario_name(name)

        if not os.path.isfile(path):
            raise FileNotFoundError("No file found at %s" % path)
        try:
            if self.verbose:
                print("Unpickling scenario at %s and adding" % path)
            scenario = pickle.load(open(path, "rb"))
        except pickle.UnpicklingError:
            raise pickle.UnpicklingError(
                "Could not restore %s. Is this file a valid scenario pickle?" % path)
        except IOError:
            raise FileNotFoundError("No file found at %s" % path)

        if self._check_scenario_name(scenario.name):
            self.scenarios[scenario.name] = scenario


    def save_scenario_to_pickle(self,scenario_name,path,overwrite=False):
        """Pickles the saved scenario to the specified path.

        Return: None
        """
        if self._check_scenario_name(scenario_name,raise_error=False):
            raise KeyError("Scenario %s doesn't exist" % scenario_name)

        # check for existing file
        if os.path.isfile(path) and not overwrite:
            raise IOError("File %s already exists" % path)

        try:
            if self.verbose:
                print("Saving scenario to %s" % path)
            self.scenarios[scenario_name].conn = None
            pickle.dump(self.scenarios[scenario_name],open(path, "wb"))
            self.scenarios[scenario_name].conn = self.conn
        except pickle.UnpicklingError:
            raise pickle.UnpicklingError(
                "Could not save to %s" % path)


    def _set_blocks(self):
        """
        Set pybna's blocks from database
        """
        blocks_table = self.config["bna"]["blocks"]["table"]
        if "schema" in self.config["bna"]["blocks"]:
            blocks_schema = self.config["bna"]["blocks"]["schema"]
        else:
            blocks_schema = self._get_schema(self.blocks_table)
        if block_id_col is None:
            block_id_col = self.config["bna"]["blocks"]["id_column"]

        if self.verbose:
            print("Getting census blocks from %s.%s" % (blocks_schema,blocks_table))
        q = sql.SQL("select {} as geom, {} as blockid from {}.{};").format(
            sql.Identifier("geom"),
            sql.Identifier(block_id_col),
            sql.Identifier(blocks_schema),
            sql.Identifier(blocks_table)
        ).as_string(self.conn)

        if self.verbose:
            print(q)

        df = gpd.GeoDataFrame.from_postgis(
            q,
            self.conn,
            geom_col=self.config["bna"]["blocks"]["geom"]
        )

        self.blocks = df
        self.blocks_table = blocks_table
        self.blocks_schema = blocks_schema
        self.block_id_col = block_id_col


    def _set_destinations(self):
        """Retrieve the generic BNA destination types and register them."""
        if self.verbose:
            print('Adding destinations')

        cur = self.conn.cursor()
        name = v["name"]

        for v in self.config["destinations"]:
            if "table" in v:
                self.destinations[name] = Destinations(
                    name, self.conn, v["table"], v["uid"], verbose=self.verbose
                )
            if "subcats" in v:
                self.destinations[k] = Destinations(
                    name, self.conn, v["table"], v["uid"], verbose=self.verbose
                )
            # add all the census blocks containing a destination from this category
            # to the pyBNA index of all blocks containing a destination of any type
            self.destination_blocks.update(
                self.destinations[name].destination_blocks)

        if self.verbose:
            print("%i census blocks are part of at least one destination" %
                  len(self.destination_blocks))


    def _get_schema(self,table):
        cur = self.conn.cursor()
        cur.execute(" \
            select nspname::text \
            from pg_namespace n, pg_class c \
            where n.oid = c.relnamespace \
            and c.oid = '%s'::regclass \
        " % table)
        return cur.next()[0]


    def _get_srid(self,table):
        schema = self._get_schema(table)
        cur = self.conn.cursor()
        cur.execute("select find_srid('%s','%s','%s')" % (schema,table,"geom"))
        srid = cur.next()[0]

        if self.verbose:
            print("SRID: %i" % srid)

        return srid


    def score(self):
        """Calculate network score."""
        pass


    def _reestablish_conn(self):
        db_connection_string = self.conn.dsn
        try:
            cur = self.conn.cursor()
            cur.execute("select 1")
            cur.fetchone()
            cur.close()
        except:
            self.conn = psycopg2.connect(db_connection_string)
