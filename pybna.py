###################################################################
# pybna is a Python module that uses networkx to implement the
# connectivity logic developed in the BNA.
###################################################################
import os
import yaml
import psycopg2
from psycopg2 import sql
import pandas as pd
import geopandas as gpd
import pickle
from tqdm import tqdm

from scenario import Scenario
from destinations import Destinations


class pyBNA:
    """Collection of BNA scenarios and attendant functions."""

    def __init__(self, config="config.yaml", host=None, db=None, user=None,
                 password=None, verbose=False, debug=False, load_scenarios=False):
        """Connects to the BNA database

        kwargs:
        config -- path to the config file
        host -- hostname or address
        db -- name of database on server
        user -- username to connect to database
        password -- password to connect to database
        verbose -- output useful messages
        debug -- set to debug mode
        load_scenarios -- automatically load scenarios defined in the config file

        return: pyBNA object
        """
        self.verbose = verbose
        self.debug = debug
        self.module_dir = os.path.dirname(os.path.abspath(__file__))
        self.config = yaml.safe_load(open(config))

        if self.verbose:
            print("")
            print("---------------pyBNA---------------")
            print("   Create and test BNA scenarios")
            print("-----------------------------------")
            print("")

        # set up db connection
        if host is None:
            host = self.config["db"]["host"]
        if db is None:
            db = self.config["db"]["dbname"]
        if user is None:
            user = self.config["db"]["user"]
        if password is None:
            password = self.config["db"]["password"]
        self.db_connection_string = " ".join([
            "dbname=" + db,
            "user=" + user,
            "host=" + host,
            "password=" + password
        ])
        self.conn = psycopg2.connect(self.db_connection_string)

        # blocks
        self.blocks = None
        self.blocks_table = None
        self.blocks_schema = None
        self.block_id_col = None
        self.block_pop = None
        if not self.debug:
            self._set_blocks()

        # get srid
        if "srid" in self.config:
            self.srid = self.config["srid"]
        elif not self.debug:
            self.srid = self._get_srid(self.blocks_table)

        # Create dictionaries to hold scenarios and destinations
        self.scenarios = dict()
        if load_scenarios:
            self.load_scenarios()

        # Set destinations from config file
        self.destinations = dict()
        self.destination_blocks = set()
        if not self.debug:
            pass
            # self._set_destinations()

        # Get tiles for running connectivity (if given)
        self.tiles = None
        if not self.debug:
            if "tiles" in self.config["bna"]:
                tile_config = self.config["bna"]["tiles"]
                if "table" in tile_config and "file" in tile_config:
                    raise ValueError("Cannot accept tile sources from both shapefile _and_ pg table")
                if "file" in tile_config:
                    self.tiles = self._get_tiles_shp(tiles_shp_path)
                if "table" in tile_config:
                    tiles_table_name = tile_config["table"]
                    tiles_table_geom_col = tile_config["geom"]
                    if "columns" in tile_config:
                        tiles_columns = tile_config["columns"]
                    else:
                        tiles_columns = list()
                    self.tiles = self._get_tiles_pg(tiles_table_name,tiles_table_geom_col,tiles_columns)


    def _get_pkid_col(self, table, schema=None):
        # connect to pg and read id col
        conn = self._get_db_connection()
        cur = conn.cursor()

        if schema:
            full_table = schema + "." + table
        else:
            full_table = table

        q = sql.SQL(" \
            SELECT a.attname \
            FROM   pg_index i \
            JOIN   pg_attribute a ON a.attrelid = i.indrelid \
                    AND a.attnum = ANY(i.indkey) \
            WHERE  i.indrelid = {}::regclass \
            AND    i.indisprimary;"
        ).format(
            sql.Literal(full_table)
        )
        cur.execute(q)

        if cur.rowcount == 0:
            raise Error("No primary key defined on table %s" % table)

        row = cur.fetchone()
        if self.verbose:
            print("   ID: %s" % row[0])
        cur.close()
        conn.close()
        return row[0]


    def _get_tiles_shp(self,path):
        return 1


    def _get_tiles_pg(self,tableName,geom_col,add_columns=list()):
        pkid = self._get_pkid_col(tableName)

        # handle additional columns
        cols = " "
        for c in add_columns:
            if c == pkid:   # we already grab the primary key column
                continue
            cols = cols + sql.SQL(",{}").format(sql.Identifier(c)).as_string(self.conn)

        # query
        q = sql.SQL("select {} as id, {} as pkid, {} as geom %s from {};" % cols).format(
            sql.Identifier(pkid),
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


    def load_scenarios(self,force_net_build=False):
        if self.debug:
            force_net_build = False
        for scenario in self.config["bna"]["scenarios"]:
            self.add_scenario_new(scenario,force_net_build)


    def add_scenario_new(self, config, build_network=True):
        """Creates a new scenario and registers it

        args:
        name -- this scenario's name. a test is run to make sure there's not
            already a scenario of the same name.
        verbose -- output useful messages

        Return: None
        """
        name = config["name"]
        if self._check_scenario_name(name):
            if self.verbose:
                print("Creating scenario %s" % name)

            self.scenarios[name] = Scenario(self, config, build_network)


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
        boundary_table = self.config["bna"]["boundary"]["table"]
        boundary_geom = self.config["bna"]["boundary"]["geom"]
        pop = self.config["bna"]["blocks"]["population"]
        geom = self.config["bna"]["blocks"]["geom"]
        if "schema" in self.config["bna"]["blocks"]:
            blocks_schema = self.config["bna"]["blocks"]["schema"]
        else:
            blocks_schema = self._get_schema(blocks_table)
        if "id_column" in self.config["bna"]["blocks"]:
            block_id_col = self.config["bna"]["blocks"]["id_column"]
        else:
            block_id_col = _get_pkid_col(blocks_table,schema=blocks_schema)

        subs = {
            "block_geom": sql.Identifier(geom),
            "block_id": sql.Identifier(block_id_col),
            "pop": sql.Identifier(pop),
            "blocks_schema": sql.Identifier(blocks_schema),
            "blocks_table": sql.Identifier(blocks_table),
            "boundary_table": sql.Identifier(boundary_table),
            "boundary_geom": sql.Identifier(boundary_geom)
        }

        if self.verbose:
            print("Getting census blocks from %s.%s" % (blocks_schema,blocks_table))

        q = sql.SQL(" \
            select b.{block_geom} as geom, b.{block_id} as blockid, b.{pop} as pop \
            from {blocks_schema}.{blocks_table} b\
            where exists ( \
                select 1 from {boundary_table} bound \
                where st_intersects(b.{block_geom},bound.{boundary_geom}) \
            );"
        ).format(**subs).as_string(self.conn)

        if self.verbose:
            print(q)

        df = gpd.GeoDataFrame.from_postgis(
            q,
            self.conn,
            geom_col=geom
        )

        self.blocks = df
        self.blocks_table = blocks_table
        self.blocks_schema = blocks_schema
        self.block_id_col = block_id_col
        self.block_pop = pop


    def _set_destinations(self):
        """Retrieve the destinations identified in the config file and register them."""
        if self.verbose:
            print('Adding destinations')

        cur = self.conn.cursor()

        for v in self.config["bna"]["destinations"]:
            if "table" in v:
                self.destinations[v["name"]] = Destinations(
                    v["name"], self.conn, v["table"], v["uid"], verbose=self.verbose
                )
                # add all the census blocks containing a destination from this category
                # to the pyBNA index of all blocks containing a destination of any type
                self.destination_blocks.update(
                    self.destinations[v["name"]].destination_blocks)
            if "subcats" in v:
                for sub in v["subcats"]:
                    self.destinations[sub["name"]] = Destinations(
                        sub["name"],
                        self.conn,
                        sub["table"],
                        sub["uid"],
                        verbose=self.verbose
                    )
                    self.destination_blocks.update(
                        self.destinations[sub["name"]].destination_blocks)


        if self.verbose:
            print("%i census blocks are part of at least one destination" %
                  len(self.destination_blocks))


    def _get_schema(self,table):
        conn = self._get_db_connection()
        cur = conn.cursor()
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


    def _get_db_connection(self):
        """
        Returns a new db connection using the settings from the config file
        """
        return psycopg2.connect(self.db_connection_string)


    def travel_sheds(self,connectivity_table,block_ids,out_table,schema=None,overwrite=False,dry=False):
        """
        Creates a new DB table showing the high- and low-stress travel sheds
        for the block(s) identified by block_ids. If more than one block is
        passed to block_ids the table will have multiple travel sheds that need
        to be filtered by a user.

        args
        connectivity_table -- the connectivity table to use for building travel sheds
        block_ids -- the ids to use building travel sheds
        out_table -- the table to save travel sheds to
        overwrite -- whether to overwrite an existing table
        """
        conn = self._get_db_connection()

        if schema is None:
            schema = self.blocks_schema

        cur = conn.cursor()

        if overwrite and not dry:
            cur.execute(sql.SQL('drop table if exists {}.{}').format(
                sql.Identifier(schema),
                sql.Identifier(out_table)
            ))

        if not dry:
            cur.execute(
                sql.SQL(
                    "create table {}.{} ( \
                        id serial primary key, \
                        geom geometry(multipolygon,{}), \
                        source_blockid text, \
                        target_blockid text, \
                        low_stress boolean, \
                        high_stress boolean \
                    )"
                ).format(
                    sql.Identifier(schema),
                    sql.Identifier(out_table),
                    sql.Literal(self.srid)
                )
            )

        # read in the raw query language
        f = open(os.path.join(self.module_dir,"sql","travel_shed.sql"))
        raw = f.read()
        f.close()

        # set global sql vars
        sidx = "sidx_" + out_table + "_geom"
        idx = "idx_" + out_table + "_source_blockid"

        for block in tqdm(block_ids):
            # compose the query
            subs = {
                "schema": sql.Identifier(schema),
                "table": sql.Identifier(out_table),
                "geom": sql.Identifier(self.config["bna"]["blocks"]["geom"]),
                "blocks_schema": sql.Identifier(self.blocks_schema),
                "blocks": sql.Identifier(self.blocks_table),
                "connectivity": sql.Identifier(connectivity_table),
                "block_id_col": sql.Identifier(self.config["bna"]["blocks"]["id_column"]),
                "source_blockid": sql.Identifier("source_blockid10"),
                "target_blockid": sql.Identifier("target_blockid10"),
                "block_id": sql.Literal(block),
                "sidx": sql.Identifier(sidx),
                "idx": sql.Identifier(idx)
            }

            q = sql.SQL(raw).format(**subs)

            if dry:
                print(q.as_string(self.conn))
            else:
                cur.execute(q)

        conn.commit()
        del cur
