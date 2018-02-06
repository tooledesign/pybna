###################################################################
# pybna is a Python module that uses networkx to implement the
# connectivity logic developed in the BNA.
###################################################################
import os
import yaml
import networkx as nx
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import quote_ident
import pandas as pd
import geopandas as gpd
import pickle

from scenario import Scenario
from destinations import Destinations


class pyBNA:
    """Collection of BNA scenarios and attendant functions."""

    def __init__(self, host, db, config="default.config", user=None, password=None,
                 blocks_table=None, block_id_col=None, tiles_shp_path=None,
                 tiles_table_name=None, tiles_table_geom_col='geom', tiles_columns=list(),
                 verbose=False):
        """Connects to the BNA database

        kwargs:
        host -- hostname or address
        db -- name of database on server
        config -- path to the config file
        user -- username to connect to database
        password -- password to connect to database
        blocks_table -- name of table of census blocks (default: neighborhood_census_blocks, the BNA default)
        block_id_col -- name of the column with census block ids in the block table (default: blockid10, the BNA default)
        tiles_shp_path -- path to a shapefile holding features to be used to limit the analysis area (cannot be given in conjunction with tiles_table_name)
        tiles_table_name -- table name in the BNA database holding features to be used to limit the analysis area (cannot be given in conjunction with tiles_shp_path)
        tiles_table_geom_col -- name of the column with geometry (default: geom)
        tiles_columns -- list of additional table columns to include with tiles (e.g. for filtering tiles)

        return: pyBNA object
        """
        self.verbose = verbose
        self.config = yaml.safe_load(open(config))

        if self.verbose:
            print("\n \
            ---------------pyBNA---------------\n \
            Create and test BNA scenarios\n \
            \n \
            Configuration parameters:")
            print(self.config)

        # set up db connection
        if user is None:
            user = self.config['db']['user']
        if password is None:
            password = self.config['db']['password']
        db_connection_string = " ".join([
            "dbname=" + db,
            "user=" + user,
            "host=" + host,
            "password=" + password
        ])
        self.conn = psycopg2.connect(db_connection_string)

        # Create dictionaries to hold scenarios and destinations
        self.scenarios = dict()
        self.destinations = dict()

        # Set default BNA destinations
        self.destination_blocks = set()
        self._set_bna_destinations()

        # Get census blocks
        if blocks_table:
            self.blocks_table = blocks_table
        else:
            self.blocks_table = self.config['db']['blocks']['table']
        self.census_schema = self._get_schema(self.blocks_table)
        if block_id_col:
            self.block_id_col = block_id_col
        else:
            self.block_id_col = self.config['db']['blocks']['id_column']
        self.blocks = self._get_blocks(self.blocks_table, self.block_id_col)

        # get srid
        try:
            self.srid = self.config['db']['srid']
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
        cur.execute(' \
        SELECT a.attname \
        FROM   pg_index i \
        JOIN   pg_attribute a ON a.attrelid = i.indrelid \
                AND a.attnum = ANY(i.indkey) \
        WHERE  i.indrelid = %(table)s::regclass \
        AND    i.indisprimary;', {"table": quote_ident(table, cur)})

        if cur.rowcount == 0:
            raise Error('No primary key defined on table %s' % table)

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
        q = sql.SQL('select {} as id, {} as geom %s from {};' % cols).format(
            sql.Identifier(pkid),
            sql.Identifier(geom_col),
            sql.Identifier(tableName)
        ).as_string(self.conn)

        if self.verbose:
            print(q)

        return gpd.GeoDataFrame.from_postgis(
            q,
            self.conn,
            geom_col='geom',
            index_col='id'
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
                raise KeyError('A scenario named %s already exists' % name)
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


    def add_scenario_new(self, name, notes,
                        max_distance=None, max_stress=None, max_detour=None,
                        road_table=None, node_table=None, edge_table=None, verbose=False):
        """Creates a new scenario and registers it

        args:
        name -- this scenario's name. a test is run to make sure there's not
            already a scenario of the same name.
        notes -- any notes to provide further information about this scenario
        max_distance -- the travel shed size, or maximum allowable trip distance (in units of the underlying coordinate system)
        max_stress -- the highest stress rating to allow for the low stress graph
        max_detour -- the maximum allowable detour for determining low stress connectivity (given as a percentage, i.e. 25 = 25%)
        road_table -- the table with road data
        node_table -- name of the table of network nodes
        edge_table -- name of the table of network edges
        verbose -- output useful messages

        Return: None
        """
        self._reestablish_conn()
        if self._check_scenario_name(name):
            if self.verbose:
                print("Creating scenario %s" % name)

            if road_table is None:
                road_table = self.config['scenario']['roads']['table']
            try:
                road_id_col = self.config['scenario']['roads']['id_column']
            except KeyError:
                road_id_col = self._get_pkid_col(road_table)
            if node_table is None:
                node_table = self.config['scenario']['nodes']['table']
            try:
                node_id_col = self.config['scenario']['nodes']['id_column']
            except KeyError:
                node_id_col = self._get_pkid_col(node_table)
            if edge_table is None:
                edge_table = self.config['scenario']['edges']['table']
            try:
                edge_id_col = self.config['scenario']['edges']['id_column']
            except KeyError:
                edge_id_col = self._get_pkid_col(edge_table)
            node_source_col = self.config['scenario']['edges']['source_column']
            node_target_col = self.config['scenario']['edges']['target_column']
            edge_stress_col = self.config['scenario']['edges']['stress_column']
            edge_cost_col = self.config['scenario']['edges']['cost_column']

            self.scenarios[name] = Scenario(
                name, notes, self.conn, self.blocks, self.srid,
                max_distance, max_stress, max_detour,
                self.census_schema, self.blocks_table, self.block_id_col,
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


    def _get_blocks(self, blocks_table, block_id_col):
        """Get census blocks from BNA database

        return: geopandas geodataframe
        """
        if self.verbose:
            print('Getting census blocks from %s' % blocks_table)
        q = sql.SQL('select {} as geom, {} as blockid from {};').format(
            sql.Identifier("geom"),
            sql.Identifier(block_id_col),
            sql.Identifier(blocks_table)
        ).as_string(self.conn)

        if self.verbose:
            print(q)

        df = gpd.GeoDataFrame.from_postgis(
            q,
            self.conn,
            geom_col='geom'
        )

        return df


    def _set_bna_destinations(self):
        """Retrieve the generic BNA destination types and register them."""
        if self.verbose:
            print('Adding destinations')

        cur = self.conn.cursor()

        for k, v in self.config['destinations'].iteritems():
            self.destinations[k] = Destinations(
                k, self.conn, v['table'], v['uid'], v['name'], verbose=self.verbose
            )
            # add all the census blocks containing a destination from this category
            # to the pyBNA index of all blocks containing a destination of any type
            self.destination_blocks.update(
                self.destinations[k].destination_blocks)

        if self.verbose:
            print('%i census blocks are part of at least one destination' %
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
            cur.execute('select 1')
            cur.fetchone()
            cur.close()
        except:
            self.conn = psycopg2.connect(db_connection_string)
