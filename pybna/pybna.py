###################################################################
# pybna is a Python module that uses networkx to implement the
# connectivity logic developed in the BNA.
#
# dependencies:
#   pyyaml
#   munch
#   psycopg2
###################################################################
import os
import yaml
import collections
from munch import Munch
import psycopg2
from psycopg2 import sql
from tqdm import tqdm

from core import Core
from connectivity import Connectivity
from destinations import Destinations
from dbutils import DBUtils


class pyBNA(DBUtils,Destinations,Connectivity,Core):
    """Parent BNA class that glues together the Core, Connectivity, and Destinations classes"""

    def __init__(self, config="config.yaml", force_net_build=False,
                 verbose=False, debug=False,
                 host=None, db_name=None, user=None, password=None):
        """Connects to the BNA database

        kwargs:
        config -- path to the config file
        force_net_build -- force a rebuild of the network even if an existing one is found
        verbose -- output useful messages
        debug -- set to debug mode
        host -- hostname or address (overrides the config file if given)
        db -- name of database on server (overrides the config file if given)
        user -- username to connect to database (overrides the config file if given)
        password -- password to connect to database (overrides the config file if given)

        return: pyBNA object
        """
        Destinations.__init__(self)
        Connectivity.__init__(self)
        Core.__init__(self)
        self.verbose = verbose
        self.debug = debug
        self.module_dir = os.path.dirname(os.path.abspath(__file__))
        self.config = self.parse_config(yaml.safe_load(open(config)))
        self.config["bna"]["connectivity"]["max_detour"] = float(100 + self.config["bna"]["connectivity"]["max_detour"])/100
        self.db_connectivity_table = self.config["bna"]["connectivity"]["table"]
        self.net_config = self.config["bna"]["network"]

        if self.verbose:
            print("")
            print("---------------pyBNA---------------")
            print("   Create and test BNA scenarios")
            print("-----------------------------------")
            print("")

        # set up db connection
        print("Connecting to database")
        if host is None:
            host = self.config["db"]["host"]
        if db_name is None:
            db_name = self.config["db"]["dbname"]
        if user is None:
            user = self.config["db"]["user"]
        if password is None:
            password = self.config["db"]["password"]
        db_connection_string = " ".join([
            "dbname=" + db_name,
            "user=" + user,
            "host=" + host,
            "password=" + password
        ])
        if self.debug:
            print("DB connection: %s" % db_connection_string)
        DBUtils.__init__(self,db_connection_string,self.verbose,self.debug)

        # blocks
        if not self.debug:
            self.set_blocks()

        # srid
        if "srid" in self.config:
            self.srid = self.config["srid"]
        elif not self.debug:
            self.srid = self.get_srid(self.blocks.table)

        # destinations
        self.destinations = dict()
        self.destination_blocks = set()
        if not self.debug:
            pass
            # self.set_destinations()

        # tiles
        if "table" in self.config.bna.tiles:
            self.tiles_exist = True
        else:
            self.tiles_exist = False

        self.tiles_table = self.config["bna"]["tiles"]["table"]
        if "schema" in self.config["bna"]["tiles"]:
            self.tiles_schema = self.config["bna"]["tiles"]["schema"]
        elif self.table_exists(self.tiles_table):
            self.tiles_schema = self.get_schema(self.config["bna"]["tiles"]["table"])

        if "id_column" in self.config["bna"]["tiles"]:
            self.tiles_pkid = self.config["bna"]["tiles"]["id_column"]
        elif self.table_exists(self.tiles_table):
            self.tiles_pkid = self.get_pkid_col(self.config["bna"]["tiles"]["table"],self.tiles_schema)

        if force_net_build:
            print("Building network tables in database")
            self.build_network()
        elif self.debug:
            pass
        elif not self.check_network():
            print("Network tables not found in database...building")
            self.build_network()
        elif self.verbose:
            print("Network tables found in database")

        self.sql_subs = self.make_subs(self.config)


    def parse_config(self,config):
        """
        Reads through the giant dictionary loaded from YAML and converts into
        munches that can be accessed with dot-notation

        args:
        config -- a dictionary of configuration options

        returns:
        Munch
        """

        if isinstance(config, collections.Mapping):
            for key, value in config.iteritems():
                config[key] = self.parse_config(value)
            return Munch(config)
        return config


    def make_subs(self, config):
        """
        Constructs universal SQL substitutions from all of the config
        parameters.

        returns:
        dictionary of SQL substitutions
        """
        blocks = config.bna.blocks
        tiles = config.bna.tiles
        network = config.bna.network
        connectivity = config.bna.connectivity

        # blocks
        if "schema" in blocks:
            blocks_schema = blocks.schema
        else:
            blocks_schema = self.get_schema(blocks.table)
        if "uid" in blocks:
            blocks_id_col = blocks.uid
        else:
            blocks_id_col = self.get_pkid_col(blocks.table,blocks_schema)
        if "geom" in blocks:
            blocks_geom_col = blocks.geom
        else:
            blocks_geom_col = "geom"

        # tiles
        if self.tiles_exist:
            tiles_table = tiles.table
            if "schema" in tiles:
                tiles_schema = tiles.schema
            elif self.table_exists(tiles_table):
                tiles_schema = self.get_schema(tiles_table)
            else:
                tiles_schema = blocks_schema

            if self.table_exists(tiles_table,tiles_schema):
                if "uid" in tiles:
                    tiles_id_col = tiles.uid
                else:
                    tiles_id_col = self.get_pkid_col(tiles_table,tiles_schema)
                if "geom" in tiles:
                    tiles_geom_col = tiles.geom
                else:
                    tiles_geom_col = "geom"
            else:
                if "uid" in tiles:
                    tiles_id_col = tiles.uid
                else:
                    tiles_id_col = "id"
                if "geom" in tiles:
                    tile_geom_col = tiles.geom
                else:
                    tile_geom_col = "geom"

        else:
            tiles_table = " "
            tiles_schema = " "
            tile_id_col = " "
            tile_geom_col = " "

        # network


        subs = {
            "blocks_table": sql.Identifier(blocks.table),
            "blocks_schema": sql.Identifier(blocks_schema),
            "blocks_id_col": sql.Identifier(blocks_id_col),
            "blocks_geom_col": sql.Identifier(blocks_geom_col),
            "blocks_population_col": sql.Identifier(blocks.population),
            "tiles_table": sql.Identifier(tiles_table),
            "tiles_schema": sql.Identifier(tiles_schema),
            "tiles_id_col": sql.Identifier(tiles_id_col),
            "tiles_geom_col": sql.Identifier(tiles_geom_col),
            "roads_table": sql.Identifier(network.roads.table)
            "roads_schema":
            "roads_id_col":
            "roads_geom_col":
            "roads_source_col": sql.Identifier(self.config.network.roads.source_column)
            "roads_target_col": sql.Identifier(self.config.network.roads.target_column)
            "roads_oneway_col": sql.Identifier(self.config.network.roads.oneway.name)
            "roads_oneway_fwd": sql.Literal(self.config.network.roads.oneway.forward)
            "roads_oneway_bwd": sql.Literal(self.config.network.roads.oneway.backward)
            "roads_stress_seg_fwd": sql.Identifier(self.config.network.roads.stress.segment.forward)
            "roads_stress_seg_bwd": sql.Identifier(self.config.network.roads.stress.segment.backward)
            "roads_stress_cross_fwd": sql.Identifier(self.config.network.roads.stress.crossing.forward)
            "roads_stress_cross_bwd": sql.Identifier(self.config.network.roads.stress.crossing.backward)
            "ints_table": sql.Identifier(self.config.network.intersections.table)
            "ints_schema":
            "ints_id_col":
            "ints_geom_col":
            "edges_table": sql.Identifier(self.config.network.edges.table)
            "edges_schema":
            "edges_id_col":
            "edges_geom_col":
            "edges_source_col": sql.Identifier(self.config.network.edges.source_column)
            "edges_target_col": sql.Identifier(self.config.network.edges.target_column)
            "edges_stress_col": sql.Identifier(self.config.network.edges.stress_column)
            "edges_cost_col": sql.Identifier(self.config.network.edges.cost_column)
            "nodes_table": sql.Identifier(self.config.network.nodes.table)
            "nodes_schema":
            "nodes_id_col":
            "nodes_geom_col":
            "zones_table": sql.Identifier(self.config.connectivity.zones.table)
            "zones_schema":
            "zones_id_col":
            "zones_geom_col":
            "connectivity_table": sql.Identifier(self.config.connectivity.table)
            "connectivity_schema":
            "connectivity_source_col": sql.Identifier(self.config.connectivity.source_column)
            "connectivity_target_col": sql.Identifier(self.config.connectivity.target_column)
            "connectivity_max_distance": sql.Literal(self.config.connectivity.max_distance)
            "connectivity_max_detour": sql.Literal(self.config.connectivity.max_detour)
            "connectivity_max_stress": sql.Literal(self.config.connectivity.max_stress)
        }

        return subs
