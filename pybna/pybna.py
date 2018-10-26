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
from zones import Zones
from dbutils import DBUtils


class pyBNA(DBUtils,Zones,Destinations,Connectivity,Core):
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

        # zones
        if "zones" in self.config.bna.connectivity:
            if "table" in self.config.bna.connectivity.zones:
                self.zones_exist = True
            else:
                self.zones_exist = False
        else:
            self.zones_exist = False

        # default schema
        if "schema" in self.config.bna.blocks:
            self.default_schema = self.config.bna.blocks.schema
        else:
            self.default_schema = self.get_schema(self.config.bna.blocks.table)

        self.sql_subs = self.make_sql_substitutions(self.config)

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


    def make_sql_substitutions(self, config):
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
            if "uid" in tiles:
                tiles_id_col = tiles.uid
            elif self.table_exists(tiles_table,tiles_schema):
                tiles_id_col = self.get_pkid_col(tiles_table,tiles_schema)
            else:
                tiles_id_col = "id"
            if "geom" in tiles:
                tiles_geom_col = tiles.geom
            elif self.table_exists(tiles_table,tiles_schema):
                tiles_geom_col = "geom"
            else:
                tiles_geom_col = "geom"

        else:
            tiles_table = " "
            tiles_schema = " "
            tiles_id_col = " "
            tiles_geom_col = " "

        # roads
        if "schema" in network.roads:
            roads_schema = network.roads.schema
        else:
            roads_schema = self.get_schema(network.roads.table)
        if "uid" in network.roads:
            roads_id_col = network.roads.uid
        else:
            roads_id_col = self.get_pkid_col(network.roads.table,roads_schema)
        if "geom" in network.roads:
            roads_geom_col = network.roads.geom
        else:
            roads_geom_col = "geom"

        # intersections
        if "schema" in network.intersections:
            ints_schema = network.intersections.schema
        else:
            ints_schema = self.get_schema(network.intersections.table)
        if "uid" in network.intersections:
            ints_id_col = network.intersections.uid
        else:
            ints_id_col = self.get_pkid_col(network.intersections.table,ints_schema)
        if "geom" in network.intersections:
            ints_geom_col = network.intersections.geom
        else:
            ints_geom_col = "geom"

        # edges
        if "schema" in network.edges:
            edges_schema = network.edges.schema
        elif self.table_exists(network.edges.table):
            edges_schema = self.get_schema(network.edges.table)
        else:
            edges_schema = roads_schema
        if "uid" in network.edges:
            edges_id_col = network.edges.uid
        elif self.table_exists(network.edges.table,edges_schema):
            edges_id_col = self.get_pkid_col(network.edges.table,edges_schema)
        else:
            edges_id_col = "edge_id"
        if "geom" in network.edges:
            edges_geom_col = network.edges.geom
        elif self.table_exists(network.edges.table,edges_schema):
            edges_geom_col = "geom"
        else:
            edges_geom_col = "geom"

        # nodes
        if "schema" in network.nodes:
            nodes_schema = network.nodes.schema
        elif self.table_exists(network.nodes.table):
            nodes_schema = self.get_schema(network.nodes.table)
        else:
            nodes_schema = roads_schema
        if "uid" in network.nodes:
            nodes_id_col = network.nodes.uid
        elif self.table_exists(network.nodes.table,nodes_schema):
            nodes_id_col = self.get_pkid_col(network.nodes.table,nodes_schema)
        else:
            nodes_id_col = "node_id"
        if "geom" in network.nodes:
            nodes_geom_col = network.nodes.geom
        elif self.table_exists(network.nodes.table,nodes_schema):
            nodes_geom_col = "geom"
        else:
            nodes_geom_col = "geom"

        # zones
        if self.zones_exist:
            zones_table = connectivity.zones.table
            if "schema" in connectivity.zones:
                zones_schema = connectivity.zones.schema
            elif self.table_exists(zones_table):
                zones_schema = self.get_schema(zones_table)
            else:
                zones_schema = blocks_schema
            if "uid" in connectivity.zones:
                zones_id_col = connectivity.zones.uid
            elif self.table_exists(zones_table,zones_schema):
                zones_id_col = self.get_pkid_col(zones_table,zones_schema)
            else:
                zones_id_col = "id"
            if "geom" in connectivity.zones:
                zones_geom_col = connectivity.zones.geom
            elif self.table_exists(zones_table,zones_schema):
                zones_geom_col = "geom"
            else:
                zones_geom_col = "geom"
        else:
            zones_table = " "
            zones_schema = " "
            zones_id_col = " "
            zones_geom_col = " "

        # connectivity
        if "schema" in connectivity:
            connectivity_schema = connectivity.schema
        else:
            connectivity_schema = blocks_schema

        # srid
        if "srid" in config:
            srid = config.srid
        else:
            srid = self.get_srid(blocks.table,schema=blocks_schema)

        subs = {
            "srid": sql.Literal(srid),
            "blocks_table": sql.Identifier(blocks.table),
            "blocks_schema": sql.Identifier(blocks_schema),
            "blocks_id_col": sql.Identifier(blocks_id_col),
            "blocks_geom_col": sql.Identifier(blocks_geom_col),
            "blocks_population_col": sql.Identifier(blocks.population),
            "blocks_roads_tolerance": sql.Literal(blocks.roads_tolerance),
            "blocks_min_road_length": sql.Literal(blocks.min_road_length),
            "tiles_table": sql.Identifier(tiles_table),
            "tiles_schema": sql.Identifier(tiles_schema),
            "tiles_id_col": sql.Identifier(tiles_id_col),
            "tiles_geom_col": sql.Identifier(tiles_geom_col),
            "roads_table": sql.Identifier(network.roads.table),
            "roads_schema": sql.Identifier(roads_schema),
            "roads_id_col": sql.Identifier(roads_id_col),
            "roads_geom_col": sql.Identifier(roads_geom_col),
            "roads_source_col": sql.Identifier(network.roads.source_column),
            "roads_target_col": sql.Identifier(network.roads.target_column),
            "roads_oneway_col": sql.Identifier(network.roads.oneway.name),
            "roads_oneway_fwd": sql.Literal(network.roads.oneway.forward),
            "roads_oneway_bwd": sql.Literal(network.roads.oneway.backward),
            "roads_stress_seg_fwd": sql.Identifier(network.roads.stress.segment.forward),
            "roads_stress_seg_bwd": sql.Identifier(network.roads.stress.segment.backward),
            "roads_stress_cross_fwd": sql.Identifier(network.roads.stress.crossing.forward),
            "roads_stress_cross_bwd": sql.Identifier(network.roads.stress.crossing.backward),
            "ints_table": sql.Identifier(network.intersections.table),
            "ints_schema": sql.Identifier(ints_schema),
            "ints_id_col": sql.Identifier(ints_id_col),
            "ints_geom_col": sql.Identifier(ints_geom_col),
            "edges_table": sql.Identifier(network.edges.table),
            "edges_schema": sql.Identifier(edges_schema),
            "edges_id_col": sql.Identifier(edges_id_col),
            "edges_geom_col": sql.Identifier(edges_geom_col),
            "edges_source_col": sql.Identifier(network.edges.source_column),
            "edges_target_col": sql.Identifier(network.edges.target_column),
            "edges_stress_col": sql.Identifier(network.edges.stress_column),
            "edges_cost_col": sql.Identifier(network.edges.cost_column),
            "nodes_table": sql.Identifier(network.nodes.table),
            "nodes_schema": sql.Identifier(nodes_schema),
            "nodes_id_col": sql.Identifier(nodes_id_col),
            "nodes_geom_col": sql.Identifier(nodes_geom_col),
            "zones_table": sql.Identifier(zones_table),
            "zones_schema": sql.Identifier(zones_schema),
            "zones_id_col": sql.Identifier(zones_id_col),
            "zones_geom_col": sql.Identifier(zones_geom_col),
            "connectivity_table": sql.Identifier(connectivity.table),
            "connectivity_schema": sql.Identifier(connectivity_schema),
            "connectivity_source_col": sql.Identifier(connectivity.source_column),
            "connectivity_target_col": sql.Identifier(connectivity.target_column),
            "connectivity_max_distance": sql.Literal(connectivity.max_distance),
            "connectivity_max_detour": sql.Literal(connectivity.max_detour),
            "connectivity_max_stress": sql.Literal(connectivity.max_stress)
        }

        return subs
