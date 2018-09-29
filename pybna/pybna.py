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

from core import Core
from connectivity import Connectivity
from destinations import Destinations
from dbutils import DBUtils
import graphutils


class pyBNA(Destinations,Connectivity,Core):
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
        self.verbose = verbose
        self.debug = debug
        self.module_dir = os.path.dirname(os.path.abspath(__file__))
        self.config = yaml.safe_load(open(config))
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
        self.db_connection_string = " ".join([
            "dbname=" + db_name,
            "user=" + user,
            "host=" + host,
            "password=" + password
        ])
        if self.debug:
            print("DB connection: %s" % self.db_connection_string)
        self.db = DBUtils(self.db_connection_string)

        # blocks
        if not self.debug:
            self.set_blocks()

        # srid
        if "srid" in self.config:
            self.srid = self.config["srid"]
        elif not self.debug:
            self.srid = self.db.get_srid(self.blocks.table)

        # destinations
        self.destinations = dict()
        self.destination_blocks = set()
        if not self.debug:
            pass
            # self.set_destinations()

        # tiles
        if not self.debug:
            if "tiles" in self.config["bna"]:
                tile_config = self.config["bna"]["tiles"]
                if "table" in tile_config and "file" in tile_config:
                    raise ValueError("Cannot accept tile sources from both shapefile _and_ pg table")
                if "file" in tile_config:
                    pass
                    # self.tiles = self._get_tiles_shp(tiles_shp_path)
                if "table" in tile_config:
                    tiles_table_name = tile_config["table"]
                    tiles_table_geom_col = tile_config["geom"]
                    if "columns" in tile_config:
                        tiles_columns = tile_config["columns"]
                    else:
                        tiles_columns = list()
                    if not self.db.table_exists(tiles_table_name):
                        self.make_tiles()
                    self.tiles = self.get_tiles(tiles_table_name,tiles_table_geom_col,tiles_columns)

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

        # build graphs
        if not self.debug:
            conn = self.db.get_db_connection()
            # self.hs_graph = graphutils.build_graph(
            #     conn,
            #     self.config["bna"]["network"]["edges"],
            #     self.config["bna"]["network"]["nodes"],
            #     self.verbose
            # )
            # self.ls_graph = graphutils.build_restricted_graph(
            #     self.hs_graph,
            #     self.config["bna"]["connectivity"]["max_stress"]
            # )
            conn.close()

        # get block nodes
        # if not self.debug:
        #     self.net_blocks = self.blocks.blocks.merge(
        #         self._get_block_nodes(),
        #         on="blockid"
        #     )
        #     self.net_blocks["graph_v"] = self.net_blocks["nodes"].apply(self._get_graph_nodes)
