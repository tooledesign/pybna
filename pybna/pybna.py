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
        if "schema" in self.config["bna"]["tiles"]:
            self.tiles_schema = self.config["bna"]["tiles"]["schema"]
        else:
            self.tiles_schema = self.get_schema(self.config["bna"]["tiles"]["table"])

        if "id_column" in self.config["bna"]["tiles"]:
            self.tiles_pkid = self.config["bna"]["tiles"]["id_column"]
        else:
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
