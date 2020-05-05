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
from munch import Munch
from psycopg2 import sql
from tqdm import tqdm

from .core import Core
from .connectivity import Connectivity
from .destinations import Destinations
from .conf import Conf
from .dbutils import DBUtils


class pyBNA(Conf,Destinations,Connectivity,Core):
    """Parent BNA class that glues together the subclasses"""

    def __init__(self, config=None, force_net_build=False,
                 verbose=False, debug=False,
                 host=None, db_name=None, user=None, password=None):
        """Connects to the BNA database

        Parameters
        ----------
        config : str, optional
            path to the config file, if not given use the default config.yaml
        force_net_build : bool, optional
            force a rebuild of the network even if an existing one is found
        verbose : bool, optional
            output useful messages
        debug : bool, optional
            set to debug mode
        host : str, optional
            hostname or address (overrides the config file if given)
        db : str, optional
            name of database on server (overrides the config file if given)
        user : str, optional
            username to connect to database (overrides the config file if given)
        password : str, optional
            password to connect to database (overrides the config file if given)

        Returns
        -------
        pyBNA object
        """
        Destinations.__init__(self)
        Connectivity.__init__(self)
        Core.__init__(self)
        Conf.__init__(self)
        self.verbose = verbose
        self.debug = debug
        self.module_dir = os.path.dirname(os.path.abspath(__file__))
        if config is None:
            config = os.path.join(self.module_dir,"config.yaml")
        self.config = self.parse_config(yaml.safe_load(open(config)))
        self.config["bna"]["connectivity"]["max_detour"] = float(100 + self.config["bna"]["connectivity"]["max_detour"])/100
        self.db_connectivity_table = self.config["bna"]["connectivity"]["table"]
        self.net_config = self.config["bna"]["network"]

        # km/mi
        if "units" in self.config:
            if self.config.units == "mi":
                self.km = False
            elif self.config.units == "km":
                self.km = True
            else:
                raise ValueError("Invalid units \"{}\" in config".format(self.config.units))
        else:
            self.km = False

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
            print("DB connection: {}".format(db_connection_string))
        DBUtils.__init__(self,db_connection_string,self.verbose,self.debug)

        # srid
        if "srid" in self.config:
            self.srid = self.config["srid"]
        elif not self.debug:
            self.srid = self.get_srid(self.config.bna.blocks.table)

        self.register_destinations()

        self.sql_subs = self.make_bna_substitutions(self.config)

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
