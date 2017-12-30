###################################################################
# pybna is a Python module that uses networkx to implement the
# connectivity logic developed in the BNA.
###################################################################
import os
import networkx as nx
import psycopg2
import pandas as pd

from scenario import Scenario


class pyBNA:
    """Collection of BNA scenarios and attendant functions."""

    def __init__(self,host,db,user,password,censusTable=None,verbose=False):
        """Connects to the BNA database

        kwargs:
        host -- hostname or address
        db -- name of database on server
        user -- username to connect to database
        password -- password to connect to database
        censusTable -- name of table of census blocks (if None use neighborhood_census_blocks, the BNA default)

        return: None
        """
        self.verbose = verbose

        # set up db connection
        db_connection_string = " ".join([
            "dbname=" + db,
            "user=" + user,
            "host=" + host,
            "password=" + password
        ])
        self.conn = psycopg2.connect(db_connection_string)

        # Create dictionary to hold scenario networks
        self.scenarios = dict()

        # Add census blocks
        self.blocks = None

        # Add destinations

    def listScenarios(self):
        """Prints the current stored scenarios

        Return: None
        """
        for k, v in self.scenarios.iteritems():
            print(v)

    def checkScenarioName(self,name,raiseError=True):
        """Checks the scenarios for whether a scenario by the given name exists.
        If raiseError is true then raise an error if a match is found.
        Returns true if the check is passed.

        Return: Boolean
        """
        if self.verbose:
            print("Checking name %s" % name)
        if name in self.scenarios:
            if raiseError:
                raise KeyError('A scenario named %s already exists' % name)
            else:
                return False
        else:
            return True

    def addScenarioExisting(self,scenario):
        """Register a pre-existing scenario object with this pyBNA

        Return: None
        """
        if self.checkScenarioName(scenario.name):
            if self.verbose:
                print("Adding scenario %s" % scenario)
            self.scenarios[scenario.name] = scenario

    def addScenarioNew(self, name, notes, maxStress, edgeTable, nodeTable,
                    edgeIdCol=None, fromNodeCol=None, toNodeCol=None, nodeIdCol=None,
                    stressCol=None, edgeCostCol=None, verbose=False):
        """Creates a new scenario and registers it

        Return: None
        """
        if self.checkScenarioName(name):
            if self.verbose:
                print("Creating scenario %s" % name)
            self.scenarios[name] = Scenario(name, notes, self.conn, maxStress,
                edgeTable, nodeTable, edgeIdCol, fromNodeCol, toNodeCol, nodeIdCol,
                stressCol, edgeCostCol, self.verbose
            )

    def addScenarioPickle(self, path, name=None):
        """Unpickles a saved scenario and registers it. If name is None uses
        the scenario's given name. Else use the given name and update the scenario
        name.

        Return: None
        """
        # check if name is specified and is OK
        if name:
            self.checkScenarioName(name)

        if not os.path.isfile(path):
            raise FileNotFoundError("No file found at %s" % path)
        try:
            if self.verbose:
                print("Unpickling scenario at %s and adding" % path)
            scenario = pickle.load(open(path,"rb"))
        except pickle.UnpicklingError:
            raise pickle.UnpicklingError("Could not restore %s. Is this file a valid scenario pickle?" % path)
        except IOError:
            raise FileNotFoundError("No file found at %s" % path)

        if self.checkScenarioName(scenario.name):
            self.scenarios[scenario.name] = scenario



    def _addBlocks(self,censusTable):
        """Add census blocks to BNA."""
        pass
        # create geopandas object to hold blocks

    def _addDestinations(self):
        """Add generic destinations."""
        pass

    def score(self):
        """Calculate network score."""
