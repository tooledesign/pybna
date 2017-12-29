###################################################################
# pybna is a Python module that uses networkx to implement the
# connectivity logic developed in the BNA.
###################################################################
import networkx as nx
import psycopg2
import pandas as pd

from scenario import Scenario


class pyBNA:
    """Collection of BNA scenarios and attendant functions."""

    def __init__(self,host,db,user,password,censusTable=None):
        """Connects to the BNA database

        kwargs:
        host -- hostname or address
        db -- name of database on server
        user -- username to connect to database
        password -- password to connect to database
        censusTable -- name of table of census blocks (if None use neighborhood_census_blocks, the BNA default)

        return: None
        """

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
        """Lists the current stored scenarios"""
        for k, v in self.scenarios:
            print(v)

    def scenarioNameAvailable(self,name):
        """Checks the scenarios for whether a scenario by the given name exists"""
        if name in scenarios:
            return False
        else:
            return True

    def addScenarioExisting(self,scenario):
        """Register a pre-existing scenario object with this pyBNA"""
        # check if name is OK
        if not scenarioNameAvailable(scenario.name):
            raise KeyError('A scenario named %s already exists' % scenario.name)
        else:
            self.scenarios[scenario.name] = scenario

    def addScenarioNew(self, name, notes, maxStress, edgeTable, nodeTable,
                    edgeIdCol=None, fromNodeCol=None, toNodeCol=None,
                    nodeIdCol=None):
        """Creates a new scenario and registers it"""
        # check if name is OK
        if not scenarioNameAvailable(name):
            raise KeyError('A scenario named %s already exists' % name)
        else:
            self.scenarios[name] = Scenario(name, notes, self.conn, maxStress,
                edgeTable, nodeTable, edgeIdCol, fromNodeCol, toNodeCol, nodeIdCol
            )

    def _addBlocks(self,censusTable):
        """Add census blocks to BNA."""
        pass
        # create geopandas object to hold blocks

    def _addDestinations(self):
        """Add generic destinations."""
        pass

    def score(self):
        """Calculate network score."""
