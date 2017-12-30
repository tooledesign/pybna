###################################################################
# pybna is a Python module that uses networkx to implement the
# connectivity logic developed in the BNA.
###################################################################
import os
import networkx as nx
import psycopg2
from psycopg2.extensions import quote_ident
import pandas as pd

from scenario import Scenario
from destinations import Destinations


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

        # Create dictionaries to hold scenarios and destinations
        self.scenarios = dict()
        self.destinations = dict()

        # Set default BNA destinations
        self._setBNADestinations()

        # Add census blocks
        self.blocks = None


    def _getPkidColumn(self,table):
        # connect to pg and read id col
        cur = self.conn.cursor()
        cur.execute(' \
        SELECT a.attname \
        FROM   pg_index i \
        JOIN   pg_attribute a ON a.attrelid = i.indrelid \
                AND a.attnum = ANY(i.indkey) \
        WHERE  i.indrelid = %(table)s::regclass \
        AND    i.indisprimary;', {"table": quote_ident(table,cur)})

        if cur.rowcount == 0:
            raise Error('No primary key defined on table %s' % table)

        row = cur.fetchone()
        if self.verbose:
            print("   ID: %s" % row[0])
        return row[0]


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
                    edgeIdCol=None, nodeIdCol=None, fromNodeCol=None, toNodeCol=None,
                    stressCol=None, edgeCostCol=None, verbose=False):
        """Creates a new scenario and registers it

        args:
        name -- this scenario's name. a test is run to make sure there's not
            already a scenario of the same name.
        notes -- any notes to provide further information about this scenario
        maxStress -- the highest stress rating to allow for the low stress graph
        edgeTable -- name of the table of network edges
        nodeTable -- name of the table of network nodes
        edgeIdCol -- column name for edge IDs. if None uses the primary key defined on the table.
        nodeIdCol -- column name for the node IDs. if None uses the primary key defined on the table.
        fromNodeCol -- column name for the from node in edge table (if None uses source_vert, the BNA default)
        toNodeCol -- column name for the to node in edge table (if None uses target_vert, the BNA default)
        stressCol -- column name for the stress of the edge (if None uses link_stress, the BNA default)
        edgeCostCol -- column name for the cost of the edge (if None uses link_cost, the BNA default)
        verbose -- output useful messages

        Return: None
        """
        if self.checkScenarioName(name):
            if self.verbose:
                print("Creating scenario %s" % name)
            if edgeIdCol is None:
                edgeIdCol = self._getPkidColumn(edgeTable)
            if nodeIdCol is None:
                nodeIdCol = self._getPkidColumn(nodeTable)
            self.scenarios[name] = Scenario(name, notes, self.conn, maxStress,
                edgeTable, nodeTable, edgeIdCol, nodeIdCol, fromNodeCol, toNodeCol,
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


    def _setBNADestinations(self):
        """Retrieve the generic BNA destination types and register them."""
        # connect to pg and read id col
        if self.verbose:
            print('Adding standard BNA destinations')

        cur = self.conn.cursor()

        self.destinations['colleges'] = Destinations(
            'colleges',self.conn,'neighborhood_colleges','id','college_name',verbose=self.verbose
        )
        self.destinations['community_centers'] = Destinations(
            'community_centers',self.conn,'neighborhood_community_centers','id','center_name',verbose=self.verbose
        )
        self.destinations['dentists'] = Destinations(
            'dentists',self.conn,'neighborhood_dentists','id','dentists_name',verbose=self.verbose
        )
        self.destinations['doctors'] = Destinations(
            'doctors',self.conn,'neighborhood_doctors','id','doctors_name',verbose=self.verbose
        )
        self.destinations['hospitals'] = Destinations(
            'hospitals',self.conn,'neighborhood_hospitals','id','hospital_name',verbose=self.verbose
        )
        self.destinations['parks'] = Destinations(
            'parks',self.conn,'neighborhood_parks','id','park_name',verbose=self.verbose
        )
        # self.destinations['paths'] = Destinations(
        #     'paths',self.conn,'neighborhood_paths','path_id','path_id',verbose=self.verbose
        # )
        self.destinations['pharmacies'] = Destinations(
            'pharmacies',self.conn,'neighborhood_pharmacies','id','pharmacy_name',verbose=self.verbose
        )
        self.destinations['retail'] = Destinations(
            'retail',self.conn,'neighborhood_retail','id','id',verbose=self.verbose
        )
        self.destinations['schools'] = Destinations(
            'schools',self.conn,'neighborhood_schools','id','school_name',verbose=self.verbose
        )
        self.destinations['social_services'] = Destinations(
            'social_services',self.conn,'neighborhood_social_services','id','service_name',verbose=self.verbose
        )
        self.destinations['supermarkets'] = Destinations(
            'supermarkets',self.conn,'neighborhood_supermarkets','id','supermarket_name',verbose=self.verbose
        )
        self.destinations['transit'] = Destinations(
            'transit',self.conn,'neighborhood_transit','id','transit_name',verbose=self.verbose
        )
        self.destinations['universities'] = Destinations(
            'universities',self.conn,'neighborhood_universities','id','college_name',verbose=self.verbose
        )


    def score(self):
        """Calculate network score."""
        pass
