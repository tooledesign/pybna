###################################################################
# pybna is a Python module that uses networkx to implement the
# connectivity logic developed in the BNA.
###################################################################
import os
import networkx as nx
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import quote_ident
import pandas as pd
import geopandas as gpd

from scenario import Scenario
from destinations import Destinations


class pyBNA:
    """Collection of BNA scenarios and attendant functions."""

    def __init__(self, host, db, user, password, censusTable=None,
                 blockIdCol=None, roadIdsCol=None, tilesShpPath=None,
                 tilesTableName=None, verbose=False):
        """Connects to the BNA database

        kwargs:
        host -- hostname or address
        db -- name of database on server
        user -- username to connect to database
        password -- password to connect to database
        censusTable -- name of table of census blocks (if None use neighborhood_census_blocks, the BNA default)
        blockIdCol -- name of the column with census block ids in the block table (if None use blockid10, the BNA default)
        roadIdsCol -- name of the column with road ids in the block table (if None use road_ids, the BNA default)
        tilesShpPath -- path to a shapefile holding features to be used to limit the analysis area (cannot be given in conjunction with tilesTableName)
        tilesTableName -- table name in the BNA database holding features to be used to limit the analysis area (cannot be given in conjunction with tilesShpPath)

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
        self.destinationBlocks = set()
        self._setBNADestinations()

        # Get census blocks
        if censusTable is None:
            censusTable = "neighborhood_census_blocks"
        if blockIdCol is None:
            blockIdCol = "blockid10"
        if roadIdsCol is None:
            roadIdsCol = "road_ids"
        self.blocks = self._getBlocks(censusTable, blockIdCol, roadIdsCol)

        # Get tiles for running connectivity (if given)
        if tilesShpPath and tilesTableName:
            raise ValueError("Cannot accept tile sources from both shapefile _and_ pg table")
        if tilesShpPath:
            self.tiles = self._getTilesShp()
        if tilesTableName:
            self.tiles = self._getTilesPg()

    def _getPkidColumn(self, table):
        # connect to pg and read id col
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
        return row[0]

    def _getTilesShp(self):
        return 1

    def _getTilesPg(self):
        return 1

    def listScenarios(self):
        """Prints the current stored scenarios

        Return: None
        """
        for k, v in self.scenarios.iteritems():
            print(v)

    def checkScenarioName(self, name, raiseError=True):
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

    def addScenarioExisting(self, scenario):
        """Register a pre-existing scenario object with this pyBNA

        Return: None
        """
        if self.checkScenarioName(scenario.name):
            if self.verbose:
                print("Adding scenario %s" % scenario)
            self.scenarios[scenario.name] = scenario

    def addScenarioNew(self, name, notes, maxDist, maxStress, edgeTable, nodeTable,
                       edgeIdCol=None, nodeIdCol=None, maxDetour=25,
                       fromNodeCol='source_vert', toNodeCol='target_vert',
                       stressCol='link_stress', edgeCostCol='link_cost', verbose=False):
        """Creates a new scenario and registers it

        args:
        name -- this scenario's name. a test is run to make sure there's not
            already a scenario of the same name.
        notes -- any notes to provide further information about this scenario
        maxDist -- the travel shed size, or maximum allowable trip distance (in units of the underlying coordinate system)
        maxStress -- the highest stress rating to allow for the low stress graph
        edgeTable -- name of the table of network edges
        nodeTable -- name of the table of network nodes
        edgeIdCol -- column name for edge IDs. if None uses the primary key defined on the table.
        nodeIdCol -- column name for the node IDs. if None uses the primary key defined on the table.
        maxDetour -- the maximum allowable detour for determining low stress connectivity (given as a percentage, i.e. 25 = 25%; if None uses 25%, the BNA default)
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
            self.scenarios[name] = Scenario(name, notes, self.conn, self.blocks,
                                            maxDist, maxStress, edgeTable, nodeTable, edgeIdCol, nodeIdCol,
                                            maxDetour, fromNodeCol, toNodeCol, stressCol, edgeCostCol,
                                            self.verbose
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
            scenario = pickle.load(open(path, "rb"))
        except pickle.UnpicklingError:
            raise pickle.UnpicklingError(
                "Could not restore %s. Is this file a valid scenario pickle?" % path)
        except IOError:
            raise FileNotFoundError("No file found at %s" % path)

        if self.checkScenarioName(scenario.name):
            self.scenarios[scenario.name] = scenario

    def _getBlocks(self, censusTable, blockIdCol, roadIdsCol):
        """Get census blocks from BNA database

        return: geopandas geodataframe
        """
        if self.verbose:
            print('Getting census blocks from %s' % censusTable)

        q = sql.SQL('select {} as geom, {} as blockid, {} as roadids from {};').format(
            sql.Identifier("geom"),
            sql.Identifier(blockIdCol),
            sql.Identifier(roadIdsCol),
            sql.Identifier(censusTable)
        ).as_string(self.conn)

        if self.verbose:
            print(q)

        df = gpd.GeoDataFrame.from_postgis(
            q,
            self.conn,
            geom_col='geom'
        )

        df["roadids"] = df["roadids"].apply(set)
        return df

        # cur = conn.cursor()
        #
        # cur.execute(
        #     sql.SQL('select {}, {} from {};')
        #         .format(
        #             sql.Identifier(blockIdCol),
        #             sql.Identifier(roadIdsCol),
        #             sql.Identifier(censusTable)
        #         )
        #         .as_string(cur)
        # )
        #
        # if self.verbose:
        #     print(cur.query)
        #
        # for row in cur:
        #     if type(row[1]) is list:
        #         roadIds = set(row[1])
        #     else:
        #         roadIds = set([row[1]])
        #     self.blocks[row[0]] = roadIds

    def _setBNADestinations(self):
        """Retrieve the generic BNA destination types and register them."""
        bnaDestinations = [
            {'cat': 'colleges', 'table': 'neighborhood_colleges',
                'uid': 'id', 'name': 'college_name'},
            {'cat': 'community_centers', 'table': 'neighborhood_community_centers',
                'uid': 'id', 'name': 'center_name'},
            {'cat': 'dentists', 'table': 'neighborhood_dentists',
                'uid': 'id', 'name': 'dentists_name'},
            {'cat': 'doctors', 'table': 'neighborhood_doctors',
                'uid': 'id', 'name': 'doctors_name'},
            {'cat': 'hospitals', 'table': 'neighborhood_hospitals',
                'uid': 'id', 'name': 'hospital_name'},
            {'cat': 'parks', 'table': 'neighborhood_parks',
                'uid': 'id', 'name': 'park_name'},
            {'cat': 'pharmacies', 'table': 'neighborhood_pharmacies',
                'uid': 'id', 'name': 'pharmacy_name'},
            {'cat': 'retail', 'table': 'neighborhood_retail',
                'uid': 'id', 'name': 'id'},
            {'cat': 'schools', 'table': 'neighborhood_schools',
                'uid': 'id', 'name': 'school_name'},
            {'cat': 'social_services', 'table': 'neighborhood_social_services',
                'uid': 'id', 'name': 'service_name'},
            {'cat': 'supermarkets', 'table': 'neighborhood_supermarkets',
                'uid': 'id', 'name': 'supermarket_name'},
            {'cat': 'transit', 'table': 'neighborhood_transit',
                'uid': 'id', 'name': 'transit_name'},
            {'cat': 'universities', 'table': 'neighborhood_universities',
                'uid': 'id', 'name': 'college_name'}
        ]
        if self.verbose:
            print('Adding standard BNA destinations')

        cur = self.conn.cursor()

        for d in bnaDestinations:
            self.destinations[d['cat']] = Destinations(
                d['cat'], self.conn, d['table'], d['uid'], d['name'], verbose=self.verbose
            )
            # add all the census blocks containing a destination from this category
            # to the pyBNA index of all blocks containing a destination of any type
            self.destinationBlocks.update(
                self.destinations[d['cat']].destinationBlocks)

        if self.verbose:
            print('%i census blocks are part of at least one destination' %
                  len(self.destinationBlocks))

    def score(self):
        """Calculate network score."""
        pass
