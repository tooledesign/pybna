###################################################################
# The Scenario class stores a BNA scenario for use in pyBNA.
# A scenario includes two graphs: one for high stress, one for low stress
###################################################################
import networkx as nx
from nxutils import *
import psycopg2
import numpy as np
import pandas as pd
import geopandas as gpd


class Scenario:
    """A scenario to analyze in the BNA."""

    def __init__(self, name, notes, conn, blocks, maxDist, maxStress, edgeTable,
                    nodeTable, edgeIdCol, nodeIdCol, maxDetour=None,
                    fromNodeCol=None, toNodeCol=None,
                    stressCol=None, edgeCostCol=None,
                    verbose=False):
        """Get already built network from PostGIS connection.

        args:
        name -- this scenario's name as stored in the parent pyBNA object
        notes -- any notes to provide further information about this scenario
        conn -- a psycopg2 database connection
        blocks -- geopandas dataframe of census blocks
        maxDist -- the travel shed size, or maximum allowable trip distance (in units of the underlying coordinate system)
        maxStress -- the highest stress rating to allow for the low stress graph
        edgeTable -- name of the table of network edges
        nodeTable -- name of the table of network nodes
        edgeIdCol -- column name for edge IDs
        nodeIdCol -- column name for the node IDs
        maxDetour -- maximum allowable detour for determining relative connectivity
        fromNodeCol -- column name for the from node in edge table
        toNodeCol -- column name for the to node in edge table
        stressCol -- column name for the stress of the edge
        edgeCostCol -- column name for the cost of the edge
        verbose -- output useful messages

        return: None
        """
        self.name = name
        self.notes = notes
        self.conn = conn
        self.blocks = blocks
        self.maxStress = maxStress
        self.verbose = verbose

        # build graphs
        self.hsG = buildNetwork(conn,edgeTable,nodeTable,edgeIdCol,nodeIdCol,
            fromNodeCol,toNodeCol,edgeCostCol,stressCol,self.verbose
        )

        self.lsG = buildRestrictedNetwork(self.hsG,self.maxStress)

        # create connectivity matrix
        self.connectivity = self._getConnectivity(maxDist,maxDetour)


    def __unicode__(self):
        return u'Scenario %s  :  Max stress %i  :  Notes: %s' % (self.name, self.maxStress, self.notes)


    def __repr__(self):
        return r'Scenario %s  :  Max stress %i  :  Notes: %s' % (self.name, self.maxStress, self.notes)


    def _getConnectivity(self,maxDist,maxDetour):
        """Create a connectivity matrix using the this class' networkx graphs and
        census blocks. The matrix relies on bitwise math so the following
        correspondence of values to connectivity combinations is possible:
        0 = neither hs nor ls connected (binary 00)
        1 = hs connected but not ls connected (binary 01)
        2 = ls connected but not hs connected (binary 10) (not possible under current methodology)
        3 = both hs and ls connected (binary 11)

        kwargs:
        hsG -- networkx graph object for the high stress network
        lsG -- networkx graph object for the low stress network
        blocks -- dataframe holding census block data
        maxDist -- the travel shed size, or maximum allowable trip distance (in units of the underlying coordinate system)
        maxDetour -- this is used to assess relative connectivity

        return: pandas sparse dataframe
        """
        if self.verbose:
            print("Building connectivity matrix")

        '''
        need to rewrite with an i,j table instead of full matrix
        itertools.product(p.blocks['blockid'].values,p.blocks['blockid'])

        '''
        matrix = np.zeros((len(self.blocks),len(self.blocks)),dtype=np.uint8)
        df = pd.DataFrame(
            matrix,
            self.blocks['blockid'].values,
            self.blocks['blockid'].values
        ).to_sparse(fill_value=0)

        df = df.apply(self._isConnected,args=(maxDist,maxDetour))


        return df

        # get nx routes using:
        # http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.applymap.html#pandas.DataFrame.applymap
        # https://stackoverflow.com/questions/43654727/pandas-retrieve-row-and-column-name-for-each-element-during-applymap

    def _isConnected(self,cell,maxDist,maxDetour):
        hsConnected = False
        lsConnected = False

        fromBlock = cell.index
        toBlock = cell.name
        hsDist = -1
        lsDist = -1

        fromNodes = self.blocks.loc[self.blocks['blockid'] == fromBlock]['roadids'].values
        toNodes = self.blocks.loc[self.blocks['blockid'] == toBlock]['roadids'].values

        # first test hs connection
        for i in fromNodes:
            for j in toNodes:
                try:
                    l = nx.dijkstra_path_length(self.hsG,i,j,weight='weight')
                    if hsDist < 0:
                        hsDist = l
                    elif l < hsDist and l < maxDist:
                        hsDist = l
                    else:
                        pass
                except nx.NetworkNoPath:
                    pass

        if hsDist > 0:
            hsConnected = True

        # next test ls connection (but only if hsConnected)
        # if hsConnected:
        #     pass

        return hsConnected | (lsConnected << 1)
