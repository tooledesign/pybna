###################################################################
# The Scenario class stores a BNA scenario for use in pyBNA.
# A scenario includes two graphs: one for high stress, one for low stress
###################################################################
import networkx as nx
from nxutils import *
import psycopg2
import numpy as np
from scipy.sparse import coo_matrix
import pandas as pd
import geopandas as gpd


class Scenario:
    """A scenario to analyze in the BNA."""

    def __init__(self, name, notes, conn, blocks, maxDist, maxStress,
                    edgeTable, nodeTable, edgeIdCol, nodeIdCol, maxDetour=None,
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
        tiles -- geodataframe holding tiles to split up processing
        maxDetour -- maximum allowable detour for determining relative connectivity (given as a percentage, i.e. 25 = 25%)
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
        self.maxDist = maxDist
        if maxDetour:
            self.maxDetour = 1 + float(maxDetour) / 100
        else:
            self.maxDetour = 1.25
        self.verbose = verbose

        # build graphs
        self.hsG = buildNetwork(conn,edgeTable,nodeTable,edgeIdCol,nodeIdCol,
            fromNodeCol,toNodeCol,edgeCostCol,stressCol,self.verbose
        )

        self.lsG = buildRestrictedNetwork(self.hsG,self.maxStress)

        # create connectivity matrix
        self.connectivity = None


    def __unicode__(self):
        return u'Scenario %s  :  Max stress %i  :  Notes: %s' % (self.name, self.maxStress, self.notes)


    def __repr__(self):
        return r'Scenario %s  :  Max stress %i  :  Notes: %s' % (self.name, self.maxStress, self.notes)


    def getConnectivity(self,tiles=None):
        """Create a connectivity matrix using the this class' networkx graphs and
        census blocks. The matrix relies on bitwise math so the following
        correspondence of values to connectivity combinations is possible:
        0 = neither hs nor ls connected       (binary 00)
        1 = hs connected but not ls connected (binary 01)
        2 = ls connected but not hs connected (binary 10) (not possible under current methodology)
        3 = both hs and ls connected          (binary 11)

        kwargs:
        tiles -- a geopandas dataframe holding polygons for breaking the analysis into chunks

        return: pandas sparse dataframe
        """
        if self.verbose:
            print("Building connectivity matrix")

        # create ixj dataframe for block connectivity
        '''
        need to reconfigure this to accept tile inputs. the ij matrix should only
        have blocks in the tile and should return a df with only blocks from the set
        that have a connection of some kind (i.e. hsls > 0)
        '''
        if tiles is None:
            tiles = gpd.GeoDataFrame.from_features(gpd.GeoSeries(
                self.blocks.unary_union.envelope,
                name="geom"
            ))

        c = 1
        ctotal = len(tiles)
        for i in tiles.index:
            if self.verbose:
                print("Processing tile %i out of %i" % (c,ctotal))
                
            self.blocks["tempkey"] = 1
            df = gpd.sjoin(
                self.blocks,
                tiles[tiles.index==i]
            )[['blockid','geom','tempkey']]

            df['geom'] = df.centroid

            df = gpd.sjoin(
                df,
                tiles[tiles.index==i]
            ).drop(columns=['geom','index_right'])

            df = df.merge(
                self.blocks[['blockid','tempkey']],
                on='tempkey',
                suffixes=('from','to')
            ).drop(columns=['tempkey']).to_sparse()

            self.blocks = self.blocks.drop(columns=['tempkey'])

            df['hsls'] = pd.SparseSeries([False] * len(df),dtype='int8',fill_value=0)

            # run connectivity for all rows
            # df['hsls'] = df.apply(self._isConnected,axis=1,args=(maxDist,maxDetour))

        return df

        # get nx routes using:
        # http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.applymap.html#pandas.DataFrame.applymap
        # https://stackoverflow.com/questions/43654727/pandas-retrieve-row-and-column-name-for-each-element-during-applymap

    def _isConnected(self,row):
        hsConnected = False
        lsConnected = False

        fromBlock = row['blockidfrom']
        toBlock = row['blockidto']
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
                    elif l < hsDist and l < self.maxDist:
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
