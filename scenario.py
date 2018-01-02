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
                    nodeTable, edgeIdCol, nodeIdCol, maxDetour=1.25,
                    fromNodeCol='source_vert', toNodeCol='target_vert',
                    stressCol='link_stress', edgeCostCol='link_cost',
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
        maxDetour -- maximum allowable detour for determining relative connectivity (defaults to 25%)
        fromNodeCol -- column name for the from node in edge table (if None uses source_vert, the BNA default)
        toNodeCol -- column name for the to node in edge table (if None uses target_vert, the BNA default)
        stressCol -- column name for the stress of the edge (if None uses link_stress, the BNA default)
        edgeCostCol -- column name for the cost of the edge (if None uses link_cost, the BNA default)
        verbose -- output useful messages

        return: None
        """
        self.name = name
        self.notes = notes
        self.conn = conn
        self.maxStress = maxStress
        self.verbose = verbose

        # build graphs
        self.hsG = buildNetwork(conn,edgeTable,nodeTable,edgeIdCol,nodeIdCol,
            fromNodeCol,toNodeCol,edgeCostCol,stressCol,self.verbose
        )

        self.lsG = buildRestrictedNetwork(self.hsG,self.maxStress)

        # create connectivity matrices
        self.hsConnectivity = self._getConnectivity(self.hsG,blocks)
        self.lsConnectivity = self._getConnectivity(self.lsG,blocks)


    def __unicode__(self):
        return u'Scenario %s  :  Max stress %i  :  Notes: %s' % (self.name, self.maxStress, self.notes)


    def __repr__(self):
        return r'Scenario %s  :  Max stress %i  :  Notes: %s' % (self.name, self.maxStress, self.notes)


    def _getConnectivity(self,graph,blocks,maxDist,maxDetour,baseConnectivity=None):
        """Create a connectivity matrix using the given networkx graph and census blocks

        kwargs:
        graph -- networkx graph object
        blocks -- dataframe holding census block data
        maxDist -- the travel shed size, or maximum allowable trip distance (in units of the underlying coordinate system)
        maxDetour -- if a comparison matrix is given, this is used to assess relative connectivity
        baseConnectivity -- optional comparison connectivity matrix. if given, connectivity is determined relative to these figures

        return: pandas sparse dataframe
        """
        if self.verbose:
            print("Getting connectivity on graph")
        matrix = np.empty([len(blocks),len(blocks)],np.bool_)
        df = pd.DataFrame(
            matrix,
            blocks['blockid'].values,
            blocks['blockid'].values
        ).to_sparse(fill_value=False)

        # change to network distances instead of t/f?
        return df

        # get nx routes using:
        # http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.applymap.html#pandas.DataFrame.applymap
        # https://stackoverflow.com/questions/43654727/pandas-retrieve-row-and-column-name-for-each-element-during-applymap

    def _isConnected()
