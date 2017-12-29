###################################################################
# The Scenario class stores a BNA scenario for use in pyBNA.
# A scenario includes two graphs: one for high stress, one for low stress
###################################################################
import networkx as nx
import psycopg2


class Scenario():
    """A scenario to analyze in the BNA."""

    def __init___(self, name, notes, conn, maxStress, edgeTable, nodeTable,
                    edgeIdCol=None, fromNodeCol=None, toNodeCol=None,
                    nodeIdCol=None):
        """Get already built network from PostGIS connection.

        kwargs:
        name -- this scenario's name as stored in the parent pyBNA object
        notes -- any notes to provide further information about this scenario
        conn -- a psycopg2 database connection
        maxStress -- the highest stress rating to allow for the low stress graph
        edgeTable -- name of the table of network edges
        nodeTable -- name of the table of network nodes
        edgeIdCol -- column name for edge IDs (if None uses pkid from the table)
        fromNodeCol -- column name for the from node in edge table (if None uses source_vert, the BNA default)
        toNodeCol -- column name for the to node in edge table (if None uses target_vert, the BNA default)
        nodeIdCol -- column name for the node IDs (if None uses pkid from the table)

        return: None
        """
        self.name = name
        self.notes = notes
        self.conn = conn
        self.maxStress = maxStress
        # Code to pull tables and build network
        pass
        self.lsG = None # This will be the low stress graph
        self.hsG = None # This will be the high stress graph

        # build low stress graph...

        if maxStress == 4:
            self.hsG = self.lsG     # high stress = low stress if maxStress is 4


    def __unicode__(self):
        return u'Scenario %s  :  Max stress %i  :  Notes: %s' % (self.name, self.maxStress, self.notes)


    def __repr__(self):
        return r'Scenario %s  :  Max stress %i  :  Notes: %s' % (self.name, self.maxStress, self.notes)


    def build_low_stress_connectivity(self, blocks):
        """Analyze low-stress connectivity between blocks.

        kwargs:
        blocks -- a GeoDataFrame of census blocks to analyze
        """
