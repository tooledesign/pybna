###################################################################
# The Scenario class stores a BNA scenario for use in pyBNA.
# A scenario includes two graphs: one for high stress, one for low stress
###################################################################
import networkx as nx
from nxutils import *
import psycopg2
from psycopg2.extensions import quote_ident


class Scenario:
    """A scenario to analyze in the BNA."""

    def __init__(self, name, notes, conn, maxStress, edgeTable, nodeTable,
                    edgeIdCol=None, fromNodeCol=None, toNodeCol=None,nodeIdCol=None,
                    stressCol=None, edgeCostCol=None, verbose=False):
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

        # set optional vars
        if edgeIdCol is None:
            edgeIdCol = self._getPkidColumn(edgeTable)
        if nodeIdCol is None:
            nodeIdCol = self._getPkidColumn(nodeTable)
        if fromNodeCol is None:
            fromNodeCol = 'source_vert'
        if toNodeCol is None:
            toNodeCol = 'target_vert'
        if stressCol is None:
            stressCol = 'link_stress'
        if edgeCostCol is None:
            edgeCostCol = 'link_cost'

        # build graphs
        self.hsG = buildNetwork(conn,edgeTable,nodeTable,edgeIdCol,nodeIdCol,
            fromNodeCol,toNodeCol,edgeCostCol,stressCol,self.verbose
        )

        self.lsG = buildRestrictedNetwork(self.hsG,self.maxStress)


    def __unicode__(self):
        return u'Scenario %s  :  Max stress %i  :  Notes: %s' % (self.name, self.maxStress, self.notes)


    def __repr__(self):
        return r'Scenario %s  :  Max stress %i  :  Notes: %s' % (self.name, self.maxStress, self.notes)

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
