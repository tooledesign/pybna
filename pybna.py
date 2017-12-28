###################################################################
# pybna is a Python module that uses networkx to implement the
# connectivity logic developed in the BNA.
###################################################################
import network as nx
import psycopg2
import pandas as pd

from routing import network


def build_network(conn, tableName=None):
    """
    Returns a networkx graph built from edges and nodes stored in the
    database at the given connection. Assumes the standard naming
    conventions of the BNA tool, unless tableName is given, which is
    used to indicate the table to use in place of the standard
    neighborhood_ways table.
    """

class BNA():
    """A BNA to analyze."""
    def __init__(self):
        # Create network

        # Add census blocks
        self.blocks
        # Add destinations

    def _buildNetwork(self):
        """Build a routing.network."""
        pass

    def _addBlocks(self):
        """Add census blocks to BNA."""
        pass

    def _addDestinations(self):
        """Add generic destinations."""
        pass

    def score(self):
        """Calculate network score."""
