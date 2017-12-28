import networkx as nx
import psycopg2
import pandas as pd


class network():
    """A network to analyze in the BNA."""

    def __init___(self, conn, tableName=None):
        """Get already built network from PostGIS connection.

        kwargs:
        conn -- a psycopg2 database connection
        tableName -- name of the tables to be pulled. If None, defaults to
                    NOTE Spencer what should be defaults?

        return: None
        """
        # Code to pull tables and build network
        pass
        self.G = None  # This will be the nx

    def build_low_stress_connectivity(self, blocks):
        """Analyze low-stress connectivity between blocks.

        kwargs:
        blocks -- a GeoDataFrame of census blocks to analyze
        """
    
