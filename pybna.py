###################################################################
# pybna is a Python module that uses networkx to implement the
# connectivity logic developed in the BNA.
###################################################################
import network as nx
import psycopg2
import pandas as pd


def build_network(conn,tableName=None):
    """
    Returns a networkx graph built from edges and nodes stored in the
    database at the given connection. Assumes the standard naming
    conventions of the BNA tool, unless tableName is given, which is
    used to indicate the table to use in place of the standard
    neighborhood_ways table.
    """
