###################################################################
# Tools for working with graphs in the BNA
###################################################################
from graph_tool.all import *
import pandas as pd
import psycopg2
from psycopg2 import sql


def buildNetwork(conn,edgeTable,nodeTable,edgeIdCol,nodeIdCol,fromNodeCol,toNodeCol,
        edgeCostCol,stressCol,verbose=False):
    """Builds a networkx graph from a complete network stored in the PostGIS database

    return: networkx DiGraph
    """
    G = Graph()

    # build edges
    if verbose:
        print("Retrieving edges")

    q = sql.SQL(
        'select {} AS fnode, {} AS tnode, {} AS id, {} AS cost, {} AS stress from {};'
    ).format(
        sql.Identifier(fromNodeCol),
        sql.Identifier(toNodeCol),
        sql.Identifier(edgeIdCol),
        sql.Identifier(edgeCostCol),
        sql.Identifier(stressCol),
        sql.Identifier(edgeTable)
    ).as_string(cur)

    if verbose:
        print(q)

    df = pd.read_sql_query(
        q,
        conn
    )

    # add edges to graph
    pkid = G.new_edge_property("int16_t")
    G.edge_properties["pkid"] = pkid
    cost = G.new_edge_property("double")
    G.edge_properties["cost"] = cost
    stress = G.new_edge_property("int16_t")
    G.edge_properties["stress"] = stress
    G.vertex_properties["pkid"] = G.add_edge_list(df.values,hashed=True,eprops=[pkid,cost,stress])

    # add data to nodes
    if verbose:
        print("Retriving nodes")

    cur.execute(
        sql.SQL('select {} AS id, road_id from {};')
            .format(
                sql.Identifier(nodeIdCol),
                sql.Identifier(nodeTable)
            )
            .as_string(cur)
    )

    attrs = dict()
    for row in cur:
        attrs[row[0]] = row[1]

    nx.set_node_attributes(DG,values=attrs,name="roadid")

    return DG


def buildRestrictedNetwork(DG,maxStress):
    return nx.DiGraph( [ (u,v,d) for u,v,d in DG.edges(data=True) if d['stress'] <= maxStress ] )
