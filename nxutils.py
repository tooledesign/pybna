###################################################################
# Tools for working with networkx graphs in the BNA
###################################################################
import networkx as nx
import psycopg2
from psycopg2 import sql


def buildNetwork(conn,edgeTable,nodeTable,edgeIdCol,nodeIdCol,fromNodeCol,toNodeCol,
        edgeCostCol,stressCol,verbose=False):
    """Builds a networkx graph from a complete network stored in the PostGIS database

    return: networkx DiGraph
    """
    DG = nx.DiGraph()

    cur = conn.cursor()

    # build edges
    if verbose:
        print("Retrieving edges")

    cur.execute(
        sql.SQL('select {} AS fnode, {} AS tnode, {} AS id, {} AS cost, {} AS stress from {};')
            .format(
                sql.Identifier(fromNodeCol),
                sql.Identifier(toNodeCol),
                sql.Identifier(edgeIdCol),
                sql.Identifier(edgeCostCol),
                sql.Identifier(stressCol),
                sql.Identifier(edgeTable)
            )
            .as_string(cur)
    )

    if verbose:
        print(cur.query)

    for row in cur:
        DG.add_edge(
            row[0],                 # fnode
            row[1],                 # tnode
            edgeid=row[2],          # id
            weight=row[3],          # cost
            stress=min(row[4],99)   # stress
        )

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

    # # build nodes
    # if verbose:
    #     print(nodeQuery)
    # cur.execute(nodeQuery)
    # for row in cur:
    #     self.DG.add_edge(
    #         int(row[0]),            # fnode
    #         int(row[1]),            # tnode
    #         edgeid=row[2],          # id
    #         weight=row[3],          # cost
    #         stress=min(row[4],99)   # stress
    #     )

    return DG


def buildRestrictedNetwork(DG,maxStress):
    return nx.DiGraph( [ (u,v,d) for u,v,d in DG.edges(data=True) if d['stress'] <= maxStress ] )
