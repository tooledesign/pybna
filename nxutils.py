###################################################################
# Tools for working with networkx graphs in the BNA
###################################################################
import networkx as nx
import psycopg2
from psycopg2.extensions import quote_ident

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
    cur.execute(' \
        SELECT  %(fromNodeCol)s AS fnode, %(toNodeCol)s AS tnode, %(edgeIdCol)s AS id, \
                %(edgeCostCol)s AS cost, %(stressCol)s AS stress \
        FROM '+edgeTable+';',{
            "fromNodeCol": quote_ident(fromNodeCol,cur),
            "toNodeCol": quote_ident(toNodeCol,cur),
            "edgeIdCol": quote_ident(edgeIdCol,cur),
            "edgeCostCol": quote_ident(edgeCostCol,cur),
            "stressCol": quote_ident(stressCol,cur)
        }
    )

    for row in cur:
        DG.add_edge(
            row[0],                 # fnode
            row[1],                 # tnode
            edgeid=row[2],          # id
            weight=row[3],          # cost
            stress=min(row[4],99)   # stress
        )

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
