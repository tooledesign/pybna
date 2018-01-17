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
    ).as_string(conn)

    if verbose:
        print(q)

    df = pd.read_sql_query(
        q,
        conn
    )

    # add edges to graph
    edgeid = G.new_edge_property("int32_t")
    G.edge_properties["pkid"] = edgeid
    cost = G.new_edge_property("double")
    G.edge_properties["cost"] = cost
    stress = G.new_edge_property("int16_t")
    G.edge_properties["stress"] = stress
    nodeid = G.add_edge_list(df.values,hashed=True,eprops=[edgeid,cost,stress])
    G.vertex_properties["pkid"] = nodeid

    # set roadid property on vertices
    if verbose:
        print("Retrieving node data")
    cur = conn.cursor()
    cur.execute(
        sql.SQL('select {} AS id, road_id, st_x(geom) as x, st_y(geom) as y from {};')
            .format(
                sql.Identifier(nodeIdCol),
                sql.Identifier(nodeTable)
            )
            .as_string(cur)
    )
    attrs = dict()
    for row in cur:
        attrs[row[0]] = (row[1],row[2],row[3])

    roadid = G.new_vertex_property("int32_t")
    G.vertex_properties["roadid"] = roadid
    map_property_values(nodeid,roadid,lambda x: attrs[x][0])
    pos =  G.new_vertex_property("vector<double>")
    G.vertex_properties["pos"] = pos
    map_property_values(nodeid,pos,lambda x: (attrs[x][1],attrs[x][2]))

    return G


def buildRestrictedNetwork(G,maxStress):
    stressFilter = G.new_edge_property("bool")
    # G.edge_properties["stressFilter"] = stressFilter
    map_property_values(G.ep.stress,stressFilter,lambda x: x<=maxStress)
    return GraphView(G,efilt=stressFilter)


def translateNode(G,nodeId):
    return find_vertex(G,G.vp.pkid,nodeId)[0]


class VisitorExample(AStarVisitor):
    def __init__(self, targets):
        # self.touched_v = touched_v
        # self.touched_e = touched_e
        self.targets = targets
    # def discover_vertex(self, u):
    #     self.touched_v[u] = True
    # def examine_edge(self, e):
    #     self.touched_e[e] = True
    def edge_relaxed(self, e):
        if e.target() in self.targets:
            raise StopSearch()


def heuristic(v,targets,pos):
    xys = [pos[i] for i in targets]
    meanTarget = np.mean(np.array(xys),axis=0)
    return np.sqrt(sum((pos[v].a - meanTarget) ** 2))

# dist, pred = astar_search(
#     g, g.vertex(0), weight,
#     VisitorExample(touch_v, touch_e, targets),
#     heuristic=lambda v: h(v, target, pos)
# )
