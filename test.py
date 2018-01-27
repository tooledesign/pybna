from pybna import pyBNA
p = pyBNA("192.168.40.225","bna_ames",tiles_table_name='tiles',verbose=True)
p.add_scenario_new("base","lksjdfkd")
s = p.scenarios['base']
a = s.getConnectivity(p.tiles[p.tiles.index>=120],dbTable="blocktest")
# s._setDebug(True)
blocks = s.blocks
tiles = p.tiles
connectivity = s.connectivity
df = s.blocks


p.save_scenario_to_pickle('base','/home/spencer/test.pickly')




import graphutils
import psycopg2
conn = psycopg2.connect('host=192.168.40.225 dbname=bna_ames user=gis')
g = graphutils.buildNetwork(conn,"neighborhood_ways_net_link","neighborhood_ways_net_vert",
    "link_id","vert_id","source_vert","target_vert","link_cost","link_stress")

targets = [g.vertex(100),g.vertex(101)]
dist, pred = astar_search(
    g, g.vertex(0), g.ep.cost,
    VisitorExample(targets),
    heuristic=lambda v: heuristic(v, targets, g.vp.pos)
)





select c.id, b.geom, c.source_blockid10, c.target_blockid10, c.high_stress, c.low_stress
from neighborhood_census_blocks b, blocktest c
where b.blockid10 = c.target_blockid10
and c.source_blockid10 = '191690004003002'
