from pybna import pyBNA
p = pyBNA("192.168.40.225","bna_ames","gis","gis",tilesTableName='tiles',verbose=True)
p.addScenarioNew("base","lksjdfkd")
s = p.scenarios['base']
blocks = s.blocks
tiles = p.tiles
connectivity = s.connectivity
a = s.getConnectivity(p.tiles[p.tiles.index==113])
