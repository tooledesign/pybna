from pybna import pyBNA
p = pyBNA("192.168.40.225","bna_ames","gis","gis",verbose=True)
p.addScenarioNew("base","lksjdfkd",3000,1,"neighborhood_ways_net_link","neighborhood_ways_net_vert")
blocks = p.blocks
s = p.scenarios['base']
connectivity = s.connectivity


def sum(row,c,d):
  return row['hsls']+c+d

connectivity.head(10).apply(sum,axis=1,args=(3,5))
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
TypeError: apply() got an unexpected keyword argument 'args'
type(connectivity)
<class 'pandas.core.sparse.frame.SparseDataFrame'>
