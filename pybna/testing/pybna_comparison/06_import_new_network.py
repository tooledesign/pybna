



import pybna

#import osm network
i = pybna.Importer(config='C:\\Users\\dpatterson\\code\\pybna\\pybna\\testing\\pybna_comparison\\config_website.yaml',
                   verbose=False,
                   debug=False,
                   host=None,
                   db_name=None,
                   user=None,
                   password=None)

i.import_osm_network(roads_table="received.updated_ways",
                    ints_table="received.updated_ints",
                    boundary_file=None,
                    boundary_buffer=None,
                    osm_file=None,
                    keep_holding_tables=False,
                    srid=None,
                    overwrite=False)

# currently gets snagged in the windows environment. The return error reads,


# (venv) (base) C:\Users\dpatterson\code\pybna\pybna\testing\pybna_comparison>python 06_import_new_network.py
# Connecting to database
# Downloading OSM data
# C:\Users\dpatterson\code\pybna\venv\lib\site-packages\pyproj\crs.py:77: FutureWarning: '+init=<authority>:<code>' syntax is deprecated. '<authority>:<code>' is the preferred initialization method.
#   return _prepare_from_string(" ".join(pjargs))
# Traceback (most recent call last):
#   File "06_import_new_network.py", line 22, in <module>
#     overwrite=False)
#   File "C:\Users\dpatterson\code\pybna\pybna\importer.py", line 437, in import_osm_network
#     ways, nodes = self._osm_net_from_osmnx(boundary,osm_file)
#   File "C:\Users\dpatterson\code\pybna\pybna\importer.py", line 667, in _osm_net_from_osmnx
#     custom_filter=None
#   File "C:\Users\dpatterson\code\pybna\venv\lib\site-packages\osmnx\core.py", line 1743, in graph_from_polygon
#     polygon_utm, crs_utm = project_geometry(geometry=polygon)
#   File "C:\Users\dpatterson\code\pybna\venv\lib\site-packages\osmnx\projection.py", line 53, in project_geometry
#     gdf_proj = project_gdf(gdf, to_crs=to_crs, to_latlong=to_latlong)
#   File "C:\Users\dpatterson\code\pybna\venv\lib\site-packages\osmnx\projection.py", line 102, in project_gdf
#     if (gdf.crs is not None) and ('proj' in gdf.crs) and (gdf.crs['proj'] == 'utm'):
# TypeError: argument of type 'CRS' is not iterable
