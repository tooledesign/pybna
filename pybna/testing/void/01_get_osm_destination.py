
import pybna

i = pybna.Importer(config='C:\\Users\\dpatterson\\code\\pybna\\pybna\\testing\\pybna_comparison\\config_website.yaml',
                   verbose=False,
                   debug=False,
                   host=None,
                   db_name=None,
                   user=None,
                   password=None)

i.import_osm_destinations(osm_file=None,               #use osmnx
                          schema=None,                 #yaml = received
                          boundary_file=None,          #yaml = received.neighborhood_boundary
                          srid=None,                   #yaml = 26916
                          destination_tags=None,       #no deviation from default
                          overwrite=True,              #do not overwrite
                          keep_intermediates=True)     #keep intermediate tables: area, ways, nodes
