



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
